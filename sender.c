/*
 * Copyright (c) 2019 Kislaya Kumar <storagXpert@gmail.com>
 *
 * This work can be distributed under the terms of the GNU GPLv3.
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License Version 3 for more details.
 */

#include "lucid.h"

// read the msg at 'send_seqno' from local partition file for this queue.
// send this msg to 'dst_ip'.
static int
send_msg (int socket, struct queue_metadata *qmeta, uint32_t dst_ip,
          uint64_t send_seqno) {
    struct qmsg_header hdr = {0};
    uint8_t *payload = (uint8_t *)malloc(sizeof(qmeta->msgsz));
    struct iovec iov[2] = {0};
    if (!payload)
        return -1;
    hdr.type = QMSG;
    hdr.qid = htobe32(qmeta->qid);
    hdr.msg_seqno = htobe64(send_seqno);
    iov[0].iov_base = &hdr;
    iov[0].iov_len  = sizeof(struct qmsg_header);
    if (read_mq(qmeta->lpart.qpart_fd, payload, qmeta->msgsz,
                 qmeta->msgsz *send_seqno)) {
        free(payload);
        return READ_ERR;
    }
    iov[1].iov_base = payload;
    iov[1].iov_len = qmeta->msgsz;
    if (-1 == sock_send(socket, dst_ip, iov, 2)) {
        free(payload);
        return SOCKSND_ERR;
    }
    free(payload);
    return 0;
}

// Figure out whether  we need to send a new msg, retry sending earlier msg or
// do nothing. To send a new msg, apart from new msg being generated by any
// client, we also need an acknowledgment for the previous msg. If we didnt get
// acknowledgment for the previous msg for twice the last measured RTT, we retry
// sending the previous msg. If this was done SENDER_RETRY_CNT times, we mark
// remote member as DISCONNECTED. No more sending msg after this. sender_thread
// detects disconnection  and starts pinging remote member until it responds.
// Messages are sent to only CONNECTED remote members.
static int
try_sending_msg (struct lucid_controller *ctl, int qmeta_index,
                 int rmember_index) {
    uint64_t curr_time = 0,send_seqno = UINT64_MAX;
    int ret = -1;
    struct queue_metadata   *qmeta = ctl->qmeta[qmeta_index];
    struct remote_partition *rpart =
                            ctl->qmeta[qmeta_index]->rpart+rmember_index;

    uint64_t lpart_msg_acked_seqno = atomic_load_explicit(
                           &rpart->lpart_msg_acked_seqno, memory_order_relaxed);
    uint64_t lpart_msg_seqno = atomic_load_explicit(
                           &qmeta->lpart.lpart_msg_seqno, memory_order_relaxed);
    uint64_t lpart_msg_sent_seqno = atomic_load_explicit(
                            &rpart->lpart_msg_sent_seqno, memory_order_relaxed);
    uint64_t lpart_msg_sent_epoch = atomic_load_explicit(
                            &rpart->lpart_msg_sent_epoch, memory_order_relaxed);
    uint16_t rtt = atomic_load_explicit(&rpart->msg_rtt, memory_order_relaxed);

    send_seqno = lpart_msg_sent_seqno;
    if (lpart_msg_seqno == UINT64_MAX) // nothing to send
        return 0;

    if (lpart_msg_sent_seqno != UINT64_MAX &&             //sent first msg
         lpart_msg_sent_seqno == lpart_msg_seqno &&       //no more msg to send
         lpart_msg_sent_seqno == lpart_msg_acked_seqno)   // sent acked
       return 0;

    if (get_timestamp(&curr_time))
        return GETTIME_ERR;

    if (lpart_msg_sent_seqno == UINT64_MAX ||            // first msg not sent
         (lpart_msg_sent_seqno < lpart_msg_seqno &&       // more msg to send
         lpart_msg_sent_seqno == lpart_msg_acked_seqno)) {// sent acked
        log_mq("sender: nxtmsg (%lu, %lu) to %u\n", lpart_msg_sent_seqno,
                lpart_msg_seqno, rpart->member_ip);
        send_seqno++;
        rpart->msg_sent_retry_cnt = 0;
    }
    if (lpart_msg_sent_seqno != UINT64_MAX &&           //sent first msg
         lpart_msg_sent_seqno != lpart_msg_acked_seqno ) {//sent not acked
        if (curr_time - lpart_msg_sent_epoch < rtt<<1) {// timeout ?
            log_mq("sender: rtt %u(%lu, %lu)\n", rtt, curr_time,
                    lpart_msg_sent_epoch);
            return 0;
        }
        if (rpart->msg_sent_retry_cnt > SENDER_RETRY_CNT) { // retry exceeded
            uint8_t state = CONNECTED;
            atomic_compare_exchange_strong(&ctl->rmember[rmember_index].state,
                                           &state, DISCONNECTED);
            log_mq("sender: member %u DISCONNECTED\n", rpart->member_ip);
            rpart->msg_sent_retry_cnt = 0;
            return SEND_RETRY_ERR;
        }
        log_mq("sender: retrymsg (%lu, %lu) to %u\n", lpart_msg_sent_seqno,
               lpart_msg_acked_seqno, rpart->member_ip);
        rpart->msg_sent_retry_cnt +=1;
    }
    log_mq("sender: sending to member %u (%lu, %lu, %lu, %lu)\n",
           rpart->member_ip, lpart_msg_seqno, lpart_msg_acked_seqno,
           lpart_msg_sent_seqno, send_seqno);

    ret = send_msg(ctl->socket, qmeta, rpart->member_ip, send_seqno);
    if (ret < 0)
        return ret;
    atomic_store_explicit(&rpart->lpart_msg_sent_seqno, send_seqno,
                          memory_order_relaxed);
    atomic_store_explicit(&rpart->lpart_msg_sent_epoch, curr_time,
                          memory_order_relaxed);
    return 0;
}

// send a ping to remote member derived from 'rmember_index'
static int
send_ping (struct lucid_controller *ctl, int rmember_index) {
    uint32_t ping_seqno = 0;
    struct ping_header hdr = {0};
    struct iovec iov[1] = {0};
    ping_seqno = atomic_load_explicit(&ctl->rmember[rmember_index].ping_seqno,
                                      memory_order_relaxed);
    hdr.type = PING;
    hdr.ping_seqno = htobe32(ping_seqno);
    iov[0].iov_base = &hdr;
    iov[0].iov_len  = sizeof(struct ping_header);
    if (-1 == sock_send(ctl->socket, ctl->rmember[rmember_index].member_ip,
                        iov, 1))
        return SOCKSND_ERR;
    log_mq("sender: sent ping to %u seq %u\n",
           ctl->rmember[rmember_index].member_ip, ping_seqno);
    return 0;
}

// Launched as a thread by the daemon, sender thread takes the care of:
//  1. sending new msgs to all remote members if local client generates them.
//  2. retry sending a msg that has not been acked by specific remote member.
//  3. mark remote members DISCONNECTED and send ping to DISCONNECTED ones.
// Sender thread conditionally sleeps for SENDER_TIMEOUT_MS after runnig 1 job
// loop. It is woken up by receiver thread from its timed sleep if anyting
// is incoming. Pings are not resent for a minimum round trip time. Sender masks
// SIGHUP and SIGTERM so that only daemon receives it. Daemon requests
// exit upon receiving these signals. Sender obliges.

void *
sender_thread (void *arg) {
    struct lucid_controller *ctl = (struct lucid_controller *)arg;
    uint8_t need_to_ping = 1;
    sigset_t mask;
	sigemptyset(&mask);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
    log_mq("sender TID: %d\n", syscall(SYS_gettid));
     while(1) {
        int  ret = -1, i=0, j=0;
        struct timespec timeout = {0};
        uint64_t curr_time = 0, prev_time = 0;
        uint16_t  min_rtt = 0, rtt = 0;
        uint8_t state = DISCONNECTED;
        if (ctl->cleanup) {
            log_mq("sender: exiting\n");
            return (void *)0;
        }
        for (i=0; i< ctl->remote_mem_cnt; i++) {
            state = atomic_load_explicit(&ctl->rmember[i].state,
                                         memory_order_relaxed);
             if (state == DISCONNECTED && need_to_ping) {
                ret = send_ping(ctl, i);
                if (ret)
                   log_mq("sender: ping (err %d, %d)\n", ret, errno);
            }
        }
        for (i=0; i < ctl->queue_cnt; i++) {
            for (j=0; j < ctl->remote_mem_cnt; j++) {
                state = atomic_load_explicit(&ctl->rmember[j].state,
                                             memory_order_relaxed);
                if (state == DISCONNECTED)
                    continue;
                ret = try_sending_msg(ctl, i, j);
                if (ret < 0)
                    log_mq("sender: msg (err %d, %d)\n", ret, errno);
            }
        }
        if (get_timestamp(&curr_time)) {
            log_mq("sender: time (err %d)\n", errno);
            continue;
        }
        prev_time = curr_time;
        //sleep until timeout or woken up by receiver
        timeout.tv_sec = (curr_time + SENDER_TIMEOUT_MS)/1000;
        pthread_mutex_lock(&ctl->sender_mutex);
        pthread_cond_timedwait(&ctl->sender_cond, &ctl->sender_mutex, &timeout);
        pthread_mutex_unlock(&ctl->sender_mutex);

        if (get_timestamp(&curr_time)) {
            log_mq("sender: time (err %d)\n", errno);
            continue;
        }
        need_to_ping = (curr_time - prev_time > MIN_RTT) ? 1 : 0;
        log_mq("sender: awake\n");
    }
    return(void *)0;
}