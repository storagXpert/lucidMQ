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

// compare_* functions below are used for binary searching respective sorted
// arrays for faster lookup in the receive path of msgs and pings. This is
// required for scaling to large number of queues and (or) servers.
static int
compare_qid (const void *qmeta1, const void *qmeta2) {
    return ((*(struct queue_metadata **)qmeta1))->qid -
            ((*(struct queue_metadata **)qmeta2))->qid;
}

static int
compare_rpart (const void *rpart1, const void *rpart2) {
    return ((struct remote_partition *)rpart1)->member_ip -
            ((struct remote_partition *)rpart2)->member_ip;
}

static int
compare_rmember (const void *rmemb1, const void *rmemb2) {
    return ((struct remote_member *)rmemb1)->member_ip -
            ((struct remote_member *)rmemb2)->member_ip;
}

// Given a qid, figure out which mmaped queue metadata, stashed in a sorted
// array of metadata pointers in the controller, we are dealing with here.
static struct queue_metadata *
get_qmeta_from_qid (struct lucid_controller *ctl, uint32_t qid) {

    struct queue_metadata   qmeta_tmp = {0};
    struct queue_metadata   *qmeta_key = NULL;
    struct queue_metadata   **qmeta = NULL;

    qmeta_tmp.qid = qid;
    qmeta_key = &qmeta_tmp;
    qmeta = bsearch(&qmeta_key, ctl->qmeta, ctl->queue_cnt,
                    sizeof(struct queue_metadata *), compare_qid);
    if (!qmeta)
        return NULL;
    return *qmeta;
}

// Given an ip and mmaped queue metadata, figure out which remote member's queue
// partition we need. On disk remote partition metadata are kept in a sorted
// order as per remote member's ip.
static struct remote_partition *
get_rpart_from_qmeta_and_ip (struct lucid_controller *ctl,
                             struct queue_metadata *qmeta, uint32_t ip) {

    struct remote_partition *rpart = NULL;
    struct remote_partition rpart_key = {0};

    rpart_key.member_ip = ip;
    rpart = bsearch(&rpart_key, qmeta->rpart, ctl->remote_mem_cnt,
                    sizeof(struct remote_partition), compare_rpart);
    return rpart;
}

// Given an ip, figure out the remote member, stashed in the controller.
static struct remote_member *
get_rmember_from_ip (struct lucid_controller *ctl, uint32_t ip) {
    struct remote_member *rmemb = NULL;
    struct remote_member rmemb_key = {0};

    rmemb_key.member_ip = ip;
    rmemb = bsearch(&rmemb_key, ctl->rmember, ctl->remote_mem_cnt,
                    sizeof(struct remote_member), compare_rmember);
    return rmemb;
}

// When a queue msg comes, accept if its seq no is 1 more than what was last
// received. Write the msg to queue's remote partition. Schedule immediate page
// flush to make msg and queue metadata hit the disk. Client's lmq_send call
// synchronizes msg written locally to disk. Daemon writes only to remote
// partitions on msg receive, therefore only receive path needs to push pages
// to disk. Furthermore, we send msg acknowledgement with what was received,
// regardless of whether we accept the incoming msg or not. This allows remote
// sender to synchronize with us even in face of node crash or network issues.
static int
handle_queue_msg (struct lucid_controller *ctl, uint32_t src_ip) {
    struct queue_metadata   *qmeta = NULL;
    struct remote_partition *rpart = NULL;
    uint64_t                hdr_msg_seqno, rpart_msg_recv_seqno;
    struct qmsg_header      *hdr = NULL;
    struct qmsg_header      ack_hdr = {0};
    struct iovec            iov[1] = {0};

    hdr = (struct qmsg_header *)ctl->recv_buf;
    qmeta = get_qmeta_from_qid(ctl, be32toh(hdr->qid));
    if (!qmeta)
        return QMETA_NOTFND;
    rpart = get_rpart_from_qmeta_and_ip(ctl, qmeta, src_ip);
    if (!rpart)
        return RMEMB_QMETA_NOTFND;
    hdr_msg_seqno = be64toh(hdr->msg_seqno);
    rpart_msg_recv_seqno = atomic_load_explicit(&rpart->rpart_msg_recv_seqno,
                                          memory_order_relaxed);
    log_mq("receiver: msg from %u (%lu, %lu)\n", src_ip, hdr_msg_seqno,
           rpart_msg_recv_seqno);
    // initialize ack setting with last received msg
    ack_hdr.msg_ack_seqno = htobe64(rpart_msg_recv_seqno);

    if (hdr_msg_seqno == rpart_msg_recv_seqno + 1) { // correct next msg
        if (write_mq(rpart->qpart_fd, ctl->recv_buf +sizeof(struct qmsg_header),
                     qmeta->msgsz, qmeta->msgsz * hdr_msg_seqno))
            return WRITE_ERR;
        atomic_store_explicit(&rpart->rpart_msg_recv_seqno, hdr_msg_seqno,
                              memory_order_relaxed);
        // Schedule memory flush to disk for persistence
        sync_file_range(rpart->qpart_fd, qmeta->msgsz *hdr_msg_seqno,
                        qmeta->msgsz, SYNC_FILE_RANGE_WRITE);
        msync(qmeta, PAGESZ, MS_ASYNC);

        // change ack setting to latest received msg
        ack_hdr.msg_ack_seqno = hdr->msg_seqno;
    }
    // send ack
    ack_hdr.type = QMSG_ACK;
    ack_hdr.qid = htobe32(qmeta->qid);
    iov[0].iov_base = &ack_hdr;
    iov[0].iov_len  = sizeof(struct qmsg_header);
    if (-1 == sock_send(ctl->socket, src_ip, iov, 1))
        return SOCKSND_ERR;
    return 0;
}

// When we receive acknowledgment for a msg that we sent, we update so that we
// dont retry the last sent msg. If ack has seqno that we didnt anticipate, we
// ignore it. Round Trip Time for msgs is updated, since msg retry timeout
// in case of lost ack depends on it. Also, wakeup sender so that it can
// send next msg if it needs to, without delay.
static int
handle_queue_msg_ack (struct lucid_controller *ctl, uint32_t src_ip) {
    struct queue_metadata    *qmeta = NULL;
    struct remote_partition  *rpart = NULL;
    uint64_t                 curr_time, lpart_msg_sent_epoch,
                             lpart_msg_sent_seqno, hdr_msg_ack_seqno;
    uint16_t                 rtt = 0;
    uint8_t                  state = DISCONNECTED;
    struct qmsg_header       *hdr = NULL;


    hdr = (struct qmsg_header *)ctl->recv_buf;
    qmeta = get_qmeta_from_qid(ctl, be32toh(hdr->qid));
    if (!qmeta)
        return QMETA_NOTFND;
    rpart = get_rpart_from_qmeta_and_ip(ctl, qmeta, src_ip);
    if (!rpart)
        return RMEMB_QMETA_NOTFND;
    hdr_msg_ack_seqno = be64toh(hdr->msg_ack_seqno);
    lpart_msg_sent_seqno = atomic_load_explicit(&rpart->lpart_msg_sent_seqno,
                                                memory_order_relaxed);

    if (lpart_msg_sent_seqno == hdr_msg_ack_seqno) { // correct ack received
        atomic_store_explicit(&rpart->lpart_msg_acked_seqno, hdr_msg_ack_seqno,
                              memory_order_relaxed);
        lpart_msg_sent_epoch = atomic_load_explicit(
                            &rpart->lpart_msg_sent_epoch, memory_order_relaxed);
        if (get_timestamp(&curr_time))
            return GETTIME_ERR;
        // update round trip time of last msg
        rtt = curr_time - lpart_msg_sent_epoch;
        rtt  = rtt < MIN_RTT ? MIN_RTT : rtt;
        atomic_store_explicit(&rpart->msg_rtt, rtt, memory_order_relaxed);
    } else { // ack for something that we didnt send last? dont care, but log it
        log_mq("receiver: bad msg ack from %u (%lu, %lu) rtt %u (%lu, %lu)\n",
               src_ip, hdr_msg_ack_seqno, lpart_msg_sent_seqno, rtt, curr_time,
               lpart_msg_sent_epoch);
        return MSGACKSEQ_ERR;
    }
    log_mq("receiver: msg ack from %u (%lu) rtt %u (%lu, %lu)\n", src_ip,
            hdr_msg_ack_seqno, rtt, curr_time, lpart_msg_sent_epoch);
    //wakeup sender
    pthread_mutex_lock(&ctl->sender_mutex);
    pthread_cond_signal(&ctl->sender_cond);
    pthread_mutex_unlock(&ctl->sender_mutex);
    return 0;
}

// Got ping from a remote member. Send acknowledgment for ping back to sender.
// Wakeup the sender so that we could send our own ping and CONNECT with
// it immediately, thereby allowing us to process ready to send local msgs.
// Worthwhile to note that a remote member could be in DISCONNECTED state and
// we can still recieve msgs from it.Its only when we want to send msg to a
// remote member, we need it to be in CONNECTED state.
static int
handle_ping (struct lucid_controller *ctl, uint32_t src_ip) {
    struct remote_member *rmemb = NULL;
    uint32_t             ping_seqno = 0;
    struct ping_header   *hdr = NULL;
    struct ping_header   ack_hdr = {0};
    struct iovec iov[1] = {0};

    hdr = (struct ping_header *)ctl->recv_buf;
    rmemb = get_rmember_from_ip(ctl, src_ip);
    if (!rmemb)
        return RMEMB_NOTFND;
    ping_seqno = be32toh(hdr->ping_seqno);
    log_mq("receiver:ping from %u (%u)\n", src_ip, ping_seqno);

    ack_hdr.type = PING_ACK;
    ack_hdr.ping_ack_seqno = hdr->ping_seqno;
    iov[0].iov_base = &ack_hdr;
    iov[0].iov_len  = sizeof(struct ping_header);
    if (-1 == sock_send(ctl->socket, src_ip, iov, 1))
        return SOCKSND_ERR;

    //wakeup sender
    pthread_mutex_lock(&ctl->sender_mutex);
    pthread_cond_signal(&ctl->sender_cond);
    pthread_mutex_unlock(&ctl->sender_mutex);
    return 0;
}

// When we receive acknowledgment for a ping that we sent, remote member state
// is changed to CONNECTED.If ack has seqno that we didnt anticipate, ignore it.
// Wakeup sender so that ready to send local msgs could be processed.
static int
handle_ping_ack (struct lucid_controller *ctl, uint32_t src_ip) {
    struct remote_member *rmemb = NULL;
    uint8_t              state = DISCONNECTED;
    uint32_t             ping_seqno = 0,ping_ack_seqno = 0;
    struct ping_header   *hdr = NULL;
    struct ping_header   ack_hdr = {0};
    struct iovec iov[1] = {0};

    hdr = (struct ping_header *)ctl->recv_buf;
    rmemb = get_rmember_from_ip(ctl, src_ip);
    if (!rmemb)
        return RMEMB_NOTFND;
    ping_ack_seqno = be32toh(hdr->ping_ack_seqno);
    ping_seqno = atomic_load_explicit(&rmemb->ping_seqno, memory_order_relaxed);
    if (ping_seqno != ping_ack_seqno) {
        log_mq("receiver: bad ping ack from %u (%u, %u)\n", src_ip,
               ping_seqno, ping_ack_seqno);
        return PINGSEQ_ERR;
    }
    log_mq("receiver: ping ack from %u (%u)\n", src_ip, ping_seqno);
    ping_seqno++;
    atomic_store_explicit(&rmemb->ping_seqno, ping_seqno, memory_order_relaxed);
    atomic_compare_exchange_strong(&rmemb->state, &state, CONNECTED);
    if (state == DISCONNECTED)
        log_mq("receiver: member %u CONNECTED\n", rmemb->member_ip);

    //wakeup sender
    pthread_mutex_lock(&ctl->sender_mutex);
    pthread_cond_signal(&ctl->sender_cond);
    pthread_mutex_unlock(&ctl->sender_mutex);
    return 0;
}

// Launched as a thread by the daemon, receiver thread takes care of:
//  1. accept packet from all remote members and process them as per their type.
//  2. Break  sender thread's timed wait for urgent processing
//  3. mark remote members CONNECTED upon successful ping acknowledgment.
// Receiver thread blocks on socket for incoming msgs. It masks SIGHUP and
// SIGTERM so that only daemon receives it. Daemon requests exit upon
// receiving these signals. Receiver unblocks after every RECVER_TIMEOUT_MS to
// check for such request from daemon for exit.
void *
receiver_thread (void *arg) {
    struct lucid_controller *ctl = (struct lucid_controller *)arg;
    sigset_t mask;
	sigemptyset(&mask);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
    log_mq("receiver TID: %d\n", syscall(SYS_gettid));
     while(1) {
        int ret = -1;
        uint32_t src_ip = 0;
        struct iovec iov[1] = {0};
        if (ctl->cleanup) {
            log_mq("receiver:exiting\n");
            return (void *)0;
        }
        iov[0].iov_base = ctl->recv_buf;
        iov[0].iov_len  = MAX_MSGSZ;
        if (-1 == sock_recv(ctl->socket, &src_ip, iov, 1)) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (ctl->cleanup) {
                     log_mq("receiver:exiting\n");
                     return (void *)0;
                }
                continue;
            }
            log_mq("receiver: sock_recv (err %d)\n", errno);
            continue;
        }
        switch (ctl->recv_buf[0]) {
            case QMSG :
                ret = handle_queue_msg(ctl, src_ip);
                if (ret < 0)
                    log_mq("receiver: queue_msg (err %d, %d)\n", ret, errno);
                break;
            case QMSG_ACK :
                ret = handle_queue_msg_ack(ctl, src_ip);
                if (ret < 0)
                    log_mq("receiver: queue_msg_ack (err %d, %d)\n", ret,errno);
                break;
             case PING :
                ret = handle_ping(ctl, src_ip);
                if (ret < 0)
                    log_mq("receiver: ping (err %d, %d)\n", ret, errno);
                break;
            case PING_ACK :
                ret = handle_ping_ack(ctl, src_ip);
                if (ret < 0)
                    log_mq("receiver: ping_ack (err %d, %d)\n", ret, errno);
                break;
            default :
                log_mq("receiver: unknown header\n");
        }
    }
    return (void *)0;
}
