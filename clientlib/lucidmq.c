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

#include "lucidmq.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdatomic.h>
#include <pthread.h>
#include <stdint.h>
#include <limits.h>
#include <sys/mman.h>
#define MAX_NAME          32        // max len of queue name
#define MAX_PATH          255       // max len of dir path of queue storage
#define PAGESZ            sysconf(_SC_PAGESIZE)

// Structure definitions copied from lucid.h for reading on-disk queue metadata
// file. Make sure to update these if structure definitions in lucid.h changes.
struct local_partition {
    int                     qpart_fd;
    uint32_t                member_ip;
    pthread_mutex_t         client_write_lock;
    atomic_uint_fast64_t    lpart_msg_seqno;
};
struct remote_partition {
    int                     qpart_fd;
    uint32_t                member_ip;
    atomic_uint_fast64_t    rpart_msg_recv_seqno;
    uint64_t                lpart_msg_sent_seqno;
    atomic_uint_fast64_t    lpart_msg_acked_seqno;
    atomic_uint_fast64_t    lpart_msg_sent_epoch;
    atomic_uint_fast16_t    msg_rtt;
    uint8_t                 msg_sent_retry_cnt;
};
struct queue_metadata {
    char                    qname[MAX_NAME+1];
    uint32_t                qid;
    uint32_t                msgsz;
    uint16_t                remote_mem_cnt;
    struct local_partition  lpart;
    struct remote_partition rpart[];
};

// library maintains its own structure copying some of mmaped queue metdata.
// This is done to protect client during dynamic reconfiguration of daemon
// as well as  accidental overwrite
struct qpartition {
    int                     qpart_fd;
    uint32_t                member_ip;
    atomic_uint_fast64_t    *msg_seqno_ptr;
    uint64_t                msg_read;
};

struct client_qmeta {
    uint32_t                msgsz;
    uint16_t                remote_mem_cnt;
    pthread_mutex_t         *write_lock;
    struct qpartition       local;
    struct qpartition       *remote;
};

// internal structure exposed as opaque pointer or handle in library header
// This is what gets passed around during lmq_* calls
struct lmq_msgq {
    struct queue_metadata  *qmeta;
    struct client_qmeta    *cmeta;
};

// Used by lmq_open for initializing library structure client_qmeta from on-disk
// queue metadata structure that is mmaped during open
static int
init_client_qmeta (struct client_qmeta *cmeta, struct queue_metadata *qmeta,
                   const char *qpath, const char *qname) {
    char qpartfile[MAX_PATH + MAX_NAME + 11] = {0};
    int qfd = -1, flag = O_RDWR|O_NONBLOCK;
    snprintf(qpartfile, MAX_PATH + MAX_NAME, "%s/%s.%u", qpath, qname,
             qmeta->lpart.member_ip);
    qfd = open(qpartfile, flag);
    if (qfd == -1) {
        printf("open : init qfile %s err(%d)\n", qpartfile, errno);
        free(cmeta->remote);
        return -1;
    }
    cmeta->msgsz               = qmeta->msgsz;
    cmeta->remote_mem_cnt      = qmeta->remote_mem_cnt;
    cmeta->write_lock          = &qmeta->lpart.client_write_lock;
    cmeta->local.qpart_fd      = qfd;
    cmeta->local.member_ip     = qmeta->lpart.member_ip;
    cmeta->local.msg_seqno_ptr = &qmeta->lpart.lpart_msg_seqno;
    cmeta->local.msg_read      = UINT64_MAX;
    cmeta->remote = (struct qpartition *)malloc(cmeta->remote_mem_cnt *
                                                sizeof(struct qpartition));
    if (!cmeta->remote)
        return -1;
    for (int j=0; j < cmeta->remote_mem_cnt; j++) {
        snprintf(qpartfile, MAX_PATH + MAX_NAME, "%s/%s.%u", qpath, qname,
                 qmeta->rpart[j].member_ip);
        qfd = open(qpartfile, flag);
        if (qfd == -1) {
            printf("open : init qfile %s err(%d)\n", qpartfile, errno);
            free(cmeta->remote);
            return -1;
        }
        cmeta->remote[j].qpart_fd      = qfd;
        cmeta->remote[j].member_ip     = qmeta->rpart[j].member_ip;
        cmeta->remote[j].msg_seqno_ptr = &qmeta->rpart[j].rpart_msg_recv_seqno;
        cmeta->remote[j].msg_read      = UINT64_MAX;
    }
    return 0;
}

// utility function for writing to a file
static int
write_mq (int fd, const void *buf, size_t count, off_t offset) {
    ssize_t len = -1;
    while (count > 0) {
        do {
             len = pwrite(fd, buf, count, offset);
        } while (len < 0 && errno == EINTR);
        if (len < 0)
            return errno;
        count -= len;
        buf += len;
        offset += len;
    }
    return 0;
}

// utility function for reading from a file
static  int
read_mq (int fd, void *buf, size_t count, off_t offset) {
    ssize_t len = -1;
    while (count > 0) {
        do {
             len = pread(fd, buf, count, offset);
        } while (len < 0 && errno == EINTR);
        if (len < 0)
            return errno;
        if (!len)
            return len;
        count -= len;
        buf += len;
        offset +=len;
    }
    return 0;
}

// Open an existing queue with name 'qname' located in directory path 'qpath'.
// Return an opaque handle to client for other lmq_* operations
struct lmq_msgq *
lmq_open (const char *qpath, const char *qname) {
    struct lmq_msgq *q = NULL;
    int qfd = -1, ret = -1, flag = O_RDWR|O_NONBLOCK;
    char filename[MAX_PATH + MAX_NAME + 11] = {0};
    if (!qpath || !qname)
        return NULL;

    snprintf(filename, MAX_PATH + MAX_NAME, "%s/%s.meta",qpath, qname);
    qfd = open(filename, flag);
    if (qfd == -1) {
        printf("open : file %s err(%d)\n", filename, errno);
        free(q);
        return NULL;
    }
    q = (struct lmq_msgq*)malloc(sizeof(struct lmq_msgq));
    if (!q)
        return NULL;
    memset(q, 0, sizeof(struct lmq_msgq));
    q->qmeta = (struct queue_metadata *)mmap(NULL, PAGESZ, PROT_READ|PROT_WRITE,
                                             MAP_SHARED, qfd, 0);
    if (q->qmeta == MAP_FAILED) {
        printf("open : mmap err(%d)\n", errno);
        close(qfd);
        free(q);
        return NULL;
    }

    q->cmeta = (struct client_qmeta *)malloc(sizeof(struct client_qmeta));
    if (!q->cmeta) {
        close(qfd);
        munmap(q->qmeta, PAGESZ);
        free(q);
        return NULL;
    }
    memset(q->cmeta, 0, sizeof(struct client_qmeta));
    close(qfd);
    ret = init_client_qmeta(q->cmeta, q->qmeta, qpath, qname);
    if (ret < 0) {
        printf("open : init err(%d, %d)\n", ret, errno);
        free(q->cmeta);
        munmap(q->qmeta, PAGESZ);
        free(q);
        return NULL;
    }
    return q;
}

// Send msg of length msglen to queue. msg buffer is not freed here.
// Make sure to use msglen of same size as queue msg size.
// Eventhough write_lock mutex is created by daemon it does not use it.
// This lock is meant for synchronizing between multiple writers to a queue on
// local host. The mutex is roboust, i.e, if client crashes holding the lock,
// other clients can detect dead owner and move on. We also schedule immediate
// page flush to make msg and queue metadata hit the disk ensuring persistency
int
lmq_send (struct lmq_msgq *q, const void *msg, unsigned int msglen) {
    uint64_t curr_seqno = 0;
    int ret = -1;
    if (!q || !msg || !msglen || msglen != q->cmeta->msgsz) {
        printf("send : invalid arg\n");
        return -1;
    }
    ret = pthread_mutex_lock(q->cmeta->write_lock);
    if (ret == EOWNERDEAD) {
        pthread_mutex_consistent(q->cmeta->write_lock);
        ret = pthread_mutex_lock(q->cmeta->write_lock);
    }
    if (ret) {
        printf("send : lock err(%d)\n", ret);
        return -1;
    }
    curr_seqno = atomic_load_explicit(q->cmeta->local.msg_seqno_ptr,
                                      memory_order_relaxed);
    ret = write_mq(q->cmeta->local.qpart_fd, msg, msglen,
                   (curr_seqno+1) * msglen);
    if (ret) {
        pthread_mutex_unlock(q->cmeta->write_lock);
        printf("send : write err(%d)\n", ret);
        return -1;
    }
    atomic_fetch_add_explicit(q->cmeta->local.msg_seqno_ptr, 1,
                              memory_order_relaxed);
    pthread_mutex_unlock(q->cmeta->write_lock);
    // Schedule memory flush to disk for persistence
    sync_file_range(q->cmeta->local.qpart_fd, (curr_seqno+1) * msglen,
                    msglen, SYNC_FILE_RANGE_WRITE);
    msync(q->qmeta, PAGESZ, MS_ASYNC);
    return 0;
}

// Read msg queue from start and return msgs one at time. Library maintains
// count of msgs read and reads next msg everytime lmq_recv is called. If
// When RECV_LOCAL flag is specified only local partition of queue is read.This
// means that you would see msgs that were written by clients on this host only.
// When RECV_REMOTE flag is specified only remote partition(s) of queue is read.
// This means that you would see msgs that were written by clients on remote
// host(s) that reached local host. Both flags could ORed. Return -1 if no msgs
//to be read or error occured.
int
lmq_recv (struct lmq_msgq *q, void *msg, unsigned int msglen, int flag) {
    uint64_t curr_seqno = 0;
    int ret = -1;
    if (!q || !msg || !msglen || msglen != q->cmeta->msgsz) {
        printf("recv : invalid arg\n");
        return -1;
    }
    if (flag & RECV_REMOTE) { // read from remote partitions
        for (int i=0; i < q->cmeta->remote_mem_cnt; i++) {
            curr_seqno = atomic_load_explicit(q->cmeta->remote[i].msg_seqno_ptr,
                                                memory_order_relaxed);
            if (curr_seqno == UINT64_MAX)
                continue;
            if (q->cmeta->remote[i].msg_read == UINT64_MAX ||
                q->cmeta->remote[i].msg_read < curr_seqno) {
                ret = read_mq(q->cmeta->remote[i].qpart_fd, msg, msglen,
                              msglen * (q->cmeta->remote[i].msg_read+1));
                if (ret) {
                    printf("recv : read err(%d)\n", ret);
                    return -1;
                }
                q->cmeta->remote[i].msg_read += 1;
                return 0;
            }
        }
    }
    if (flag & RECV_LOCAL) { // read from local partition
        curr_seqno = atomic_load_explicit(q->cmeta->local.msg_seqno_ptr,
                                          memory_order_relaxed);
        if (curr_seqno == UINT64_MAX)
            return -1;
        if (q->cmeta->local.msg_read == UINT64_MAX ||
            q->cmeta->local.msg_read < curr_seqno) {
            ret = read_mq(q->cmeta->local.qpart_fd, msg, msglen,
                          msglen * (q->cmeta->local.msg_read+1));
            if (ret) {
                printf("recv : read err(%d)\n", ret);
                return -1;
            }
            q->cmeta->local.msg_read += 1;
            return 0;
        }
    }
    return -1;  // no msgs found
}

// close queue handle and free up resources.
void
lmq_close (lmq_hdl q) {
    if (!q)
        return;
    close(q->cmeta->local.qpart_fd);
     for (int j=0; j < q->cmeta->remote_mem_cnt; j++)
          close(q->cmeta->remote[j].qpart_fd);
    munmap(q->qmeta, PAGESZ);
    free(q->cmeta->remote);
    free(q->cmeta);
    free(q);
}


