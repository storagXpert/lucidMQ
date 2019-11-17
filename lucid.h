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

#ifndef LUCID_H
#define LUCID_H

#define MAX_NAME          32        // max len of queue name
#define MAX_PATH          255       // max len of dir path of queue storage
#define LUCID_PORT        49160     // dynamic port for daemon communication
#define MAX_MSGSZ         1400      // max msg size capped at 1400 Bytes
#define MIN_RTT           100       // MIN_RTT for sender retry in ms
#define SENDER_TIMEOUT_MS (MIN_RTT*40) // idle sender can sleep for 4s
#define RECVER_TIMEOUT_MS 4000      // idle receiver unblocks on socket every 5s

#define SENDER_RETRY_CNT  3         // retry sending msg before disconnecting
#define PAGESZ  sysconf(_SC_PAGESIZE) // max queue meta size

#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <arpa/inet.h>
#include <libxml/parser.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <pthread.h>
#include <stdarg.h>
#include <fcntl.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>

// structure used for logging
struct loginfo {
    FILE* file;
    pthread_mutex_t lock;
};

// global variable instantiated in util.c for logging
extern struct loginfo logger;

// state of remote members as seen by this local host
typedef enum __attribute__((packed)) {
    CONNECTED       = 0,
    DISCONNECTED    = 1,
} member_state;

// types of messages that go on the wire
typedef enum __attribute__((packed)) {
    QMSG            = 0,
    QMSG_ACK        = 1,
    PING            = 2,
    PING_ACK        = 3,
} header_type;

// packed header for all QMSG and QMSG_ACK type of msgs
struct qmsg_header {
    uint8_t      type;
    uint32_t     qid;
    union {
        uint64_t     msg_seqno;
        uint64_t     msg_ack_seqno;
    };
} __attribute__ ((packed));

// packed header for all PING and PING_ACK type of msgs
struct ping_header {
    uint8_t      type;
    union {
        uint32_t    ping_seqno;
        uint32_t    ping_ack_seqno;
    };
} __attribute__ ((packed));


// structure is part of queue_metadata
struct local_partition {
    int                     qpart_fd;
    uint32_t                member_ip;
    pthread_mutex_t         client_write_lock;
    atomic_uint_fast64_t    lpart_msg_seqno;
};

// structure is part of queue_metadata
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

// structure represents the binary format of a queue metadata file on disk.
// it is mmaped by the daemon and all clients who wish to use the queue.
// can extend upto _SC_PAGESIZE. Provides persistence in face
// of node crash and allows for stateless operations on queues.
struct queue_metadata {
    char                    qname[MAX_NAME+1];
    uint32_t                qid;
    uint32_t                msgsz;
    uint16_t                remote_mem_cnt;
    struct local_partition  lpart;
    struct remote_partition rpart[];
};

// structure is part of lucid_controller
struct remote_member {
    uint32_t                member_ip;
    atomic_int_fast8_t      state;
    atomic_uint_fast32_t    ping_seqno;

};

// structure populated from parsed xml configuration and mmaped queue metadata
// serves as an in memory object for all operations
struct lucid_controller {
    uint16_t                queue_cnt;
    uint16_t                remote_mem_cnt;
    int                     socket;
    struct queue_metadata   **qmeta;
    struct remote_member    *rmember;
    pthread_cond_t          sender_cond;
    pthread_mutex_t         sender_mutex;
    uint8_t                 cleanup;
    uint8_t                 recv_buf[MAX_MSGSZ];

};

// structure is part of lucid_conf
struct queue_conf {
    unsigned char           *qname;
    uint32_t                qid;
    uint32_t                msgsz;
};

// structure populated after parsing xml configuration file
struct lucid_conf {
     unsigned char          *storage_dir;
     uint32_t               queue_cnt;
     uint32_t               lmember_ip;
     uint32_t               remote_mem_cnt;
     uint32_t               *rmember_ip;
     struct queue_conf      *queue;
};

// errors returned by various functions as integers
typedef enum {
    QMETA_NOTFND       = -100,
    RMEMB_QMETA_NOTFND = -101,
    RMEMB_NOTFND       = -102,
    GETTIME_ERR        = -103,
    WRITE_ERR          = -104,
    READ_ERR           = -103,
    SOCKSND_ERR        = -105,
    SOCKRECV_ERR       = -106,
    PINGSEQ_ERR        = -107,
    SEND_RETRY_ERR     = -108,
    CONF_DIR_NOTFND    = -109,
    CONF_INVALID_IP    = -110,
    CONF_RMEMB_NOTFND  = -111,
    CONF_QUEUE_NOTFND  = -112,
    CONF_HASH_CLN      = -113,
    CONF_VALIDATE_ERR  = -114,
    SOCK_CREAT_ERR     = -115,
    CTRL_QFILESZ_ERR   = -116,
    CTRL_QFILE_ERR     = -117,
    CTRL_MMAP_ERR      = -118,
    CTRL_MFILE_ERR     = -119,
    MEM_FAIL           = -120,
    MUTEX_FAIL         = -121,
    MSGACKSEQ_ERR      = -122,
    QDIR_CREAT_ERR     = -123
} errors;

// function declarations : util.c
int sock_send (int socket, uint32_t dst_ip, struct iovec *buf, int buf_cnt);
int sock_recv (int socket, uint32_t *src_ip, struct iovec *buf, int buf_cnt);
int sock_create (uint32_t src_ip, int timeout_ms );
int get_timestamp (uint64_t *stamp);
uint32_t hash (const unsigned char* text);
int write_mq (int fd, const void *buf, size_t count, off_t offset);
int read_mq (int fd, void *buf, size_t count, off_t offset);
void log_mq( const char *fmt, ...);

// function declarations : initconf.c
int init_controller(struct lucid_controller **ctlp, struct lucid_conf *conf);
void free_controller(struct lucid_controller *ctl);
int init_qmeta_files(struct lucid_conf *conf);

// function declarations : xmlconf.c
struct lucid_conf *get_configuration(char *conf_file);

// function declarations : sender.c
void *sender_thread (void *arg);

// function declarations : receiver.c
void *receiver_thread (void *arg);

#endif // LUCID_H
