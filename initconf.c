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

// used by qsort for sorting queue metadata by qid within controller structure
static int
compare_qid (const void *qmeta1, const void *qmeta2) {
    return ((*(struct queue_metadata **)qmeta1))->qid -
            ((*(struct queue_metadata **)qmeta2))->qid;
}

// Make sure that queue metadata file exists and queue configuration read from
// xml is same as what is read from metadata file.This would change once dynamic
// reconfiguration is supported.
static int
validate_qmeta_file(const char *filename, struct lucid_conf *conf, int qindex) {
    int qfd = -1, flag = O_RDWR|O_NONBLOCK;
    struct queue_metadata *qmeta = NULL;
    qfd = open(filename, flag);
    if (qfd == -1)
        return CTRL_MFILE_ERR;
    qmeta = (struct queue_metadata *)mmap(NULL, PAGESZ, PROT_READ, MAP_SHARED,
                                           qfd, 0);
    if (qmeta == MAP_FAILED) {
        close(qfd);
        return CTRL_MMAP_ERR;
    }
    close(qfd);
    if (conf->queue[qindex].qid   !=  qmeta->qid ||
        conf->queue[qindex].msgsz != qmeta->msgsz ||
        conf->lmember_ip          != qmeta->lpart.member_ip  ||
        conf->remote_mem_cnt      != qmeta->remote_mem_cnt) {
            munmap(qmeta, PAGESZ);
            return CONF_VALIDATE_ERR;
    }
    for (int i=0; i<qmeta->remote_mem_cnt; i++) {
        if (qmeta->rpart[i].member_ip != conf->rmember_ip[i]) {
            munmap(qmeta, PAGESZ);
            return CONF_VALIDATE_ERR;
        }
    }
    return 0;
}

// allocate memory for the controller after xml configuration is read and
// memory usage can be estimated
static struct lucid_controller *
allocate_controller(struct lucid_conf *conf) {
    struct lucid_controller * ctl = NULL;

    ctl = (struct lucid_controller *)malloc(sizeof(struct lucid_controller));
    if (!ctl)
        return NULL;
    memset(ctl, 0, sizeof(struct lucid_controller));
    ctl->qmeta = (struct queue_metadata **)malloc(conf->queue_cnt *
                                               sizeof(struct queue_metadata*));
    if (!ctl->qmeta) {
        free(ctl);
        return NULL;
    }
    ctl->queue_cnt = conf->queue_cnt;
    memset(ctl->qmeta, -1, ctl->queue_cnt * sizeof(struct queue_metadata*));
    ctl->rmember = (struct remote_member *)malloc(conf->remote_mem_cnt *
                                                  sizeof(struct remote_member));
    if (!ctl->rmember) {
        free(ctl->qmeta);
        free(ctl);
        return NULL;
    }
    ctl->remote_mem_cnt = conf->remote_mem_cnt;
    memset(ctl->rmember, 0, ctl->remote_mem_cnt * sizeof(struct remote_member));
    return ctl;
}

// free controller memory. Make sure that if files where opened and their fds
// where stashed in the controller, they are closed. Also, unmap queue metadata
void
free_controller(struct lucid_controller *ctl) {
    if (!ctl)
        return;
    if (ctl->rmember)
        free(ctl->rmember);
    if (ctl->qmeta) {
        for (int i=0; i < ctl->queue_cnt && ctl->qmeta[i] != NULL &&
                      ctl->qmeta[i] != MAP_FAILED; i++) {
            if (-1 != ctl->qmeta[i]->lpart.qpart_fd)
                close(ctl->qmeta[i]->lpart.qpart_fd);

            for ( int j=0; j<ctl->remote_mem_cnt; j++)
                if (-1 != ctl->qmeta[i]->rpart[j].qpart_fd)
                    close(ctl->qmeta[i]->rpart[j].qpart_fd);
            munmap(ctl->qmeta[i], PAGESZ);
        }
        free(ctl->qmeta);
    }
    pthread_cond_destroy(&ctl->sender_cond);
    pthread_mutex_destroy(&ctl->sender_mutex);
    free(ctl);
    return;
};

// Create queue metadata file for all the queues mentioned in xml configuration
// if they dont exist. If they exist, only validate them.Also, create queue
// partition files both local and remote, for each queue if queue metadata file
// is being created for the first time.
int
init_qmeta_files(struct lucid_conf *conf) {
    struct queue_metadata *qmeta = NULL;
    pthread_mutexattr_t attrs;
    int qmeta_len = sizeof(*qmeta) +
                      sizeof(struct remote_partition) * conf->remote_mem_cnt;
    qmeta = (struct queue_metadata *)malloc(qmeta_len);
    if (!qmeta) {
        log_mq("controller:file(err %d)\n", MEM_FAIL);
        return MEM_FAIL;
    }
    memset(qmeta, 0, qmeta_len);
    for (int i=0; i<conf->queue_cnt; i++) {
        int qfd = -1, ret = -1;
        int flag = O_CREAT|O_EXCL|O_RDWR|O_NONBLOCK;
        int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH;
        char filename[MAX_PATH + MAX_NAME + 1] = {0};
        char qpart_file[MAX_PATH + MAX_NAME +11] = {0};

        snprintf(filename, MAX_PATH + MAX_NAME, "%s/%s.meta", conf->storage_dir,
                 conf->queue[i].qname);
        qfd = open(filename, flag, mode);
        if (qfd == -1) {
            if (errno == EEXIST) {
                ret = validate_qmeta_file(filename, conf, i);
                if (ret < 0) {
                    log_mq("controller:file(err %d)\n", ret);
                    free(qmeta);
                    return ret;
                }
                continue;
            } else {
                log_mq("controller:file(err %d, %d)\n", CTRL_MFILE_ERR, errno);
                free(qmeta);
                return CTRL_MFILE_ERR;
            }
        }
        if (-1 == ftruncate(qfd, PAGESZ)) {
            close(qfd);
            log_mq("controller:file(err %d, %d)\n", CTRL_QFILESZ_ERR, errno);
            free(qmeta);
            return CTRL_QFILESZ_ERR;
        }

        strncpy(qmeta->qname, conf->queue[i].qname, MAX_NAME);
        qmeta->qid = conf->queue[i].qid;
        qmeta->msgsz = conf->queue[i].msgsz;
        qmeta->remote_mem_cnt = conf->remote_mem_cnt;
        qmeta->lpart.member_ip = conf->lmember_ip;

        ret = pthread_mutexattr_init(&attrs) ||
              pthread_mutexattr_settype(&attrs, PTHREAD_MUTEX_ERRORCHECK) ||
              pthread_mutexattr_setrobust(&attrs, PTHREAD_MUTEX_ROBUST) ||
              pthread_mutexattr_setpshared(&attrs, PTHREAD_PROCESS_SHARED) ||
              pthread_mutex_init(&qmeta->lpart.client_write_lock, &attrs);
        if (ret) {
            close(qfd);
            log_mq("controller:mutex(err %d)\n",ret);
            free(qmeta);
            return MUTEX_FAIL;
        }
        pthread_mutexattr_destroy(&attrs);

        qmeta->lpart.lpart_msg_seqno = UINT64_MAX;
        snprintf(qpart_file, MAX_PATH + MAX_NAME+ 10, "%s/%s.%u",
                 conf->storage_dir, conf->queue[i].qname, conf->lmember_ip);
        qmeta->lpart.qpart_fd  = open(qpart_file, flag, mode);
        if (-1 == qmeta->lpart.qpart_fd) {
            log_mq("controller:file(err %d, %d)\n", CTRL_QFILE_ERR, errno);
            close(qfd);
            free(qmeta);
            return CTRL_QFILE_ERR;
        }
        for (int j=0; j < conf->remote_mem_cnt; j++) {
            qmeta->rpart[j].member_ip = conf->rmember_ip[j];
            qmeta->rpart[j].rpart_msg_recv_seqno  = UINT64_MAX;
            qmeta->rpart[j].lpart_msg_sent_seqno  = UINT64_MAX;
            qmeta->rpart[j].lpart_msg_acked_seqno = UINT64_MAX;
            qmeta->rpart[j].lpart_msg_sent_epoch  = 0;
            qmeta->rpart[j].msg_rtt = MIN_RTT;
            qmeta->rpart[j].msg_sent_retry_cnt = 0;
            snprintf(qpart_file, MAX_PATH + MAX_NAME+ 10, "%s/%s.%u",
                 conf->storage_dir, conf->queue[i].qname, conf->rmember_ip[j]);
            qmeta->rpart[j].qpart_fd  = open(qpart_file, flag, mode);
            if (-1 == qmeta->rpart[j].qpart_fd) {
                log_mq("controller:file(err %d, %d)\n", CTRL_QFILE_ERR, errno);
                close(qfd);
                free(qmeta);
                return CTRL_QFILE_ERR;
            }
        }
        if (write_mq(qfd, (const void*)qmeta, qmeta_len, 0)) {
            log_mq("controller:file(err %d, %d)\n", WRITE_ERR, errno);
            close(qfd);
            free(qmeta);
            return WRITE_ERR;
        }
        close(qfd);
     }
     free(qmeta);
     return 0;
}

// Initialize controller with  configuration info. init_qmeta_files must have
// been called prior to this. Socket creation, mutex initialization etc. are
// done here. Controller is passed by the daemon to sender and receiver threads
// for queue operations.
int
init_controller(struct lucid_controller **ctlp, struct lucid_conf *conf) {
    int qfd = -1, i, j;
    int flag = O_RDWR|O_NONBLOCK;
    char qmeta_file[MAX_PATH + MAX_NAME + 1] = {0};
    char qpart_file[MAX_PATH + MAX_NAME +11] = {0};

    *ctlp = allocate_controller(conf);
    if (!ctlp) {
        log_mq("controller:init fail(err %d)\n", MEM_FAIL);
        return MEM_FAIL;
    }
    for (i=0; i<conf->queue_cnt; i++) {
        snprintf(qmeta_file, MAX_PATH + MAX_NAME, "%s/%s.meta",
                 conf->storage_dir, conf->queue[i].qname);
        qfd = open(qmeta_file, flag);
        if (qfd == -1) {
            log_mq("controller:init(err %d, %d)\n", CTRL_MFILE_ERR, errno);
            free_controller(*ctlp);
            return CTRL_MFILE_ERR;
        }
        (*ctlp)->qmeta[i] = (struct queue_metadata *)mmap(NULL, PAGESZ,
                                    PROT_READ|PROT_WRITE, MAP_SHARED, qfd, 0);
        if ((*ctlp)->qmeta[i] == MAP_FAILED) {
            log_mq("controller:init(err %d, %d)\n", CTRL_MMAP_ERR, errno);
            close(qfd);
            free_controller(*ctlp);
            return CTRL_MMAP_ERR;
        }
        // creation of partition files happens in init_qmeta_files if they dont
        // exist.Here we try to open them. If file has been deleted, init fails.
        snprintf(qpart_file, MAX_PATH + MAX_NAME+ 10, "%s/%s.%u",
                 conf->storage_dir, (*ctlp)->qmeta[i]->qname,
                 (*ctlp)->qmeta[i]->lpart.member_ip);
        (*ctlp)->qmeta[i]->lpart.qpart_fd  = open(qpart_file, flag);
        if (-1 == (*ctlp)->qmeta[i]->lpart.qpart_fd) {
            log_mq("controller:init(err %d, %d)\n", CTRL_QFILE_ERR, errno);
            close(qfd);
            free_controller(*ctlp);
            return CTRL_QFILE_ERR;
        }
        for (j=0; j<(*ctlp)->qmeta[i]->remote_mem_cnt; j++) {
            snprintf(qpart_file, MAX_PATH + MAX_NAME+ 10, "%s/%s.%u",
                     conf->storage_dir, (*ctlp)->qmeta[i]->qname,
                     (*ctlp)->qmeta[i]->rpart[j].member_ip);
            (*ctlp)->qmeta[i]->rpart[j].qpart_fd  = open(qpart_file, flag);
            if (-1 == (*ctlp)->qmeta[i]->rpart[j].qpart_fd) {
                log_mq("controller:init(err %d, %d)\n", CTRL_QFILE_ERR, errno);
                close(qfd);
                free_controller(*ctlp);
                return CTRL_QFILE_ERR;
            }
        }
    }
    //keep  queue metadata pointers within controller sorted by qid
    qsort((*ctlp)->qmeta, (*ctlp)->queue_cnt, sizeof(struct queue_metadata *),
          compare_qid);

    for (i=0; i < (*ctlp)->remote_mem_cnt; i++) {
        (*ctlp)->rmember[i].member_ip = conf->rmember_ip[i];
        (*ctlp)->rmember[i].state = DISCONNECTED;
        (*ctlp)->rmember[i].ping_seqno = 0;
    }
    (*ctlp)->socket = sock_create(conf->lmember_ip, RECVER_TIMEOUT_MS);
    if ((*ctlp)->socket < 0) {
        log_mq("controller:init(err %d, %d)\n", SOCK_CREAT_ERR, errno);
        close(qfd);
        free_controller(*ctlp);
        return SOCK_CREAT_ERR;
    }
    pthread_mutex_init(&(*ctlp)->sender_mutex, NULL);
    pthread_cond_init(&(*ctlp)->sender_cond, NULL);
    (*ctlp)->cleanup = 0;
    memset((*ctlp)->recv_buf, 0, MAX_MSGSZ);
    return 0;
}
