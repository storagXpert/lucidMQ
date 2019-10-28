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

// self explanatory utility functions
int
sock_send (int socket, uint32_t dst_ip, struct iovec *buf, int buf_cnt) {
    struct msghdr mhdr = {0};
    struct sockaddr_in sin = {0};
    int sendlen = -1;
    sin.sin_family = AF_INET;
    sin.sin_port = htobe16(LUCID_PORT);
    sin.sin_addr.s_addr = dst_ip;
    mhdr.msg_name = &sin;
    mhdr.msg_namelen = sizeof (struct sockaddr_in);
    mhdr.msg_iov = (struct iovec *) buf;
    mhdr.msg_iovlen =buf_cnt;
    do {
        sendlen = sendmsg (socket, &mhdr, 0);
    } while (sendlen == -1 && errno == EINTR);
    return sendlen;
}

int
sock_recv (int socket, uint32_t *src_ip, struct iovec *buf, int buf_cnt) {
    struct msghdr mhdr = {0};
    struct sockaddr_in sin = {0};
    int recvlen = -1;
    mhdr.msg_name = &sin;
    mhdr.msg_namelen = sizeof(struct sockaddr_in);
    mhdr.msg_iov = (struct iovec *) buf;
    mhdr.msg_iovlen = buf_cnt;
    do {
        recvlen = recvmsg (socket, &mhdr, MSG_WAITALL);
    } while (recvlen == -1 && errno == EINTR);
    *src_ip = (uint32_t) sin.sin_addr.s_addr;
    return recvlen;
}

int
sock_create (uint32_t src_ip, int timeout_ms ) {
    struct timeval timeout = {0};
    struct sockaddr_in sin = {0};
    int sfd = -1, ret = -1;
    sfd = socket(PF_INET, SOCK_DGRAM, 0);
    if (sfd == -1)
        return -1;
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = src_ip;
    sin.sin_port = htobe16(LUCID_PORT);
    ret = bind (sfd, (struct sockaddr *)&sin, sizeof(struct sockaddr_in));
    if (-1 == ret)
        return -1;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    ret = setsockopt (sfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,
                sizeof(struct timeval));
    if (-1 == ret)
        return -1;
    return sfd;
}

int
get_timestamp (uint64_t *stamp) {
    int status = -1;
    struct timeval time = {0};
    status = gettimeofday(&time, NULL);
    *stamp = time.tv_sec * 1000 + time.tv_usec / 1000;
    return status;
}


inline uint32_t
hash (const unsigned char* text) { //FNV1a hash
    const uint32_t prime = 0x01000193;
    uint32_t       val   = 0x811C9DC5;
    const unsigned char* ptr = text;
    while (*ptr)
      val = (*ptr++ ^ val) * prime;
    return val;
}

int
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

int read_mq (int fd, void *buf, size_t count, off_t offset) {
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

struct loginfo logger = {0};
void
log_mq( const char *fmt, ...) {
    if (!logger.file)
        return;
    struct timeval time = {0};
    gettimeofday(&time, NULL);
    va_list args;
    va_start(args, fmt);
    pthread_mutex_lock(&logger.lock);
    fprintf(logger.file,"[%lds.%ldms]", time.tv_sec, time.tv_usec/1000);
    vfprintf(logger.file, fmt, args);
    pthread_mutex_unlock(&logger.lock);
    va_end(args);
    fflush(logger.file);
}
