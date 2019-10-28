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

#ifndef __LUCIDMQ_H__
#define __LUCIDMQ_H__  1
#define _GNU_SOURCE 1
#ifdef __cplusplus
extern "C"
{
#endif

#define MAX_MSGSZ         1400
typedef struct lmq_msgq* lmq_hdl;

extern lmq_hdl lmq_open (const char *qpath, const char *qname);

extern void lmq_close (lmq_hdl q);

extern int lmq_send (lmq_hdl q, const void *msg, unsigned int msglen);

extern int lmq_recv (lmq_hdl q, void *msg, unsigned int msglen, int flag);

typedef enum {
    RECV_LOCAL   =  (1<<0),
    RECV_REMOTE  =  (1<<1)
} recv_flag;

#ifdef __cplusplus
}
#endif

#endif /* __LUCIDMQ_H__ */
