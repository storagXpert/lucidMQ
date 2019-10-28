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

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>

#define MAX_NAME          32            // max len of queue name
#define MAX_PATH          255           // max len of dir path of queue storage
#define PAGESZ            sysconf(_SC_PAGESIZE)  // max queue meta size

// Structure definitions copied from lucid.h for reading on-disk queue metadata
// file. Make sure to update these if structure definitions in lucid.h changes
// else you would see weird stuff
struct local_partition {
    int                     qpart_fd;
    uint32_t                member_ip;
    pthread_mutex_t         client_write_lock;
    atomic_uint_fast64_t    lpart_msg_seqno;
};

struct remote_partition {
    int                     qpart_fd;
    uint32_t                member_ip;
    atomic_uint_fast64_t    rpart_msg_recv_seqno; // initialize to -1 use atomic_init()
    uint64_t                lpart_msg_sent_seqno;
    atomic_uint_fast64_t    lpart_msg_acked_seqno;
    atomic_uint_fast64_t    lpart_msg_sent_epoch;
    atomic_uint_fast16_t    msg_rtt;             // set default at 100ms
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

struct remote_member {
    uint32_t                member_ip;
    atomic_int_fast8_t      state;
    atomic_uint_fast32_t    ping_seqno;

};

// utility function copied from util.c for reading metadata file
static int
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

void
print_help(void)
{
	printf("\n Usage: lucidctl [OPTIONS]\n\n");
	printf("  Options:\n");
	printf("   -h --help                 Print this help\n");
	printf("   -n --name    qname        Name of queue to show\n");
	printf("   -p --path    qdir         Directory of queue metadata file\n");
	printf("\n");
}

void print_queue_meta (struct queue_metadata *qmeta) {
    struct in_addr in = {0};

    printf("qname: %s\nqid: %u\nmsgsz : %u\nremote_mem_cnt: %u", qmeta->qname,
           qmeta->qid, qmeta->msgsz, qmeta->remote_mem_cnt);
    printf("\nNote : 18446744073709551615 = UINT64_MAX");
    printf("\n=============== Local partition =============:");
    in.s_addr = qmeta->lpart.member_ip;
    printf("\ndaemon fd: %d\nip: %u (%s)\nlpart_msg_seqno: %lu",
           qmeta->lpart.qpart_fd, qmeta->lpart.member_ip, inet_ntoa(in),
           (uint64_t)qmeta->lpart.lpart_msg_seqno);

    for (int i=0; i<qmeta->remote_mem_cnt; i++) {
         printf("\n=============== Remote partition(%d) =========:", i);
         in.s_addr = qmeta->rpart[i].member_ip;
        printf("\ndaemon FD: %d\nip: %u (%s)\nrpart_msg_recv_seqno: %lu\n"
               "lpart_msg_sent_seqno: %lu\nlpart_msg_acked_seqno: %lu\n"
               "lpart_msg_sent_epoch: %lu ms\nmsg_rtt: %u ms\n"
               "msg_sent_retry_cnt: %u",
               qmeta->rpart[i].qpart_fd, qmeta->rpart[i].member_ip,
               inet_ntoa(in), (uint64_t)qmeta->rpart[i].rpart_msg_recv_seqno,
               qmeta->rpart[i].lpart_msg_sent_seqno,
               (uint64_t)qmeta->rpart[i].lpart_msg_acked_seqno,
               (uint64_t)qmeta->rpart[i].lpart_msg_sent_epoch,
               (uint16_t)qmeta->rpart[i].msg_rtt,
               qmeta->rpart[i].msg_sent_retry_cnt);
    }
    printf("\n");
}

int main (int argc, char *argv[]) {
    static struct option long_options[] = {
		{"name", required_argument, 0, 'n'},
		{"path", required_argument, 0, 'p'},
		{"help", no_argument, 0, 'h'},
		{NULL, 0, 0, 0}
	};
	char filename[MAX_PATH + MAX_NAME + 11] = {0};
	int value, option_index = 0;
	char *qname = NULL, *qpath = NULL;
	int qfd = -1, flag = O_RDONLY|O_NONBLOCK;
	struct queue_metadata *qmeta = NULL;
    while ((value = getopt_long(argc, argv, "n:p:h", long_options,
                                &option_index)) != -1) {
		switch (value) {
			case 'n':
				qname = strdup(optarg);
				break;
		    case 'p':
				qpath = strdup(optarg);
				break;
			case 'h':
				print_help();
				return EXIT_SUCCESS;
			case '?':
				print_help();
				return EXIT_FAILURE;
		}
	}
	if (!qname || !qpath) {
        print_help();
		return EXIT_FAILURE;
    }
    snprintf(filename, MAX_PATH + MAX_NAME + 10, "%s/%s.meta",qpath, qname);
    qfd = open(filename, flag);
    if (qfd == -1) {
        printf("open : file %s err(%d)\n", filename, errno);
        return EXIT_FAILURE;
    }
    qmeta = (struct queue_metadata *)mmap(NULL, PAGESZ, PROT_READ, MAP_SHARED,
                                          qfd, 0);
    if (qmeta == MAP_FAILED) {
        printf("open : mmap err(%d)\n", errno);
        close(qfd);
        return EXIT_FAILURE;
    }
    close(qfd);
    print_queue_meta(qmeta);
    munmap(qmeta, PAGESZ);
    return EXIT_SUCCESS;
}
