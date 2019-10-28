#include "lucidmq.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main (int argc, char *argv[]) {
    lmq_hdl qhdl = NULL;
    int ret = -1, len = 0, seq = 0;
    if (argc != 4) {
        printf("usage: mqread <msglen> <qpath> <qname>\n");
        return 0;
    }
    len = atoi(argv[1]);
    if (len <= 0 || len > MAX_MSGSZ) {
        printf("invalid msglen: check len from lucid.xml for queue\n");
        return 0;
    }
    char *buf = malloc(len);
    if (!buf) {
        printf("mem alloc fail\n");
        return 0;
    }
    qhdl = lmq_open (argv[2], argv[3]);
    if (!qhdl) {
        printf("lmq_open failed\n");
        return 0;
    }
   printf("======== LOCAL MSGS ==========\n");
   while (1) {
        ret = lmq_recv (qhdl, buf, len, RECV_LOCAL);
        if (ret == -1)
            break;
        for (int i=0; i<len; i++)
            printf("%d ", buf[i]);
        printf("\nend of msg seq %d\n", seq++);
   };
   seq = 0;
   printf("======= REMOTE MSGS ==========\n");
   while (1) {
        ret = lmq_recv (qhdl, buf, len, RECV_REMOTE);
        if (ret == -1)
            break;
        for (int i=0; i<len; i++)
            printf("%d ", buf[i]);
        printf("\nend of msg seq %d\n", seq++);
    };
  lmq_close(qhdl);
}
