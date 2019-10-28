#include "lucidmq.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int main (int argc, char *argv[]) {
    lmq_hdl qhdl = NULL;
    int ret = -1, len = 0, seq = 0, cnt = 0, byte = 0;
    if (argc != 6) {
        printf("usage: mqwrite <msgcnt> <byte> <msglen> <qpath> <qname>\n");
        return 0;
    }
    cnt = atoi(argv[1]);
    byte = atoi(argv[2]);
    len = atoi(argv[3]);
    if (len <= 0 || len > MAX_MSGSZ) {
        printf("invalid msglen: check len from lucid.xml for queue\n");
        return 0;
    }
    char *buf = malloc(len);
    if (!buf) {
        printf("mem alloc fail\n");
        return 0;
    }
    qhdl = lmq_open (argv[4], argv[5]);
    if (!qhdl) {
        printf("lmq_open failed\n");
        return 0;
    }
    memset(buf, byte, len);
    while (seq < cnt) {
        ret = lmq_send (qhdl, buf, len);
        printf("msg no %d written\n", ++seq);
    };
    lmq_close(qhdl);
}
