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

// used by qsort for sorting remote members by ip within lucid_conf structure
static int
compare_ip (const void *ip1, const void *ip2) {
    return (*(uint32_t *)ip1 - *(uint32_t *)ip2);
}

// Below, we have 4 static helper functions. These read indiviual xml elements
// of interest on behest of get_configuration function which in turn is  called
// by the daemon during initialization.
static int
get_storage_dir(xmlNodePtr root, struct lucid_conf *conf) {

    xmlNodePtr cur = root->xmlChildrenNode;
    while (cur) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *)"storage_dir"))) {
            conf->storage_dir = xmlNodeListGetString(cur->doc,
                                                     cur->xmlChildrenNode, 1);
            return 0;
        }
        cur = cur->next;
    }
    return CONF_DIR_NOTFND;
}

static int
get_cluster_local_member(xmlNodePtr root, struct lucid_conf *conf) {

    uint32_t member_cnt = 0, i = 0;
    unsigned char *ip_str = NULL;
    xmlNodePtr cur = root->xmlChildrenNode;

    while (cur) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *)"local_member"))) {
            ip_str = xmlNodeListGetString(cur->doc, cur->xmlChildrenNode, 1);
            if (1 != inet_pton(AF_INET,
                               (const char *)ip_str, &conf->lmember_ip)) {
                xmlFree(ip_str);
                return CONF_INVALID_IP;
            }
            i++;
            xmlFree(ip_str);
        }
        cur = cur->next;
    }
    return 0;
}

static int
get_cluster_remote_members(xmlNodePtr root, struct lucid_conf *conf) {

    uint32_t remote_mem_cnt = 0, i = 0;
    unsigned char *ip_str = NULL;
    xmlNodePtr cur = root->xmlChildrenNode;

    while (cur) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *)"remote_member")))
            remote_mem_cnt++;
        cur = cur->next;
    }
    if (!remote_mem_cnt)
        return CONF_RMEMB_NOTFND;
    conf->remote_mem_cnt = remote_mem_cnt;
    conf->rmember_ip = (uint32_t*)malloc(remote_mem_cnt * sizeof(uint32_t));
    if (!conf->rmember_ip)
        return MEM_FAIL;
    memset(conf->rmember_ip, 0, remote_mem_cnt * sizeof(uint32_t));

    cur = root->xmlChildrenNode;
    while (cur) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *)"remote_member"))) {
            ip_str = xmlNodeListGetString(cur->doc, cur->xmlChildrenNode, 1);
            if (1 != inet_pton(AF_INET, (const char *)ip_str,
                               &conf->rmember_ip[i])){
                xmlFree(ip_str);
                return CONF_INVALID_IP;
            }
            i++;
            xmlFree(ip_str);
        }
        cur = cur->next;
    }
     //sort conf by ip before controller is populated for binary search later
    qsort(conf->rmember_ip, remote_mem_cnt, sizeof(uint32_t), compare_ip);
    return 0;
}

static int
get_queues(xmlNodePtr root, struct lucid_conf *conf) {

    uint32_t queue_cnt = 0, i = 0, j = 0;
    xmlNodePtr cur = root->xmlChildrenNode;

    while (cur) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *)"queue")))
            queue_cnt++;
        cur = cur->next;
    }
    if (!queue_cnt)
        return CONF_QUEUE_NOTFND;
    conf->queue_cnt = queue_cnt;
    conf->queue = (struct queue_conf *)malloc(queue_cnt *
                                              sizeof(struct queue_conf));
    if (!conf->queue)
        return MEM_FAIL;
    memset(conf->queue, 0, queue_cnt * sizeof(struct queue_conf));

    cur = root->xmlChildrenNode;
    while (cur) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *)"queue"))) {
            conf->queue[i].qname = xmlGetProp(cur, "name");
            conf->queue[i].msgsz = atoi(xmlGetProp(cur, "msgsize"));
            conf->queue[i].qid   = hash(conf->queue[i].qname);
            for (j=0; j<i; j++) {
                if (conf->queue[i].qid == conf->queue[j].qid) {
                    free(conf->queue);
                    return CONF_HASH_CLN;
                }
            }
            i++;
        }
        cur = cur->next;
    }
    return 0;
}

// Read the xml configuration file, allocate and populate lucid_conf structure
// with it and return it to the daemon.
struct lucid_conf *
get_configuration(char *conf_file) {

    xmlDocPtr doc = NULL;
    xmlNodePtr root = NULL;
    struct lucid_conf *conf = NULL;
    int ret = -1;

    if (!conf_file)
        return NULL;
    doc = xmlReadFile(conf_file, NULL, 0);
    if (!doc) {
        log_mq("conf:cannot read xml file\n");
        return NULL;
    }
    root = xmlDocGetRootElement(doc);
    if (!root) {
     log_mq("conf:cannot get root elem\n");
        goto cleanup;
    }
    if (xmlStrcmp(root->name, (const xmlChar *) "lucid")) {
         log_mq("conf:wrong xml\n");
        goto cleanup;
    }
    conf = (struct lucid_conf *)malloc(sizeof(struct lucid_conf));
    if(!conf)
        goto cleanup;
    memset(conf, 0, sizeof(struct lucid_conf));

    ret = get_storage_dir(root, conf);
    if (ret) {
        free(conf);
        conf = NULL;
        log_mq("conf:storage_dir (err %d\n)", ret);
        goto cleanup;
    }
    ret = get_cluster_local_member(root, conf);
     if (ret) {
        free(conf);
        conf = NULL;
        log_mq("conf:cluster_local_member (err %d\n)", ret);
        goto cleanup;
    }
    ret = get_cluster_remote_members(root, conf);
     if (ret) {
        free(conf);
        conf = NULL;
        log_mq("conf:cluster_remote_members (err %d\n)", ret);
        goto cleanup;
    }
    ret = get_queues(root, conf);
    if (ret) {
        free(conf);
        log_mq("conf:queues (err %d\n)", ret);
        conf = NULL;
    }

   cleanup :
         xmlFreeDoc(doc);
         xmlCleanupParser();
   return conf;
}
