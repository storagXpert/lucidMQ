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

// global static variable for daemon is set when SIGHUP or SIGTERM is received
static int recvd_signal = 0;

// standard daemon creation operations
static int
daemonize(char * pid_file) {
	pid_t pid = 0;
	int fd = -1, pid_fd = -1;
	pid = fork();
	if (pid < 0)
		exit(EXIT_FAILURE);
	if (pid > 0)
		exit(EXIT_SUCCESS);
	if (setsid() < 0)
		exit(EXIT_FAILURE);
	signal(SIGCHLD, SIG_IGN);
	pid = fork();
	if (pid < 0)
		exit(EXIT_FAILURE);
	if (pid > 0)
		exit(EXIT_SUCCESS);
	umask(0);
	chdir("/");
	for (fd = sysconf(_SC_OPEN_MAX); fd > 0; fd--)
		close(fd);
	stdin = fopen("/dev/null", "r");
	stdout = fopen("/dev/null", "w+");
	stderr = fopen("/dev/null", "w+");
	if (pid_file) {
		char str[MAX_PATH+MAX_NAME];
		pid_fd = open(pid_file, O_RDWR|O_CREAT, 0640);
		if (pid_fd < 0)
			exit(EXIT_FAILURE);
		if (lockf(pid_fd, F_TLOCK, 0) < 0)
			exit(EXIT_FAILURE);
		sprintf(str, "%d\n", getpid());
		write(pid_fd, str, strlen(str));
	}
    return pid_fd;
}

// signal handler and setup for SIGHUP and SIGTERM
// SIGHUP reloads configuration - for dynmic reconfigure support(TODO)
// SIGTERM exits daemon after stopping threads and freeing all resources
static void
handle_signal(int sig) {
    recvd_signal = sig;
}

static void
setup_signal_handlers() {
    struct sigaction sa;
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = handle_signal;
    sa.sa_flags = SA_RESTART;
    sigaction(SIGHUP, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
}

void
print_help(void) {
	printf("\n Usage: lucidd [OPTIONS]\n\n");
	printf("  Options:\n");
	printf("   -h --help                 Print this help\n");
	printf("   -c --conf_file filename   Read configuration from the file\n");
	printf("   -l --log_file  filename   Write logs to the file\n");
	printf("   -p --pid_file  filename   Prevent multiple instatiation\n");
	printf("\n");
}

int
main (int argc, char *const argv[]) {
	static struct option long_options[] = {
		{"conf_file", required_argument, 0, 'c'},
		{"log_file", required_argument, 0, 'l'},
		{"pid_file", required_argument, 0, 'p'},
		{"help", no_argument, 0, 'h'},
		{NULL, 0, 0, 0}
	};
	int value, option_index = 0, pid_fd = -1;
	char *log_file = NULL, *conf_file = NULL, *pid_file = NULL;
	while ((value = getopt_long(argc, argv, "c:l:p:h", long_options, &option_index)) != -1) {
		switch (value) {
			case 'c':
				conf_file = strdup(optarg);
				break;
			case 'l':
				log_file = strdup(optarg);
				break;
		    case 'p':
				pid_file = strdup(optarg);
				break;
			case 'h':
				print_help();
				return EXIT_SUCCESS;
			case '?':
				print_help();
				return EXIT_FAILURE;
			default:
				break;
		}
	}
	pid_fd = daemonize(pid_file);
    logger.file = fopen(log_file, "a");
    if (!logger.file)
        return EXIT_FAILURE;
    pthread_mutex_init(&logger.lock, NULL);

	while (1) {
	    int ret = -1;
	    struct lucid_conf *conf = NULL;
        struct lucid_controller *ctl = NULL;
        pthread_t thread_id[2] = {0};
        pthread_attr_t attr = {0};

        log_mq("Init daemon pid :%d\n", getpid());
        conf = get_configuration(conf_file);
        if (!conf)
            break;
        ret = init_qmeta_files(conf);
        if (ret < 0)
            break;
        ret = init_controller(&ctl, conf);
        if (ret < 0)
            break;
        ret = pthread_attr_init (&attr) ||
              pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);
        if (ret)
            break;
        ret = pthread_create(&thread_id[0], &attr, sender_thread,(void*)ctl);
         if (ret)
            break;
        ret = pthread_create(&thread_id[1], &attr, receiver_thread,(void*)ctl);
         if (ret)
            break;
        pthread_attr_destroy(&attr);
        log_mq("Sender Receiver threads created(%lu, %lu)\n",
                thread_id[0], thread_id[1]);
        setup_signal_handlers();
        pause();                                // sleep until signalled
        if (recvd_signal == SIGHUP) {           //reload configuration
            ctl->cleanup = 1;
            log_mq("Waiting for threads exit\n");
            pthread_join(thread_id[0], NULL);
            pthread_join(thread_id[1], NULL);
            close(ctl->socket);
            free_controller(ctl);
            continue;
        } else if (recvd_signal == SIGTERM) {   //exit after cleanup
            ctl->cleanup = 1;
            log_mq("Waiting for threads exit\n");
            pthread_join(thread_id[0], NULL);
            pthread_join(thread_id[1], NULL);
            close(ctl->socket);
            free_controller(ctl);
            free(conf_file);
	        free(log_file);
	        free(pid_file);
	        lockf(pid_fd, F_ULOCK, 0);
	        close(pid_fd);
            break;
        } else
            log_mq("daemon:unknown signal(err %d, %d)\n", recvd_signal, errno);
	}
	log_mq("daemon: exit %d, %d\n", recvd_signal, errno);
	fclose(logger.file);
    return EXIT_SUCCESS;
}
