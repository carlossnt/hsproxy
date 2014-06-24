/**
 * Proxy daemon
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#include <signal.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/unistd.h>
#include <sys/select.h>

#include <libdaemon/dfork.h>
#include <libdaemon/dsignal.h>
#include <libdaemon/dlog.h>
#include <libdaemon/dpid.h>
#include <libdaemon/dexec.h>

#include <hsproxy.hpp>
#include <pwd.h>

#define HSPROXY_USER "hsproxy"

int signal_received = 0;

void signal_handler(int parameter) {
  daemon_log(LOG_WARNING, "Received signal %d\n", parameter);
  signal_received = 1;
}

int start_proxy(char* path, int mc_port, string sock_file,
      string db_name, bool syslog);
int start_daemon(char* path);


int main(int argc, char *argv[]) {

  /* Set indetification string for the daemon for both syslog and PID file */
  daemon_pid_file_ident = daemon_log_ident = daemon_ident_from_argv0(argv[0]);

  /* Check parameters */
  int c, ret;
  char* path = NULL;
  int mc_port = -1;
  string db_name = "";
  string sock_file = "";
  bool daemon = false;
  bool syslog = false;

  while ((c=getopt(argc, argv, "hksdc:p:f:n:")) != -1) {
    switch (c) {
      case 'k':
        if ((ret = daemon_pid_file_kill_wait(SIGTERM, 5)) < 0)
          daemon_log(LOG_WARNING, "Failed to kill daemon: %s", strerror(errno));
        if (ret==0) {
          daemon_log(LOG_WARNING, "Daemon stopped\n");
          daemon_pid_file_remove();
        }
        return ret < 0 ? 1 : 0;
      case 'c':
        path = optarg;
        break;
      case 'd':
        daemon = true;
        break;
      case 'p':
        mc_port = atoi(optarg);
        break;
      case 'f':
        sock_file = optarg;
        break;
      case 'n':
        db_name = optarg;
        break;
      case 's':
        syslog = true;
        break;
      case 'h':
        fprintf (stderr, "Usage: %s [options]\n", argv[0]);
        fprintf (stderr, "  -k: kill running daemon\n");
        fprintf (stderr, "  -d: start proxy as daemon\n");
        fprintf (stderr, "  -c: config path (default: /etc/hsproxy)\n");
        fprintf (stderr, "  -s: use syslog for error messages\n");
        fprintf (stderr, "  -p: override proxy mc_port\n");
        fprintf (stderr, "  -n: override DB name\n");
        fprintf (stderr, "  -f: override unix sock file\n");
        fprintf (stderr, "  -h: print this usage message\n\n");
        return 0;
      case '?':
        if (optopt == 'c' || optopt == 'f' || optopt == 'p' || optopt == 'n')
          daemon_log(LOG_ERR, "Option -%c requires an argument.\n", optopt);
        else 
          fprintf (stderr, "Unknown option `-%c'.\n", optopt);
        return 1;
      default:
        abort();
    }
  }

  if (daemon) start_daemon(path);
  else start_proxy(path, mc_port, sock_file, db_name, syslog);
}


int start_daemon(char* path) {
  pid_t pid;
  hsproxy *proxy = hsproxy::get_instance();

  /* Reset signal handlers */
  if (daemon_reset_sigs(-1) < 0) {
    daemon_log(LOG_ERR, "Failed to reset all signal handlers: %s", strerror(errno));
    return 1;
  }

  /* Unblock signals */
  if (daemon_unblock_sigs(-1) < 0) {
    daemon_log(LOG_ERR, "Failed to unblock all signals: %s", strerror(errno));
    return 1;
  }

  /* Check that the daemon is not running twice a the same time */
  if ((pid = daemon_pid_file_is_running()) >= 0) {
    daemon_log(LOG_ERR, "Daemon already running on PID file %u", pid);
    return 1;
  }

  /* Prepare for return value passing from the initialization procedure of the daemon process */
  if (daemon_retval_init() < 0) {
    daemon_log(LOG_ERR, "Failed to create pipe.");
    return 1;
  }

  /* Do the fork */
  if ((pid = daemon_fork()) < 0) {
    /* Exit on error */
    daemon_retval_done();
    return 1;

  } else if (pid) { /* The parent */
    int ret;
    /* Wait for 20 seconds for the return value passed from the daemon process */
    if ((ret = daemon_retval_wait(20)) < 0) {
      daemon_log(LOG_ERR, "Could not recieve return value from daemon process: %s", strerror(errno));
      return 255;
    } else if (ret>0) {
      daemon_log(LOG_ERR, "Failed to start daemon (%i)\n", ret);
    } else {
      daemon_log(LOG_INFO, "Daemon started\n");
    }
    return ret;

  } else { /* The daemon */
    struct passwd *pwd;
    int fd, quit = 0;
    fd_set fds;

    /* Close FDs */
    if (daemon_close_all(-1) < 0) {
      daemon_log(LOG_ERR, "Failed to close all file descriptors: %s", strerror(errno));
      daemon_retval_send(1);
      goto finish;
    }

    /* Create the PID file */
    if (daemon_pid_file_create() < 0) {
      daemon_log(LOG_ERR, "Could not create PID file (%s).", strerror(errno));
      daemon_retval_send(2);
      goto finish;
    }

    /* Set uid */
    pwd = getpwnam(HSPROXY_USER);
    if (pwd == NULL) {
      daemon_log(LOG_ERR, "User %s does not exist\n", HSPROXY_USER);
      daemon_retval_send(3);
      goto finish;
    }
    setuid(pwd->pw_uid);

    /* Initialize signal handling */
    if (daemon_signal_init(SIGINT, SIGTERM, SIGQUIT, 0) < 0) {
      daemon_log(LOG_ERR, "Could not register signal handlers (%s).", strerror(errno));
      daemon_retval_send(4);
      goto finish;
    }

    /* Send OK to parent process */
    proxy->init_proxy(path, -1, "", "");
    daemon_retval_send(0);
    daemon_log(LOG_INFO, "Successfully started");

    /* Prepare for select() on the signal fd */
    FD_ZERO(&fds);
    fd = daemon_signal_fd();
    FD_SET(fd, &fds);

    while (!quit) {
      fd_set fds2 = fds;

      /* Wait for an incoming signal */
      if (select(FD_SETSIZE, &fds2, 0, 0, 0) < 0) {
        /* If we've been interrupted by an incoming signal, continue */
        if (errno == EINTR)
          continue;
        daemon_log(LOG_ERR, "select(): %s", strerror(errno));
        break;
      }

      /* Check if a signal has been recieved */
      if (FD_ISSET(fd, &fds2)) {
        int sig;
        /* Get signal */
        if ((sig = daemon_signal_next()) <= 0) {
          daemon_log(LOG_ERR, "daemon_signal_next() failed: %s", strerror(errno));
          break;
        }
        /* Dispatch signal */
        switch (sig) {
          case SIGINT:
          case SIGQUIT:
          case SIGTERM:
            daemon_log(LOG_WARNING, "Got SIGINT, SIGQUIT or SIGTERM.");
            quit = 1;
            break;
        }
      }
    }

    /* Stop hsproxy */
    proxy->shutdown();

/* Do a cleanup */
finish:
    if (log_file) fclose(log_file);
    daemon_log(LOG_INFO, "Exiting...");
    daemon_retval_send(255);
    daemon_signal_done();
    daemon_pid_file_remove();
    return 0;
  }
}


int start_proxy(char* path, int mc_port, string sock_file, string db_name, bool syslog) {
  // set signal hanlders
  signal(SIGTERM, signal_handler);
  signal(SIGINT, signal_handler);
  signal(SIGQUIT, signal_handler);

  // Close stderr if syslog defined
  if (syslog) {
    daemon_log_use = DAEMON_LOG_SYSLOG;
    fclose(stderr);
    fclose(stdout);
  }

  // Init proxy threads
  hsproxy *proxy = hsproxy::get_instance();
  proxy->init_proxy(path, mc_port, db_name, sock_file);

  // do nothing (wait for signals)
  while (!signal_received) {
    proxy->check_config();
    sleep(1);
  }

  // Exiting
  daemon_log(LOG_WARNING, "Got SIGINT, SIGQUIT or SIGTERM.");
  proxy->shutdown();
  daemon_log(LOG_WARNING, "Proxy stopped\n");
  if (log_file) fclose(log_file);

  return 0;
}
