#include <arpa/inet.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <ctime>
#include <errno.h>
#include <vector>
#include <netdb.h>

#define BUFLEN 8*1024*1024
#define SRV_IP "127.0.0.1"
#define MC_PORT 9887
#define NUM_KEYS 1

#define TABLE_NAME "user_basic"
#define MC_VERSION 4
#define US_TIMEOUT 25000
#define US_MAXWAIT 700

#define DEBUG_RES(x) //x

using namespace std;

int total_ok=0;
int total_ko=0;

char* table = NULL;
int version = MC_VERSION;

char* host = NULL;
char* unix_socket = NULL;
int port = MC_PORT;
int us_timeout = US_TIMEOUT;
int us_maxwait = US_MAXWAIT;


bool check_results(int* ids, int num_keys, char* res) {
  char strnum[9];
  bool result = true;
  int total_found=0;
  vector<int> unexpected;
  char needle1[256], needle2[256];
  char *pos1, *pos2;
  char* current = res;

  sprintf(needle1, "%s:", table); // table_name:
  sprintf(needle2, "@%d:", version); // @version:

  while ((pos1 = strstr(current, needle1)) && ((pos2 = strstr(pos1, needle2)))) {
    current = pos2+strlen(needle2);
    strncpy(strnum, current, 8);
    strnum[8]=0;
    int user_id = atoi(strnum);
    DEBUG_RES(fprintf(stderr, "user_id=[%d]", user_id));
    bool found = false;
    for (int i=0; i<num_keys; i++) {
      if (user_id == ids[i]) {
        found = true;
        total_found++;
        break;
      }
    }
    if (!found) {
      unexpected.push_back(user_id);
      result = false;
    }
    if (strstr(current, strnum)==NULL) {
      result = false;
    }
  }
  if (result==false) {
    fprintf(stderr, "Error: ---------------------------\n");
    if (unexpected.size()>0) {
      fprintf(stderr, "Total unexpected: %zu\n", unexpected.size());
      for (size_t j=0; j<unexpected.size(); j++) {
        fprintf(stderr, "%d ", unexpected[j]);
      }
      fprintf(stderr, "\n");
    } else {
      fprintf(stderr, "Key insconsistent with values\n");
    }
  }
  DEBUG_RES(fprintf(stderr, "Total found: %d\n", total_found));
  return result;
}

int init_unix_socket() {
  int sock;
  struct sockaddr_un sun;
  socklen_t slen=sizeof(sun);
  sun.sun_family = AF_UNIX;
  sprintf(sun.sun_path, "%s", unix_socket);

  // create socket
  if ((sock=socket(AF_UNIX, SOCK_STREAM, 0))==-1) {
    fprintf(stderr, "socket() failed errno=%i\n", errno);
    exit(1);
  }
  // set timeout
  struct timeval tv;
  tv.tv_sec  = us_timeout / 1000000;
  tv.tv_usec = us_timeout % 1000000;
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv,sizeof(tv)) < 0) {
      fprintf(stderr, "setsockopt() failed errno=%i\n", errno);
  }
  if (connect(sock, (sockaddr *) &sun, slen)==-1) {
    fprintf(stderr, "connect() failed\n");
    exit(1);
  }
  return sock;
}

int init_tcp_socket() {
  int sock;
  struct sockaddr_in si_other;
  socklen_t slen=sizeof(si_other);
  memset((char *) &si_other, 0, sizeof(si_other));
  si_other.sin_family = AF_INET;
  si_other.sin_port = htons(port);

  hostent *he = gethostbyname(host);
  if (he == NULL) {
    fprintf(stderr, "Unknown host %s\n", host);
    exit(1);
  }
  si_other.sin_addr = *((in_addr*)he->h_addr_list[0]);

  // create socket
  if ((sock=socket(AF_INET, SOCK_STREAM, 0))==-1) {
    fprintf(stderr, "socket() failed errno=%i\n", errno);
    exit(1);
  }
  // set timeout
  struct timeval tv;
  tv.tv_sec  = us_timeout / 1000000;
  tv.tv_usec = us_timeout % 1000000;
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
      fprintf(stderr, "setsockopt() failed errno=%i\n", errno);
  }
  if (connect(sock, (sockaddr *) &si_other, slen)==-1) {
    fprintf(stderr, "connect() failed\n");
    exit(1);
  }
  return sock;
}

int init_socket() {
  if (unix_socket) {
    return init_unix_socket();
  } else {
    return init_tcp_socket();
  }
}

void handle_error(int ret, int &sock) {
  if (ret == 0) {
    fprintf(stderr, "Connection closed\n");
    exit(1);
  } else if (ret < 0) {
    if (errno==EAGAIN) {
      fprintf(stderr, "recv() timeout\n");
      // restart connection
      close(sock);
      sock = init_socket();
    } else {
      fprintf(stderr, "recv() failed, errno=%i\n", errno);
      exit(1);
    }
  }
}

void consume_buffer(int sock) {
  int bytes = 0;
  char buffer[1024];
  do {
    bytes = recv(sock, buffer, 1024, 0);
  } while (bytes>0);
}

int main(int argc, char *argv[]) {
  char* req_buf;
  char* res_buf;
  int max_keys = NUM_KEYS;
  bool random = false;
  int c, i=0;
  bool reuse_conn = true;

  req_buf = (char*) malloc(BUFLEN);
  res_buf = (char*) malloc(BUFLEN);
  srand(time(0));

  /* Check parameters */
  while ((c=getopt(argc, argv, "f:t:v:h:p:k:u:w:rc")) != -1) {
    switch (c) {
      case 'p':
        port = atoi(optarg);
        break;
      case 'h':
        host = strdup(optarg);
        break;
      case 'f':
        unix_socket = strdup(optarg);
        break;
      case 't':
        table = strdup(optarg);
        break;
      case 'v':
        version = atoi(optarg);
        break;
      case 'k':
        max_keys = atoi(optarg);
        break;
      case 'u':
        us_timeout = atoi(optarg);
        break;
      case 'w':
        us_maxwait = atoi(optarg);
        break;
      case 'r':
        random = true;
        break;
      case 'c':
        reuse_conn = false;
        break;
      case '?':
        if (optopt == 'h' || optopt == 'p' || optopt == 'k'
            || optopt == 't' || optopt == 'v' || optopt == 'f'
            || optopt == 'u' || optopt == 'w')
          fprintf(stderr, "Option -%c requires an argument.\n", optopt);
        else
          fprintf(stderr, "Unknown option `-%c'.\n", optopt);
        return 1;
      default:
        return 1;
    }
  }

  int* ids = (int*) malloc(sizeof(int) * max_keys);
  if (host==NULL) host = strdup(SRV_IP);
  if (table==NULL) table = strdup(TABLE_NAME);

  int sock = init_socket();

  while (true) {
    int num_keys = max_keys;
    if (random) {
      num_keys = (rand()%max_keys)+1;
    }
    sprintf(req_buf, "get");
    char *current = req_buf + strlen(req_buf);
    for (int j=0; j<num_keys; j++) {
      int user_id = 60000000 + (rand()%10000000);
      int partition = user_id%100;
      sprintf(current, " %s:%d@%d:%d", table, partition, 
          version, user_id);
      current += strlen(current);
      ids[j] = user_id;
    }
    sprintf(current, "\n");
    int bytes=0;

    // Send request
    int pending = strlen(req_buf);
    char* req_pos = req_buf;
    int sent = 0;
    do {
      sent = send(sock, req_pos, pending, 0);
      if (sent<=0)
        break;
      if (sent>0) {
        req_pos+=sent;
        pending-=sent;
      }
    } while(pending);

    if (sent<=0) {
      handle_error(bytes, sock);
      total_ko++;
      continue;
    }
  
    // get response
    int total = 0;
    do {
       bytes = recv(sock, res_buf+total, BUFLEN-total, 0);
       if (bytes<=0)
         break;
       if (bytes>0) 
         total += bytes;
    } while (0!=strncmp(res_buf+total-5, "END\r\n", 5));

    if (bytes<=0) {
      handle_error(bytes, sock);
      total_ko++;
      continue;

    } else {
      res_buf[total]=0;
      if (check_results(ids, num_keys, res_buf)) {
        total_ok++;
        if (!reuse_conn) {
          close(sock);
          sock = init_socket();
        }
      } else {
        total_ko++;
        consume_buffer(sock);
      }
      if (++i == 100) {
        fprintf(stderr, "Status: OK->%d KO->%d\n", total_ok, total_ko);
        i = 0;
      }
    }
   
    int usec = rand() % us_maxwait;
    usleep(usec);
  }

  // Release resources
  close(sock);
  free(res_buf);
  free(req_buf);
  free(host);
  free(unix_socket);
  free(table);
  free(ids);

  return 0;
}  
