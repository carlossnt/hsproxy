#include <arpa/inet.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <ctime>
#include <errno.h>
#include <netdb.h>

#define BUFLEN 8*1024*1024
#define SRV_IP "127.0.0.1"
#define NUM_SAMPLES 1000
#define MC_PORT 9887

#define TABLE_NAME "user_basic"
#define MC_VERSION 4

char* table = NULL;
char* host = NULL;
char* unix_socket = NULL;
int version = MC_VERSION;
int port = MC_PORT;

bool check_results(int* ids, int num_keys, char* res) {
  char strnum[9];
  bool result = true;
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
    bool found = false;
    for (int i=0; i<num_keys; i++) {
      if (user_id == ids[i]) {
        found = true;
        break;
      }
    }
    if (!found) {
      char mc_key[256];
      strncpy(mc_key, pos1, current-pos1);
      fprintf(stderr, "Unknown key %s\n", mc_key);
      fprintf(stderr, "Request User: %d\n\n", ids[0]);
      result = false;
    }
    if (strstr(current, strnum)==NULL) {
      fprintf(stderr, "Unknown user_id %d\n", user_id);
      fprintf(stderr, "%s\n\n", res);
      result = false;
    }
  }
  return result;
}

int comp(const void * elem1, const void * elem2) {
    int f = *((int*)elem1);
    int s = *((int*)elem2);
    if (f > s) return  1;
    if (f < s) return -1;
    return 0;
}

void print_stats(int* samples, int count) {
  int total=0;
  int pos50=(int)count/2;
  int pos90=(int)(count*90)/100;
  int pos99=(int)(count*99)/100;
  qsort(samples, count, sizeof(int), comp);
  for (int i=0; i<count; i++) {
    total+=samples[i];
    if (i==pos50) {
      fprintf(stderr, "50 latency < %d\n", samples[i]);
    }
    if (i==pos90) {
      fprintf(stderr, "90 latency < %d\n", samples[i]);
    }
    if (i==pos99) {
      fprintf(stderr, "99 latency < %d\n", samples[i]);
    }
  }
  fprintf(stderr, "Average: %d\n", (int) total/count);
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
  tv.tv_sec  = 0;
  tv.tv_usec = 150000;
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
  tv.tv_sec  = 0;
  tv.tv_usec = 150000;
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

int main(int argc, char *argv[]) {
  int sock, i, c;
  int num_keys = 1;
  char* req_buf = (char*) malloc(BUFLEN);
  char* res_buf = (char*) malloc(BUFLEN);
  timeval t1, t2;
  int elapsedTime, num=0;
  int samples[NUM_SAMPLES];
  bool reuse_conn = true;

  /* Check parameters */
  while ((c=getopt(argc, argv, "f:t:v:h:p:k:c")) != -1) {
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
        num_keys = atoi(optarg);
        break;
      case 'c':
        reuse_conn = false;
        break;
      case '?':
        if (optopt == 'h' || optopt == 'p' || optopt == 'k' ||
            optopt == 't' || optopt == 'v' || optopt == 'f')
          fprintf(stderr, "Option -%c requires an argument.\n", optopt);
        else
          fprintf(stderr, "Unknown option `-%c'.\n", optopt);
        return 1;
      default:
        return 1;
    }
  }

  if (table==NULL) table = strdup(TABLE_NAME);
  if (host==NULL) host = strdup(SRV_IP);

  sock = init_socket();
  srand(time(0));
  int* ids = (int*) malloc(sizeof(int) * num_keys);

  for (i=0; i<10000000; i++) {
    sprintf(req_buf, "get");
    char *current = req_buf + strlen(req_buf);
    for (int j=0; j<num_keys; j++) {
      int user_id = 60000000 + (rand()%10000000);
      int partition = user_id % 100;
      ids[j]=user_id;
      sprintf(current, " %s:%d@%d:%d", table,
          partition, version, user_id);
      current += strlen(current);
    }
    sprintf(current, "\n");

    // Send request
    gettimeofday(&t1, NULL);
    if (!reuse_conn) {
      close(sock);
      sock = init_socket();
    }

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

    if (sent==-1) {
      fprintf(stderr, "send() failed, errno=%i\n", errno);
      exit(1);
    }

    // get response
    int bytes = 0;
    int total = 0;
    do {
       bytes = recv(sock, res_buf+total, BUFLEN-total, 0);
       if (bytes<=0)
         break;
       if (bytes>0)
         total += bytes;
    } while (0!=strncmp(res_buf+total-5, "END\r\n", 5));

    if (bytes<0) {
      fprintf(stderr, "recv() failed errno=%d\n", errno);
    } else {
      gettimeofday(&t2, NULL);
      res_buf[total]=0;
      // compute and print the elapsed time in microsec
      elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000000; 
      elapsedTime += (t2.tv_usec - t1.tv_usec); 
      fprintf(stderr, "Elapsed time: %d\n", elapsedTime);
      samples[num++]=elapsedTime;
      //fprintf(stderr, "%s\n", res_buf);
      check_results(ids, num_keys, res_buf);
    }
    if (num>=NUM_SAMPLES) {
      print_stats(samples, num);
      break;
    }
    usleep(200);
  }

  // Release resources
  close(sock);
  free(req_buf);
  free(res_buf);
  free(host);
  free(unix_socket);
  free(table);
  free(ids);

  return 0;
}
