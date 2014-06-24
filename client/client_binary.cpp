#include <arpa/inet.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <ctime>
#include <errno.h>
#include <vector>
#include <protocol_binary.h>
#include <netdb.h>
#include <signal.h>

#define BUFLEN 8*1024*1024
#define SRV_IP "127.0.0.1"
#define MC_PORT 9887
#define NUM_KEYS 1
#define GET_CMD PROTOCOL_BINARY_CMD_GETK
#define MC_VALUE_SERIALIZED 0

#define TABLE_NAME "user_basic"
#define MC_VERSION 4
#define US_TIMEOUT 25000
#define US_MAXWAIT 700

#define DEBUG_RES(x) //x

using namespace std;

int total_ok=0;
int total_ko=0;
vector<int> received;

char* table = NULL;
int version = MC_VERSION;

char* host = NULL;
char* unix_socket = NULL;
int port = MC_PORT;
int us_timeout = US_TIMEOUT;
int us_maxwait = US_MAXWAIT;


bool check_results(int* ids, int num_keys) {
  bool result = true;
  DEBUG_RES(fprintf(stderr, "Total received: %zu\n", received.size()));
  for (size_t i=0; i<received.size(); i++) {
    bool found = false;
    for (int j=0; j<num_keys; j++) {
      if (received[i] == ids[j]) {
        found = true;
        break;
      }
    }
    if (!found) {
      fprintf(stderr, "Unexpected user_id %d\n", received[i]);
      result = false;
    }
  }
  return result;
}

int append_request(char* dest, const char* mc_key) {
  uint16_t keylen = strlen(mc_key);
  uint8_t extlen = 0;
  uint32_t bodylen = keylen + extlen;

  protocol_binary_request_header* header = (protocol_binary_request_header*) dest;
  header->request.magic = PROTOCOL_BINARY_REQ;
  header->request.opcode = GET_CMD;
  header->request.keylen = htons(keylen);
  header->request.extlen = extlen;
  header->request.datatype = 0;
  header->request.reserved = 0;  
  header->request.bodylen = htonl(bodylen);
  header->request.opaque = 1234567890;
  header->request.cas = 0;
  memcpy(dest+24, mc_key, keylen);

  return 24+keylen;
}

int process_response(char* buffer, int& total) {
  char mc_key[1024];
  char mc_value[8096];
  int responses = 0;
  char* current = buffer;
  int pending = total;
  char needle1[256];
  char needle2[256];
  char* pos1;
  char* pos2;

  sprintf(needle1, "%s:", table); // table_name:
  sprintf(needle2, "@%d:", version); // @version:

  while (pending>=24) {
    protocol_binary_response_header* header = (protocol_binary_response_header*) current;
    uint16_t keylen = ntohs(header->response.keylen);
    uint32_t bodylen = ntohl(header->response.bodylen);
    uint16_t status = ntohs(header->response.status);
    uint8_t extlen = header->response.extlen;
    DEBUG_RES(fprintf(stderr, "magic=[%x] opcode=[%x] keylen=[%d] bodylen=[%d]",
        header->response.magic, header->response.opcode, keylen, bodylen));
    if (pending>=24+(int)bodylen) {
      memcpy(mc_key, current+24+extlen, keylen);
      mc_key[keylen] = 0;
      int valuelen = bodylen-keylen-extlen;
      memcpy(mc_value, current+24+extlen+keylen, valuelen);
      mc_value[valuelen] = 0;
      pending -= 24+(int)bodylen;
      current += 24+(int)bodylen;
      responses++;

      if (keylen > 0) {
        // add user_id to received array
        if ((pos1=strstr(mc_key, needle1)) && (pos2 = strstr(pos1, needle2))) {
          int user_id = atoi(pos2+strlen(needle2));
          received.push_back(user_id);
          DEBUG_RES(fprintf(stderr, " user_id=[%d]", user_id));
        } 
      } else if (valuelen > 0) {
        char* pos;
        int offset;
        if (MC_VALUE_SERIALIZED) {
          pos = strstr(mc_value, "[1] => string(8) \"");
          offset = 18;
        } else {
          pos = mc_value;
          offset = 0;
        }
        int user_id = atoi(pos+offset);
        received.push_back(user_id);
        DEBUG_RES(fprintf(stderr, " user_id=[%d]", user_id));
      }
      if (valuelen == 0) {
        if (status != PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) { // not a Miss
          fprintf(stderr, "valuelen = 0 and not a miss\n");
        } else {
          DEBUG_RES(fprintf(stderr, " [miss]"));
        }
      }
      DEBUG_RES(fprintf(stderr, "\n"));
    } else {
      DEBUG_RES(fprintf(stderr, "... need more data\n"));
      break;
    }
  }
  for (int i=0; i<pending; i++) {
    buffer[i] = buffer[total-pending+i];
  }
  total=pending;
  return responses;
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
    return -1;
  }
  // set timeout
  struct timeval tv;
  tv.tv_sec  = us_timeout / 1000000;
  tv.tv_usec = us_timeout % 1000000;
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv,sizeof(tv)) < 0) {
      close(sock);
      return -1;
  }
  if (connect(sock, (sockaddr *) &sun, slen)==-1) {
    fprintf(stderr, "connect() failed\n");
    close(sock);
    return -1;
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
    return -1;
  }
  si_other.sin_addr = *((in_addr*)he->h_addr_list[0]);

  // create socket
  if ((sock=socket(AF_INET, SOCK_STREAM, 0))==-1) {
    fprintf(stderr, "socket() failed errno=%i\n", errno);
    return -1;
  }
  // set timeout
  struct timeval tv;
  tv.tv_sec  = us_timeout / 1000000;
  tv.tv_usec = us_timeout % 1000000;
  if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
    close(sock);
    return -1;
  }
  if (connect(sock, (sockaddr *) &si_other, slen)==-1) {
    fprintf(stderr, "connect() failed\n");
    close(sock);
    return -1;
  }
  return sock;
}

int init_socket() {
  int sock = -1;
  while (sock == -1) {
    if (unix_socket) 
      sock = init_unix_socket();
    else 
      sock = init_tcp_socket();
    if (sock == -1) sleep(1);
  }
  return sock;
}

void handle_error(int ret, int &sock) {
  if (ret == 0) {
    fprintf(stderr, "Connection closed\n");
  } else if (ret < 0) {
    if (errno==EAGAIN) {
      fprintf(stderr, "recv() timeout\n");
    } else {
      fprintf(stderr, "recv() failed, errno=%i\n", errno);
    }
  }
  close(sock);
  sock = init_socket();
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
  char mc_key[1024];
  int c, i=0;
  bool reuse_conn = true;

  signal(SIGPIPE, SIG_IGN);

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
      case 'f':
        unix_socket = strdup(optarg);
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
    int bytes=0;

    if (random) {
      num_keys = (rand()%max_keys)+1;
    }
    char* current = req_buf;
    received.clear();

    for (int j=0; j<num_keys; j++) {
      int user_id = 60000000 + (rand()%10000000);
      int partition= user_id%100;
      sprintf(mc_key, "%s:%d@%d:%d", table, partition, version, user_id);
      bytes = append_request(current, mc_key);
      ids[j] = user_id;
      current += bytes;
    }

    // Send request
    int pending = current - req_buf;
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
      handle_error(sent, sock);
      total_ko++;
      continue;
    }
  
    // get response
    int total = 0;
    int pending_users = num_keys;
    do {
       bytes = recv(sock, res_buf+total, BUFLEN-total, 0);
       if (bytes<=0)
         break;
       if (bytes>0)
         total += bytes;
       int responses = process_response(res_buf, total);
       pending_users -= responses;
    } while (pending_users>0);

    if (bytes<=0) {
      handle_error(bytes, sock);
      total_ko++;
      continue;

    } else {
      if (check_results(ids, num_keys)) {
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
  free(unix_socket);
  free(host);
  free(table);
  free(ids);

  return 0;
}  
