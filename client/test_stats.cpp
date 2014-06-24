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

#define US_TIMEOUT 25000
#define DEBUG_RES(x) //x

using namespace std;

vector<int> received;

char* host = NULL;
char* unix_socket = NULL;
int port = MC_PORT;
int us_timeout = US_TIMEOUT;

int stats_request(char* dest) {
  protocol_binary_request_header* header = (protocol_binary_request_header*) dest;
  header->request.magic = PROTOCOL_BINARY_REQ;
  header->request.opcode = PROTOCOL_BINARY_CMD_STAT;
  header->request.keylen = 0;
  header->request.extlen = 0;
  header->request.datatype = 0;
  header->request.reserved = 0;  
  header->request.bodylen = 0;
  header->request.opaque = 1234567890;
  header->request.cas = 0;
  return 24;
}

bool process_response(char* buffer, int& total) {
  char mc_key[1024];
  char mc_value[8096];
  bool end = false;
  int pending = total;
  char* current = buffer;

  while (pending>=24) {
    protocol_binary_response_header* header = (protocol_binary_response_header*) current;
    uint16_t keylen = ntohs(header->response.keylen);
    uint32_t bodylen = ntohl(header->response.bodylen);
    uint8_t extlen = header->response.extlen;
    DEBUG_RES(fprintf(stderr, "magic=[%x] opcode=[%x] keylen=[%d] bodylen=[%d]\n",
        header->response.magic, header->response.opcode, keylen, bodylen));
    if (pending>=24+(int)bodylen) {
      memcpy(mc_key, current+24+extlen, keylen);
      mc_key[keylen] = 0;
      int valuelen = bodylen-keylen-extlen;
      memcpy(mc_value, current+24+extlen+keylen, valuelen);
      mc_value[valuelen] = 0;
      pending -= 24+(int)bodylen;
      current += 24+(int)bodylen;

      if (keylen > 0) {
        fprintf(stderr, "%s = ", mc_key); 
      }
      if (valuelen > 0) {
        fprintf(stderr, "%s\n", mc_value);
      }
      if (keylen == 0 and valuelen ==0) {
        DEBUG_RES(fprintf(stderr, "END\n"));
        end = true;
      }
    }
  }
  return end;
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

int main(int argc, char *argv[]) {
  char* req_buf;
  char* res_buf;
  int c;

  signal(SIGPIPE, SIG_IGN);

  req_buf = (char*) malloc(BUFLEN);
  res_buf = (char*) malloc(BUFLEN);
  srand(time(0));

  /* Check parameters */
  while ((c=getopt(argc, argv, "f:h:p:u:")) != -1) {
    switch (c) {
      case 'p':
        port = atoi(optarg);
        break;
      case 'h':
        host = strdup(optarg);
        break;
      case 'u':
        us_timeout = atoi(optarg);
        break;
      case 'f':
        unix_socket = strdup(optarg);
        break;
      case '?':
        if (optopt == 'h' || optopt == 'p' || optopt == 'f' || optopt == 'u')
          fprintf(stderr, "Option -%c requires an argument.\n", optopt);
        else
          fprintf(stderr, "Unknown option `-%c'.\n", optopt);
        return 1;
      default:
        return 1;
    }
  }

  if (host==NULL) host = strdup(SRV_IP);
  int sock = init_socket();

  int bytes=0;
  char* current = req_buf;
  bytes = stats_request(current);
  current += bytes;

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
  }
  
  // get response
  int total = 0;
  bool end = false; 
  do {
     bytes = recv(sock, res_buf+total, BUFLEN-total, 0);
     if (bytes<=0)
       break;
     if (bytes>0)
       total += bytes;
     end = process_response(res_buf, total);
  } while (!end);

  // Release resources
  close(sock);
  free(res_buf);
  free(req_buf);
  free(unix_socket);
  free(host);

  return 0;
}  
