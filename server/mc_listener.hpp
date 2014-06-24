/**
 * mc_listerner thread
 * - receives all MC/TCP requests from local processes
 * - build hs_request objects
 * - enqueue them into proper hs_pools
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#ifndef MC_LISTENER_H
#define MC_LISTENER_H

#include <arpa/inet.h>
#include <sys/stat.h>
#include <pthread.h>
#include <string.h>
#include <event2/event.h>
#include <hs_request.hpp>
#include <queue>
#include <map>

// Buffer used in recvfrom call
#define REQ_BUFFER_SIZE 128*1024
#define MAX_CONN_ID 65536
#define MAX_KEY_LENGTH 250
#define MAX_KEYS_PER_REQUEST 2000
#define STATS_BUFFER 5012

#define MC_COMMAND_GET 1
#define MC_COMMAND_GETS 2
#define MC_COMMAND_QUIT 3
#define MC_COMMAND_FLUSH 4
#define MC_COMMAND_STAT 5
#define MC_COMMAND_UNKNOWN 6

using namespace std;

struct mc_connection {
 public:
  int sock;
  pthread_mutex_t write_lock;
  sockaddr* sa;
  struct event *read_event;
  char* buffer;
  int used;
  int conn_id;
  int cluster_id;
  hs_request* current_req;
 public:
  mc_connection(int fd, sockaddr* client_addr);
  ~mc_connection();
  void set_read_event(struct event* er) {read_event = er;}
  int send_response(char* res_buffer, int bytes);
};

struct mc_conn_table {
 private:
  map<int, mc_connection*> connections;
  pthread_rwlock_t table_lock;
  int last_id;
  int open_conn;
  int opened_conn;
 public:
  mc_conn_table();
  ~mc_conn_table();
  mc_connection* get_connection(int conn_id);
  int add_connection(mc_connection *conn);
  int send_response(int conn_id, char* buffer, int bytes);
  void close_connection(int conn_id);
  void lock_table();
  void unlock_table();
  void empty_table();
  int get_open_connections() {return open_conn;}
  int get_opened_connections() {return opened_conn;}
};

struct mc_listener;
struct mc_parser;

struct mc_conn_queue {
 private:
  queue<mc_connection*> connections;
  pthread_mutex_t lock;
 public:
  mc_conn_queue();
  ~mc_conn_queue();
  void empty_queue();
  void push(mc_connection *conn);
  mc_connection* pop();
};

struct mc_pool {
 private:
  vector<mc_parser*> parsers;
  mc_listener* listener;
  int mc_port;
  string mc_sock_file;
  int use_unix_sock;
  int mc_sock;
  int size;
  int last;
 public:
  mc_pool(int p, int s) : listener(NULL), mc_port(p), 
      use_unix_sock(0), mc_sock(-1), size(s), last(0) {}
  mc_pool(string sf, int s) : listener(NULL), mc_port(-1),
      mc_sock_file(sf), use_unix_sock(1), 
      mc_sock(-1), size(s), last(0) {}
  ~mc_pool();
  int get_mc_sock() {return mc_sock;}
  int get_use_unix_sock() {return use_unix_sock;}
  int init_pool();
  void stop_pool();
  void init_mcsocket();
  void assign_connection(mc_connection *conn);
  int send_response(int conn_id, char* buffer, int bytes);
  int get_open_connections();
  int get_opened_connections();
 private:
  void init_unix_socket(const char* path);
  void init_inet_socket(int port);
};

struct mc_listener {
  pthread_t thd_id;
  event_base* base;
  event* ev_accept;
  event* ev_cancel;
  int sig_sock[2];
  mc_pool* pool;
 private:
  void run();
  // Event handlers
  static void accept_handler(evutil_socket_t listener, short events, void *arg);
  static void cancel_handler(evutil_socket_t fd, short events, void *arg);
  int get_sockaddr_size();
 public:
  mc_listener(mc_pool *p);
  ~mc_listener();
  static void* thread_func(void *this_arg);
  pthread_t get_thread_id() {return thd_id;}
  void cancel_thread();
  void start_thread();
};

struct mc_parser {
 private: 
  pthread_t thd_id;
  event_base* base;
  mc_conn_queue conn_queue;
  event* ev_cancel;
  event* ev_new_conn;
  int sig_sock[2];
  int req_sock[2];
  mc_pool* pool;
 private: 
  void run();
  // Event handlers
  static void read_handler(evutil_socket_t fd, short events, void *arg);
  static void cancel_handler(evutil_socket_t fd, short events, void *arg);
  static void new_conn_handler(evutil_socket_t fd, short events, void *arg);
  // Parsing routines
  void read_mcbuffer(int fd, int conn_id);
  bool is_ascii_request(mc_connection *conn);
  bool pending_ascii_requests(mc_connection *conn);
  int dispatch_ascii_request(mc_connection* mc_conn);
  int dispatch_binary_request(mc_connection* mc_conn);
  int dispatch_binary_quiet_request(mc_connection* mc_conn);
  mckey_data* parse_key_content(char *current, char *token_end);
  int add_request_keys(hs_request* hs_req, char* beginln, char* endln);
  int get_ascii_command(char* current);
  void send_command_response(int conn_id, int command, int protocol, uint32_t opaque);
  char* get_stats_response(int protocol, int &bytes, uint32_t opaque);
 public: 
  mc_parser(mc_pool *p);
  ~mc_parser();
  void cancel_thread();
  static void* thread_func(void *this_arg);
  pthread_t get_thread_id() {return thd_id;}
  int init();
  void start_thread();
  void push_connection(mc_connection *conn);
};

#endif // MC_LISTENER_H
