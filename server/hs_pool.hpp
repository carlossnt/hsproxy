/**
 * Connection pool to HS cluster
 * Number of connections is defined in configuration.
 * Foreach TCP/HS connection we have a worker thread (hs_thread). 
 * This hs_thread will:
 * - compose HS requests and send them to HS server
 * - parse HS responses, and send MC responses to clients
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#ifndef HS_POOL_H
#define HS_POOL_H

#include <netinet/in.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <queue>
#include <map>

#define MC_VALUE_SIZE 64*1024
#define HS_BUFFER_SIZE 64*1024
#define MC_BUFFER_SIZE 256*1024
#define HS_KEYS_REQ 50

#define SYNC_LOST 0
#define REQUEST_COMPLETED 1
#define REQUEST_IN_PROGRESS 2

using namespace std;

struct hs_thread;

/**
 * Queue to store hs_request pointers
 * Push/pop/size/empty are sychronized (mutex)
 */
struct hs_request_queue {
 private:
  queue<hs_request*> requests;
  pthread_mutex_t lock;
 public:
  ~hs_request_queue();
  hs_request_queue();
  bool empty();
  int size();
  void empty_queue();
  void push(hs_request *request, size_t max_size=0);
  hs_request* pop();
};


/**
 * We'll have 1 pool per cluster
 * Number of connections defined when creating
 */
struct hs_pool {
 private:
  vector<hs_thread*> threads;
  struct hs_request_queue queue;
  int req_sock[2];
  int cluster_id;
  int size;
  volatile bool ready;
 public:
  hs_pool(int cluster, int pool_size) :
    cluster_id(cluster), size(pool_size), ready(false) {}
  ~hs_pool();
  void init_pool();
  void push(hs_request *request);
  hs_request *pop();
  int get_cluster_id() {return cluster_id;}
  int get_req_socket() {return req_sock[1];}
  void stop_pool();
  long get_total_gets();
  long get_total_misses();
  bool is_ready() { return ready; }
  void set_ready() { if (!ready) ready = true; }
};

/**
 * Wroker thread. Handles a TCP connection with HS server.
 */
struct hs_thread {
 private:
  pthread_t thd_id;
  struct hs_request_queue sent_requests;
  int hs_sock;
  hs_pool* pool;
  hs_request* current_req;
  int current_get;
  int format;
  event_base* base;
  event* ev_new_req;
  event* ev_hs_res;
  event* ev_cancel;
  int sig_sock[2];
  long total_gets;
  long total_misses;
  // buffer HS in
  char *hs_buffer_in;
  char *current_hs_in;
  char *last_hs_in;
  // buffer HS out
  char *hs_buffer_out;
  char *current_hs_out;
  // buffer MC
  char *mc_buffer;
  char *current_mc;
 public:
  hs_thread(hs_pool *cluster_pool);
  ~hs_thread();
  pthread_t get_thread_id() {return thd_id;}
  void empty_queue();
  static void* thread_func(void *this_arg);
  void init_thread();
  void start_thread();
  void cancel_thread();
  void read_hs_responses();
  long get_total_gets() {return total_gets;}
  long get_total_misses() {return total_misses;}
  // event handlers
  static void new_reqs_handler(evutil_socket_t fd, short events, void *arg);
  static void hs_response_handler(evutil_socket_t fd, short events, void *arg);
  static void cancel_handler(evutil_socket_t fd, short events, void *arg);
 private:
  void run();
  bool unescape_string(char* dest, const char *start, const char *finish);
  void escape_string(char* dest, const char *start, const char *finish);
  int copy_unused_bytes(char *dest, char* start, char* finish);
  bool init_hs_connection();
  bool open_indexes(int cluster_id);
  void clear_thread();
  void reset_connection();
  void append_noop_response(uint32_t noop_opaque);
  void check_cancel_sock();
  // HS out
  int buffer_find_request(mckey_data* get);
  void send_hs_request(hs_request *req);
  int send_message(int sock, char* buffer, int total);
  // HS in
  void process_hs_response();
  int process_request_lines();
  bool check_pk_fields(const mckey_data* get, char* end);
  // MC out
  int buffer_mc_value(int mc_protocol, bool cas, const mckey_data* get, char* end);
  void add_mc_value(int mc_protocol, bool cas, const mckey_data* get, char* mc_value, int bytes);
  void flush_mc_buffer(int conn_id);
};

#endif // HS_POOL_H
