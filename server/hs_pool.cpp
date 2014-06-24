/**
 * Connection pool to each HS cluster
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

#include <hsproxy.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <protocol_binary.h>
#include <string.h>

#define CHAR_ESCAPE_PREFIX 0x01
#define CHAR_ESCAPE_SHIFT  0x40
#define CHAR_NOESCAPE_MIN  0x10

#define HS_CONN_RETRYINTERVAL 500000 // us
#define MAX_ERROR_SIZE 1024

#define MAX_HS_REQUESTS 512

using namespace std;

// HS requests queue -----------------

hs_request_queue::hs_request_queue() {
  pthread_mutex_init(&lock, NULL);
}

hs_request_queue::~hs_request_queue() {
  pthread_mutex_destroy(&lock);
}

void hs_request_queue::push(hs_request *request, size_t max_size) {
  pthread_mutex_lock(&lock);
  if (max_size>0 && requests.size()>max_size) {
    delete request;
  } else {
    requests.push(request);
  }
  pthread_mutex_unlock(&lock);
}

hs_request* hs_request_queue::pop() {
  hs_request* request = NULL;
  pthread_mutex_lock(&lock);
  if (!requests.empty()) {
    request = requests.front();
    requests.pop();
  }
  pthread_mutex_unlock(&lock);
  return request;
}

bool hs_request_queue::empty() {
  pthread_mutex_lock(&lock);
  bool result = requests.empty();
  pthread_mutex_unlock(&lock);
  return result;
}

int hs_request_queue::size() {
  pthread_mutex_lock(&lock);
  int result = requests.size();
  pthread_mutex_unlock(&lock);
  return result;
}

void hs_request_queue::empty_queue() {
  pthread_mutex_lock(&lock);
  while (!requests.empty()) {
    hs_request* request = requests.front();
    delete request;
    requests.pop();
  }
  pthread_mutex_unlock(&lock);
}

// HS pool ---------------------

hs_pool::~hs_pool() {
  queue.empty_queue();
  for (size_t i=0; i<threads.size(); i++) {
    if (threads[i]) delete threads[i];
  }
  threads.clear();
  close(req_sock[1]);
  close(req_sock[0]);
}

/**
 * Start pool. One thread per HS connection.
 * req_sock used to signal threads there are new requests in queue.
 */
void hs_pool::init_pool() {
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, req_sock) < 0) {
    HSLOG_ERROR("Error opening stream socket pair (req_sock)\n");
  }
  fcntl(req_sock[0], F_SETFL, O_NONBLOCK);
  fcntl(req_sock[1], F_SETFL, O_NONBLOCK);
  for (int i=0; i<size; i++) {
    hs_thread *thr = new hs_thread(this);
    thr->start_thread();
    threads.push_back(thr);
  }
}

/**
 * Cancel and wait for all threads.
 */
void hs_pool::stop_pool() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += 1;

  for (size_t i=0; i<threads.size(); i++) {
    pthread_t thr_id = threads[i]->get_thread_id();
    threads[i]->cancel_thread();
    while (pthread_timedjoin_np(thr_id, NULL, &ts)!=0) {
      HSLOG_INFO(20, "Timeout: Retry cancel hs_thread %lu\n", thr_id);
      threads[i]->cancel_thread();
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_sec += 1;
    }
  }
}

/**
 * Return total amount of keys requested to the pool
 */
long hs_pool::get_total_gets() {
  long total = 0;
  for (size_t i=0; i<threads.size(); i++) {
    total += threads[i]->get_total_gets();
  }
  return total;
}

/**
 * Return total amount of misses noticed in pool
 */
long hs_pool::get_total_misses() {
  long total = 0;
  for (size_t i=0; i<threads.size(); i++) {
    total += threads[i]->get_total_misses();
  }
  return total;
}

/**
 * Push request into queue and generate a 
 * new_request event to wake up pool threads.
 */
void hs_pool::push(hs_request *request) {
  char flag = 1;
  queue.push(request, MAX_HS_REQUESTS);
  send(req_sock[0], &flag, 1, 0);
}

hs_request* hs_pool::pop() {
  return queue.pop();
}


// HS thread ----------------------------

hs_thread::hs_thread(hs_pool* cluster_pool) {
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sig_sock) < 0) {
    HSLOG_ERROR("Error opening stream socket pair (sig_sock)\n");
  }
  fcntl(sig_sock[0], F_SETFL, O_NONBLOCK);
  fcntl(sig_sock[1], F_SETFL, O_NONBLOCK);

  hs_sock = -1;
  pool = cluster_pool;
  current_req = NULL;
  current_get = 0;
  hs_buffer_in = NULL;
  hs_buffer_out = NULL;
  mc_buffer = NULL;
  ev_hs_res = NULL;
  ev_new_req = NULL;
  ev_cancel = NULL;
  base = NULL;
  total_gets = 0;
  total_misses = 0;
}

hs_thread::~hs_thread() {
  clear_thread();
  close(sig_sock[1]);
  close(sig_sock[0]);
  if (ev_new_req) {
    event_del(ev_new_req);
    event_free(ev_new_req);
  }
  if (ev_hs_res) {
    event_del(ev_hs_res);
    event_free(ev_hs_res);
  }
  if (ev_cancel) {
    event_del(ev_cancel);
    event_free(ev_cancel);
  }
  if (base) event_base_free(base);
}

/**
 * Close HS connection, clear all data structures,
 * release resources (fd, memory, events)
 */
void hs_thread::clear_thread() {
  HSLOG_INFO(50, "Clear thread (closing connection)\n");
  // close connection
  if (hs_sock != -1) {
    if (ev_hs_res) {
      event_del(ev_hs_res);
      event_free(ev_hs_res);
      ev_hs_res = NULL;
    }
    close(hs_sock);
    hs_sock = -1;
  }
  // release buffers
  if (hs_buffer_in) {
    free(hs_buffer_in);
    hs_buffer_in = NULL;
  }
  if (hs_buffer_out) {
    free(hs_buffer_out);
    hs_buffer_out = NULL;
  }
  if (mc_buffer) {
    free(mc_buffer);
    mc_buffer = NULL;
  }
  // empty queue (free requests memory)
  sent_requests.empty_queue();
  if (current_req) {
    delete current_req;
    current_req = NULL;
  }
}

/**
 * Restart thread. Release resources and 
 * init new HS connection, events, buffers
 */
void hs_thread::reset_connection() {
  clear_thread();
  init_thread();
}

/**
 * Open TCP connection to cluster slave (HS)
 * Set connection timeout
 * Disable Nagle algorithm (tcp_nodelay)
 * Open HS indexes
 */
bool hs_thread::init_hs_connection() {
  config_manager *config = config_manager::get_instance();
  int cluster_id = pool->get_cluster_id();
  in_addr slave = config->pick_slave(cluster_id);
  if (slave.s_addr == 0) {
    HSLOG_ERROR("There is no slaves available in cluster=[%d]\n", cluster_id);
    HSLOG_ERROR("Cannot init HS connection\n");
    return false;
  }
  int hs_port = config->get_hs_port();
  int handle = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
  bool result = false;

  if (hs_sock != -1) {
    if (ev_hs_res) {
      event_del(ev_hs_res);
      event_free(ev_hs_res);
      ev_hs_res = NULL;
    }
    close(hs_sock);
    hs_sock = -1;
  }

  if (handle > 0) {
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr = slave;
    address.sin_port = htons((unsigned short) hs_port);

    if (connect(handle, (sockaddr *) &address, sizeof(address)) < 0) {
      HSLOG_ERROR("Error connecting to HS, ip=[%s], port=[%d], sockfd=[%d] errno=[%i]\n",
              inet_ntoa(slave), hs_port, handle, errno);
      close(handle);
    } else {
      HSLOG_INFO(50, "Init HS/TCP connection (ip=[%s], port=[%d], sockfd=[%d])\n",
            inet_ntoa(slave), hs_port, handle);
      hs_sock = handle;

      // Disable Nagle algorithm
      int flag = 1;
      if (setsockopt(hs_sock,  IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int)) < 0) {
        HSLOG_ERROR("setsockopt() TCP_NODELAY failed, errno=%i\n", errno);
      }
    }
  }
  if (hs_sock != -1) {
    if (!open_indexes(pool->get_cluster_id())) {
      HSLOG_ERROR("Error opening HS indexes\n");
    } else {
      fcntl(hs_sock, F_SETFL, O_NONBLOCK);
      ev_hs_res = event_new(base, hs_sock,
          EV_READ|EV_PERSIST, hs_response_handler, (void*) this);
      event_priority_set(ev_hs_res, 1);
      event_add(ev_hs_res, NULL);
      result = true;
    }
  }
  return result;
}

void* hs_thread::thread_func(void *this_arg) {
  hs_thread* thr = static_cast<hs_thread*>(this_arg);
  thr->init_thread();
  thr->run();
  return NULL;
}

/**
 * Allocate resources (buffers, event_base)
 * Init HS connection, register events
 * Configure thread using config values
 */
void hs_thread::init_thread() {
  hs_buffer_in = (char*) malloc(HS_BUFFER_SIZE);
  hs_buffer_out = (char*) malloc(HS_BUFFER_SIZE);
  mc_buffer = (char*) malloc(MC_BUFFER_SIZE);
  current_hs_in = hs_buffer_in;
  current_hs_out = hs_buffer_out;
  last_hs_in = hs_buffer_in;
  current_mc = mc_buffer;
  current_req = NULL;
  current_get = 0;
  *hs_buffer_in = 0;
  *hs_buffer_out = 0;
  *mc_buffer = 0;
  total_gets = 0;
  total_misses = 0;

  if (base==NULL) {
    base = event_base_new();
    event_base_priority_init(base, 3);
  }

  while (!init_hs_connection()) {
    HSLOG_INFO(20, "Error starting HS thread, retrying\n");
    usleep(HS_CONN_RETRYINTERVAL);
    check_cancel_sock();
  }

  pool->set_ready();
  HSLOG_INFO(20, "%lu: HS Thread started...\n", pthread_self());
}

void hs_thread::check_cancel_sock() {
  char buffer[1];
  int bytes = recv(sig_sock[1], buffer, 1, 0);
  if (bytes==1) {
      clear_thread();
      HSLOG_INFO(20, "%lu: HS Thread exit (check_cancel_sock)\n", pthread_self());
      pthread_exit(NULL);
  }
}

void hs_thread::start_thread() {
  int rc = pthread_create(&thd_id, NULL, thread_func, static_cast<void*>(this));
  if (rc != 0) {  
    HSLOG_ERROR("Error in thread creation (errno = %i)\n", errno);
    return;
  } 
}

/**
 * Trigger a cancel_thread event. The results is the thread exits.
 */
void hs_thread::cancel_thread() {
  char flag = 1;
  send(sig_sock[0], &flag, 1, 0);
}

/**
 * Send the message placed in buffer to HS server
 * @param int sock HS socket
 * @param char* buffer pointer to message to be sent
 * @param int total message length
 * @return int bytes sent, -1 if error
 */
int hs_thread::send_message(int sock, char* buffer, int total) {
  int offset=0;
  while (offset<total) {
    int bytes = send(sock, buffer+offset, total-offset, 0);
    if (bytes<0 && errno!=EAGAIN) {
      return -1; // send error
    }
    if (bytes>0) 
      offset += bytes;
  }
  return offset;
}

/**
 * Unescape string delimited by start and finish. 
 * Restore original values as they are in database.
 * Column values cannot contain chars like '\t' and '\n' since they are used
 * by HS protocol to separate columns and rows. All chars less than 0x10 are escaped.
 * @param char* dest pointer where we'll write unescaped column value
 * @param char* start begin of escaped column value to be parsed
 * @param char* finish end of escaped column value to be parsed
 * @return true if OK, false if input string is inconsistent
 */
bool hs_thread::unescape_string(char* dest, const char *start, const char *finish) {
  while (start != finish) {
    const unsigned char c = *start;
    if (c != CHAR_ESCAPE_PREFIX) {
      *dest = c;
    } else if (start + 1 != finish) {
      ++start;
      const unsigned char cn = *start;
      if (cn < CHAR_ESCAPE_SHIFT) {
        return false;
      }
      *dest = cn - CHAR_ESCAPE_SHIFT;
    } else {
      return false;
    }
    ++start;
    ++dest;
  }
  *dest=0;
  return true;
}

/**
 * Escape special chars in field value.
 * Field value cannot contain chars like '\t' and '\n' since they are used
 * by HS protocol to separate field values and requests.
 * HS protocol escapes all char values less than 0x10 by 2 bytes:
 * 0x01 and (char+0x40)
 * @param char* dest pointer where we'll write escaped string
 * @param char* start field value begin
 * @param char* finish field value end
 */

void hs_thread::escape_string(char* dest, const char *start, const char *finish) {
  while (start != finish) {
    const unsigned char c = *start;
    if (c >= CHAR_NOESCAPE_MIN) {
      dest[0] = c; /* no need to escape */
    } else {
      dest[0] = CHAR_ESCAPE_PREFIX;
      ++dest;
      dest[0] = c + CHAR_ESCAPE_SHIFT;
    }
    ++start;
    ++dest;
  }
  *dest=0;
}

/**
 * Copy bytes between start and finish into dest
 * @param char* dest pointer where we'll write unused bytes
 * @param char* start unused bytes begin
 * @param char* finish unused bytes end
 */
int hs_thread::copy_unused_bytes(char *dest, char* start, char* finish) {
  int i=0;
  while (start[i]!=0 && start+i<finish) {
    dest[i] = start[i];
    i++;
  }
  dest[i]=0;
  return i;
}

/**
 * hs_thread main loop
 * Before starting loop we register new_request and cancel events.
 */
void hs_thread::run() {
  ev_new_req = event_new(base, pool->get_req_socket(), 
      EV_READ|EV_PERSIST, new_reqs_handler, (void*) this);
  ev_cancel = event_new(base, sig_sock[1],
      EV_READ|EV_PERSIST, cancel_handler, (void*) this);
  event_priority_set(ev_new_req, 2);
  event_priority_set(ev_cancel, 0); // max priority
  event_add(ev_new_req, NULL);
  event_add(ev_cancel, NULL);
  event_base_dispatch(base);
}

/**
 * Handler called when arriving new requests to pool request_queue.
 * In other words, called when any thread write into req_sock[0]
 * by using hs_pool::push(...) method.
 * Pop requests from queue, prepare HS request messages and send them to HS server.
 * @param int fd file descriptor associated to pool request_queue
 * @param short events
 * @param void *arg pointer to running hs_thread
 */
void hs_thread::new_reqs_handler(evutil_socket_t fd, short events, void *arg) {
  hs_thread* thr = static_cast<hs_thread*>(arg);
  char buffer[1];
  int bytes;
  while ((bytes=recv(fd, buffer, 1, 0))>0) {
    hs_request* req = NULL;
    while ((req = thr->pool->pop())) {
      PROFILE("mc get: %s\n", req->gets[0]->mc_key.c_str());
      DEBUG(100, req->print_request());
      thr->send_hs_request(req);
    }
  }
}

/**
 * Handler called when HS response arrives.
 * Bytes received through hs connection will be processed.
 * @param int fd HS connection socket
 * @param short events
 * @param void *arg pointer to running hs_thread
 */
void hs_thread::hs_response_handler(evutil_socket_t fd, short events, void *arg){
  hs_thread* thr = static_cast<hs_thread*>(arg);
  thr->read_hs_responses();
}

/**
 * Handler called when cancel event is triggered (someone calls cancel_thread)
 * Clean thread and exit.
 * @param int fd file descriptor associated to cancel event
 * @param short events
 * @param void *arg pointer to running hs_thread
 */
void hs_thread::cancel_handler(evutil_socket_t fd, short events, void *arg){
  hs_thread* thr = static_cast<hs_thread*>(arg);
  thr->clear_thread();
  HSLOG_INFO(20, "%lu: HS Thread exit (cancel_handler)\n", pthread_self());
  pthread_exit(NULL);
}

/**
 * Get all received bytes from HS server and process data
 */
void hs_thread::read_hs_responses() {
  int bytes;
  while (1) {
    int used = current_hs_in-hs_buffer_in;
    bytes = recv(hs_sock, current_hs_in, HS_BUFFER_SIZE-used-1, 0);
    if (bytes>0) {
      last_hs_in = current_hs_in + bytes;
      *last_hs_in = 0;
      PROFILE("<< TCP/HS response received <<----\n");
      HSLOG_INFO(90,  "HS response received (%d bytes)\n", bytes);
      HSLOG_INFO(100, "HS response received: %s\n", current_hs_in);
      process_hs_response();
    } else {
      break;
    }
  } 
  // handle errors/close
  if (bytes==0) {
    HSLOG_INFO(20, "HS Connection closed... restarting\n");
    reset_connection();
  }
  if (bytes<0 && errno!=EAGAIN) {
    HSLOG_INFO(20, "HS Connection error... restarting\n");
    reset_connection();
  }
}
 
/**
 * Compose and send HS request using hs_request data
 * Foreach key in req we append a find command to hs_buffer_out
 * We push hs_request pointer to sent_requests queue before sending 
 * TCP request to HS. 
 * Receiver callback will use req data to match results received and keys sent.
 * @param hs_request* req request data
 */
void hs_thread::send_hs_request(hs_request *req) {
  string mc_response;
  size_t i=0;
  size_t total_req = req->gets.size();
  current_hs_out = hs_buffer_out;
  sent_requests.push(req);
  
  // iterate request gets
  while (i<total_req) {
    buffer_find_request(req->gets[i++]);
    // flush HS requests every 50 requests
    if (i%HS_KEYS_REQ==0 || i==total_req) {
      int bytes = current_hs_out - hs_buffer_out;
      HSLOG_INFO(90,  "Send HS request (%d bytes)\n", bytes);
      HSLOG_INFO(100, "Send HS request %s\n", hs_buffer_out);
      if (send_message(hs_sock, hs_buffer_out, bytes) < 0) {
        HSLOG_ERROR("Error sending HS request (errno=%i)\n", errno);
      }
      current_hs_out = hs_buffer_out;
    }
  }
  PROFILE(">> Sent TCP/HS request >>\n");
}

/**
 * Append find request to HS message (stored in hs_buffer_out)
 * @param mckey_data* get structure with data we got 
 *   after parsing mckey (contains pk values, index_id...)
 * @return int bytes appended
 */
int hs_thread::buffer_find_request(mckey_data* get) {
  char field_value[MC_VALUE_SIZE];
  int bytes = 0;
  int length = 0;
  // <indexid> <op> <vlen> <v1> ... <vn>
  int num_key_fields = get->pk_values.size();
  const char* start = get->pk_values[0].c_str();
  const char* finish = start + get->pk_values[0].size();
  escape_string(field_value, start, finish);
  sprintf(current_hs_out, "%d\t=\t%d\t%s", get->index_id,
      num_key_fields, field_value);
  length = strlen(current_hs_out);
  current_hs_out += length;
  bytes += length;
  for (int i=1; i<num_key_fields; i++) {
    const char* start = get->pk_values[i].c_str();
    const char* finish = start + get->pk_values[i].size();
    escape_string(field_value, start, finish);
    sprintf(current_hs_out, "\t%s", field_value);
    length = strlen(current_hs_out);
    current_hs_out += length;
    bytes += length;
  }
  sprintf(current_hs_out, "\n");
  current_hs_out += 1;
  bytes += 1;
  total_gets++; //stats
  return bytes;
}

/**
 * Send all openIndex messages to HS server
 * @param int cluster_id cluster we are connected to
 * @return true if all indexes were opened
 */
bool hs_thread::open_indexes(int cluster_id) {
  config_manager* config = config_manager::get_instance();
  string db_name = config->get_db_name();
  vector<index_data> indexes = config->get_cluster_indexes(cluster_id);
  size_t open_indexes = 0;
  char numstr[11];

  for (size_t i=0; i<indexes.size(); i++) {
    HSLOG_INFO(60, "Open index %d\n", indexes[i].index_id);
    sprintf(numstr, "%d", indexes[i].index_id);
    // generate column list
    const storage_fields* meta = config->get_target_metadata(indexes[i].target_name, indexes[i].version);
    int num_key_fields = meta->key_fields.size();
    string column_list = meta->key_fields[0];
    for (int j=1; j<num_key_fields; j++) {
      column_list = column_list + "," + meta->key_fields[j];
    }
    int num_values = meta->value_fields.size();
    for (int j=0; j<num_values; j++) {
      column_list = column_list + "," + meta->value_fields[j];
    }
    // P <indexid> <dbname> <tablename> <indexname> <columns> [<fcolumns>]
    string open_request = string("P\t") + numstr + "\t" + db_name + "\t" + indexes[i].table_name
                           + "\t" + meta->index + "\t" + column_list + "\n";
    HSLOG_INFO(80, "%s\n", open_request.c_str());

    // Send request
    if (send(hs_sock, open_request.c_str(), open_request.size(), 0) >= 0) {
      // parse response (it should be 0 1)
      int total = 0;
      int bytes = 0;
      do {
        bytes = recv(hs_sock, hs_buffer_in+total, HS_BUFFER_SIZE-total-1, 0);
        total+= bytes;
        if (bytes==0) {
          HSLOG_ERROR("HS connection closed\n");
          return false;
        } else if (bytes<0 && errno!=EAGAIN) {
          HSLOG_ERROR("HS connection recv error (%d)\n", errno);
          return false;
        }
      } while (bytes<4);

      if (total>=4) { // 0 \t 1 \n
        hs_buffer_in[total] = 0;
        if (hs_buffer_in[0] == '0' && hs_buffer_in[2] == '1') {
          open_indexes++;
        } else {
          HSLOG_ERROR("open_indexes error: table=[%s] received => %s\n", 
            indexes[i].table_name.c_str(), hs_buffer_in);
          return false;
        }
      }
    }
  }
  return (open_indexes == indexes.size());
}

/**
 * Parse all data received (hs_buffer_in)
 * Loop
 * - process hs response from current_hs_in position
 * - if current_req was completed send response to MC/TCP client
 * - if we detected a sync_lost event, reset_connection
 * - if there's no more data, return and wait for more HS packets
 */
void hs_thread::process_hs_response() {
  current_hs_in = hs_buffer_in;
  int ret = REQUEST_IN_PROGRESS;

  while (current_hs_in<last_hs_in) {
    if (current_req == NULL) {
      current_req = sent_requests.pop();
      current_get = 0;
      current_mc = mc_buffer;
      mc_buffer[0] = 0;
    }

    // Process HS buffer (append MC responses)
    ret = process_request_lines();

    switch (ret) {
      case REQUEST_COMPLETED: // (send MC/TCP response)
        PROFILE("mc response: %s\n", current_req->gets[0]->mc_key.c_str());
        if (current_req->mc_protocol == MC_PROTOCOL_ASCII) {
          HSLOG_INFO(100, "MC response: %s\n", mc_buffer);
        }
        flush_mc_buffer(current_req->mc_conn_id);
        delete current_req;
        current_req = NULL;
        break;
      case SYNC_LOST: // (restart connection, clear buffers)
        HSLOG_INFO(20, "Sync lost: Reset connection (%d)\n", pool->get_cluster_id());
        reset_connection();
        break;
      case REQUEST_IN_PROGRESS: // (wait for new hs packets)
        HSLOG_INFO(90, "Waiting more packets from HS\n");
        // Move pending bytes to buffer begin
        int unused = copy_unused_bytes(hs_buffer_in, current_hs_in, last_hs_in);
        current_hs_in = hs_buffer_in + unused;
        last_hs_in = current_hs_in;
        // send partial results
        flush_mc_buffer(current_req->mc_conn_id);
        break;
    }
  } 
  // If we used all the buffer, empty it
  if (ret != REQUEST_IN_PROGRESS) {
    current_hs_in = hs_buffer_in;
    last_hs_in = hs_buffer_in;
  }
}

/**
 * Send MC responses to client connected through conn_id
 * @param int conn_id connection id
 */
void hs_thread::flush_mc_buffer(int conn_id) {
  int bytes = current_mc - mc_buffer;
  if (bytes>0) {
    hsproxy::get_instance()->send_mc_response(conn_id, mc_buffer, bytes);
  }
  current_mc = mc_buffer;
  mc_buffer[0] = 0;
}

/**
 * Parse data received from current_hs_in position (hs_buffer_in)
 * Foreach row in received data
 * - append mc_key response to mc_buffer
 * We'll return any time we detect an event like this
 * - all hs_buffer_in processed (we need more data)
 * - current mc request completed (we have to send it to client and empty mc_buffer)
 * - sync error (we must reset connection)
 * @return int any of these: REQUEST_COMPLETED, REQUEST_IN_PROGRESS, SYNC_LOST
 */
int hs_thread::process_request_lines() {
  if (current_req==NULL) {
    HSLOG_ERROR("%lu: Received response, but requests list is empty\n", pthread_self());
    return SYNC_LOST;
  }
  int total_keys = (int) current_req->gets.size();
  while (last_hs_in>current_hs_in && current_get<total_keys) {
    int pending = last_hs_in - current_hs_in;
    char *row_end = (char*) memchr(current_hs_in, '\n', pending);
    if (row_end != NULL) {
      if (buffer_mc_value(current_req->mc_protocol, current_req->cas,
           current_req->gets[current_get], row_end) < 0) {
        return SYNC_LOST;
      }
      // If we are using MC_BUFFER_SIZE/2 => flush
      if (current_mc - mc_buffer > MC_BUFFER_SIZE/2) {
        flush_mc_buffer(current_req->mc_conn_id);
      }
      // Added MC value --------
      current_hs_in = row_end + 1;
      current_get++;
    } else {
       return REQUEST_IN_PROGRESS;
    }
  }

  if (current_get == total_keys) {
    if (current_req->mc_protocol == MC_PROTOCOL_ASCII) {
      sprintf(current_mc, "END\r\n");
      current_mc+=strlen(current_mc);
    } else {
      if (current_req->noop) {
        append_noop_response(current_req->noop_opaque);
      }
    }
    return REQUEST_COMPLETED;
  }
  return REQUEST_IN_PROGRESS;
}

/**
 * Append NOOP reponse to MC buffer.
 * Header (24 bytes), opcode (0x0a), status (ok), opaque
 */
void hs_thread::append_noop_response(uint32_t noop_opaque) {
  int header_size = sizeof(protocol_binary_response_header);
  memset(current_mc, 0, header_size);

  protocol_binary_response_header* header = (protocol_binary_response_header*) current_mc;
  header->response.magic = PROTOCOL_BINARY_RES;
  header->response.opcode = PROTOCOL_BINARY_CMD_NOOP;
  header->response.status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
  header->response.opaque = noop_opaque;

  DEBUG(100, print_mc_binary_response(log_file, current_mc));
  current_mc += header_size;
}

/**
 * Check we've received the same PK values a we expected.
 * Used to detect sync errors.
 * We just compare PK values contained in get param 
 *  with PK values received in HS response.
 * @param mckey_data* get contains mckey we asked for, pk_values list...
 * @param char* end pointer to hs row end
 * HS response for current mckey is contained between current_hs_in and end pointers.
 *  e.g. '0      2       value1  value2'
 * PK values are always the first values in HS response
 * After executing this method current_hs_in points to the next value after PK values
 * @return bool true if OK, false if sync lost or broken response
 */
bool hs_thread::check_pk_fields(const mckey_data* get, char* end) {
  size_t num_key_values = get->pk_values.size();
  char field_value[MC_VALUE_SIZE];
  const char* expected;
  int value_length;

  for (size_t i=0; i<num_key_values; i++) {
    int pending = end - current_hs_in;
    char* value_end = (char*) memchr(current_hs_in, '\t', pending);
    if (value_end == NULL) {
      HSLOG_ERROR("Broken response\n");
      return false; // Broken response
    }
    unescape_string(field_value, current_hs_in, value_end);
    value_length = strlen(field_value);
    expected = get->pk_values[i].c_str();
    if (strncmp(expected, field_value, value_length) != 0) {
      HSLOG_INFO(20, "Sync lost (cluster %i): %s got (%s expected)\n",
              pool->get_cluster_id(), field_value, expected);
      return false; // Sync lost
    }
    current_hs_in = value_end + 1;
  }
  return true;
}

/**
 * Parse HS response to get row values, prepare MC key response 
 * to be sent and append it to mc_buffer.
 * @oaram int mc_protocol MC_PROTOCOL_ASCII or MC_PROTOCOL_BINARY
 * @param bool cas  include cas field in ascii response if true
 * @param mckey_data* get contains mckey we asked for, pk_values list...
 * @param char* end pointer to hs row end 
 * HS response for current mckey is contained between current_hs_in and end pointers.
 * e.g. '0	2	value1	value2'
 * @return 1 if we received data, 0 if we received a miss, -1 if error (sync_lost)
 */
int hs_thread::buffer_mc_value(int mc_protocol, bool cas, const mckey_data* get, char* end) {
  char mc_value[MC_VALUE_SIZE];
  const char* mc_key = get->mc_key.c_str();
  
  // <errorcode> <numcolumns> <r1> ... <rn>
  if (current_hs_in[0] == '0' && current_hs_in[1]=='\t') {
    current_hs_in += 2;
    int pending = end - current_hs_in;
    char* value_end = (char*) memchr(current_hs_in, '\t', pending);
    if (value_end == NULL || value_end>end) {
      config_manager *config = config_manager::get_instance();
      if (config->get_miss_ack()) {
        char miss_ack = 6; // 0x06
        add_mc_value(mc_protocol, cas, get, &miss_ack, 1);
      } else {
        add_mc_value(mc_protocol, cas, get, NULL, 0);
      }
      total_misses++; // stats
      return 0;
    } 
    current_hs_in = value_end + 1;

    // Check key fields
    if (check_pk_fields(get, end) == false) {
      return -1; // SYNC_LOST
    }
    // Prepare MC value string
    int bytes = end - current_hs_in;
    memcpy(mc_value, current_hs_in, bytes);
    // Append MC resposne to buffer
    add_mc_value(mc_protocol, cas, get, mc_value, bytes);
    return 1;

  } else if (current_hs_in[0] == '1') {
    char error_str[MAX_ERROR_SIZE];
    int bytes = end-current_hs_in;
    int error_len = min(bytes, MAX_ERROR_SIZE-1);
    strncpy(error_str, current_hs_in, error_len);
    error_str[error_len]=0;
    HSLOG_ERROR("Error fetching key (%s)\n", mc_key);
    HSLOG_ERROR("HS error: %s\n", error_str);
    return -1;
  } else {
    HSLOG_ERROR("Error fetching key (%s)\n", mc_key);
    HSLOG_ERROR("current_hs_in => %s\n", current_hs_in);
    return -1;
  }
}

/**
 * Append MC key response to mc_buffer
 * @oaram int mc_protocol MC_PROTOCOL_ASCII or MC_PROTOCOL_BINARY
 * @param bool cas  include cas field in ascii response if true
 * @param mckey_data* get contains mckey we asked for, pk_values list...
 * @param char* mc_value pointer mc value
 */
void hs_thread::add_mc_value(int mc_protocol, bool cas,  const mckey_data* get, char* mc_value, int bytes) {
  int opcode = get->req_type;
  const char* mc_key = get->mc_key.c_str();

  // ASCII
  if (mc_protocol == MC_PROTOCOL_ASCII) {
    if (mc_value != NULL) { // null == miss
      if (cas) {
        sprintf(current_mc, "VALUE %s 0 %d 0\r\n", mc_key, bytes);
      } else {
        sprintf(current_mc, "VALUE %s 0 %d\r\n", mc_key, bytes);
      }
      current_mc += strlen(current_mc);
      memcpy(current_mc, mc_value, bytes);
      current_mc += bytes;
      sprintf(current_mc, "\r\n");
      current_mc += strlen(current_mc);
    }

  // BINARY
  } else{
    uint32_t valuelen = 0;
    uint8_t extlen = 4; // 4 bytes flags
    uint8_t datatype = 0;
    uint16_t status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    if (mc_value == NULL) { // Miss
      if (opcode == PROTOCOL_BINARY_CMD_GETQ || 
          opcode == PROTOCOL_BINARY_CMD_GETKQ) {
        return;
      } else {
        status = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT; // Miss
      }
    } else {
      valuelen = bytes;
    }

    uint16_t keylen = 0;
    if (opcode == PROTOCOL_BINARY_CMD_GETK ||
        opcode == PROTOCOL_BINARY_CMD_GETKQ) {
      keylen = strlen(mc_key);
    }
    uint32_t bodylen = keylen + extlen + valuelen;

    int header_size = sizeof(protocol_binary_response_header); // 24
    memset(current_mc, 0, header_size);

    protocol_binary_response_header* header = (protocol_binary_response_header*) current_mc;
    header->response.magic = PROTOCOL_BINARY_RES;
    header->response.opcode = opcode;
    header->response.keylen = (uint16_t) htons(keylen);
    header->response.extlen = extlen;
    header->response.datatype = datatype;
    header->response.status = (uint16_t) htons(status);
    header->response.bodylen = (uint32_t) htonl(bodylen);
    header->response.opaque = get->opaque; // as we received it
    header->response.cas = 0;
    
    // flags
    memset(current_mc+header_size, 0, extlen);

    switch (opcode) {
      case PROTOCOL_BINARY_CMD_GET:
      case PROTOCOL_BINARY_CMD_GETQ:
        memcpy(current_mc+header_size+extlen, mc_value, valuelen);
        break;
      case PROTOCOL_BINARY_CMD_GETK:
      case PROTOCOL_BINARY_CMD_GETKQ:
        memcpy(current_mc+header_size+extlen, mc_key, keylen);
        memcpy(current_mc+header_size+extlen+keylen, mc_value, valuelen);
        break;
      default:
        HSLOG_ERROR("Unknown mc command %d\n", opcode);
        break;
    }

    DEBUG(100, print_mc_binary_response(log_file, current_mc));
    current_mc += header_size + bodylen;
  }
}
