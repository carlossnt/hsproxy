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

#include <event2/event.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <hsproxy.hpp>
#include <errno.h>
#include <fcntl.h>
#include <protocol_binary.h>
#include <sys/un.h>

#define POINTER_TO_INT(p) ((int)  (long) (p))
#define INT_TO_POINTER(i) ((void*) (long) (i))

using namespace std;

mc_conn_table conn_table;

mc_connection::mc_connection(int fd, sockaddr* client_addr) {
  pthread_mutex_init(&write_lock, NULL);
  buffer = (char*) malloc(REQ_BUFFER_SIZE);
  sa = client_addr;
  sock = fd;
  used = 0;
  read_event = NULL;
  conn_id = -1;
  cluster_id = -1;
  current_req = NULL;
}

mc_connection::~mc_connection() {
  HSLOG_INFO(60, "Closing connection fd=[%d]\n", sock);
  close(sock);
  free(sa);
  free(buffer);
  if (read_event) {
    event_del(read_event);
    event_free(read_event);
  }
  if (current_req) {
    free(current_req);
  }
  pthread_mutex_destroy(&write_lock);
}

/**
 * Send response in buffer to client
 * Before sending response, lock connection for writes (to avoid 
 *  several threads writing in the same socket at the same time).
 * @param char* res_buffer buffer begin
 * @param int total response length
 * @return int bytes sent, -1 if error
 */
int mc_connection::send_response(char* res_buffer, int total) {
  int offset=0;
  pthread_mutex_lock(&write_lock);
  while (offset<total) {
    int bytes = send(sock, res_buffer+offset, total-offset, 0);
    if (bytes<0 && errno!=EAGAIN) {
      pthread_mutex_unlock(&write_lock);
      return -1; // send error
    }
    if (bytes>0)
      offset += bytes;
  }
  pthread_mutex_unlock(&write_lock);
  return offset;
}


// Connections table --------------------

mc_conn_table::mc_conn_table() {
  pthread_rwlock_init(&table_lock, NULL);
  opened_conn = 0;
  open_conn = 0;
}

mc_conn_table::~mc_conn_table() {
  empty_table();
  pthread_rwlock_destroy(&table_lock);
}

/**
 * Return mc_connection identified by conn_id.
 * If conn_id is valid we also lock the table. Code using this method 
 *  should call unlock_table after using connection.
 * @param int conn_id connection id
 * @return mc_connection* connection data
 */
mc_connection* mc_conn_table::get_connection(int conn_id) {
  mc_connection* conn = NULL;
  if (connections.find(conn_id) != connections.end()) {
    conn = connections[conn_id];
  } else {
    HSLOG_INFO(60, "Connection %d does not exist\n", conn_id);
  }
  return conn;
}

void mc_conn_table::lock_table() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += 1;
  while (pthread_rwlock_timedrdlock(&table_lock, &ts)!=0) {
    HSLOG_INFO(20, "Unable to get rdlock (conn_table)... retry\n");
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;
  }
}

void mc_conn_table::unlock_table() {
  pthread_rwlock_unlock(&table_lock);
}

/**
 * Send response to client using conn_id
 * Lock table, send response, unlock table.
 * @param int conn_id connection id
 * @param char* buffer response begin
 * @param int bytes response length
 * @return int total bytes sent or -1 if error
 */
int mc_conn_table::send_response(int conn_id, char* buffer, int bytes) {
  int ret = -1;
  lock_table();
  mc_connection* conn = get_connection(conn_id);
  if (conn) {
    ret = conn->send_response(buffer, bytes);
    if (ret == -1) {
      HSLOG_ERROR("Error sending MC response (errno=%d)\n", errno);
    }
  }
  unlock_table();
  return ret;
}

/**
 * Add new connection to table. It return new conn_id.
 * @param mc_connection* conn connection data
 * @return int connection id
 */
int mc_conn_table::add_connection(mc_connection *conn) {
  pthread_rwlock_wrlock(&table_lock);
  int conn_id=-1;
  int max_connections = config_manager::get_instance()->get_max_connections();
  if (connections.size() < (size_t) max_connections) {
    for (int i=0; i<max_connections; i++) {
      conn_id = (last_id+i+1) % MAX_CONN_ID;
      // check if already exists
      if (connections.find(conn_id) == connections.end()) {
        connections[conn_id] = conn;
        last_id = conn_id;
        break;
      }
    }
  }
  open_conn++;
  opened_conn++;
  pthread_rwlock_unlock(&table_lock);
  return conn_id;
}

/**
 * Remove connection from table, destroy connection structure and release conn_id
 * @param int conn_id connection id to be closed
 */
void mc_conn_table::close_connection(int conn_id) {
  pthread_rwlock_wrlock(&table_lock);
  if (connections.find(conn_id) != connections.end()) {
    delete connections[conn_id];
    connections.erase(conn_id);
    open_conn--;
  }
  pthread_rwlock_unlock(&table_lock);
}

void mc_conn_table::empty_table() {
  map<int, mc_connection*>::iterator it;
  for (it=connections.begin(); it!=connections.end(); it++) {
    delete it->second;
    open_conn--;
  }
  connections.clear();
}


// MC connections queue ---------------

mc_conn_queue::mc_conn_queue() {
  pthread_mutex_init(&lock, NULL);
}

mc_conn_queue::~mc_conn_queue() {
  pthread_mutex_destroy(&lock);
}

void mc_conn_queue::push(mc_connection *conn) {
  pthread_mutex_lock(&lock);
  connections.push(conn);
  pthread_mutex_unlock(&lock);
}

mc_connection* mc_conn_queue::pop() {
  mc_connection* conn = NULL;
  pthread_mutex_lock(&lock);
  if (!connections.empty()) {
    conn = connections.front();
    connections.pop();
  }
  pthread_mutex_unlock(&lock);
  return conn;
}

void mc_conn_queue::empty_queue() {
  pthread_mutex_lock(&lock);
  while (!connections.empty()) {
    connections.pop();
  }
  pthread_mutex_unlock(&lock);
}


// MC Pool ----------------------------

/**
 * Release resorces. 
 * Pool must be stopped before (parsers and listener canceled)
 */
mc_pool::~mc_pool() {
  // release resources (socket...)
  if (mc_sock>0){
    close(mc_sock);
    conn_table.empty_table();
    delete listener;
    for (int i=0; i<size; i++) {
      delete parsers[i];
    }
    parsers.clear();
  }
}

/**
 * Init MC socket, start listener and parsers
 * @return 0 if OK, -1 if error
 */
int mc_pool::init_pool() {
  // Start MC parsers
  for (int i=0; i<size; i++) {
    mc_parser *thr = new mc_parser(this);
    thr->start_thread();
    parsers.push_back(thr);
  }

  // Start MC listener
  init_mcsocket();
  if (mc_sock < 1) {
    HSLOG_ERROR("Error setting MC socket....\n");
    return -1;
  }
  listener = new mc_listener(this);
  listener->start_thread();

  return 0;
}

/**
 * Cancel listener and parsers. Wait for threads.
 */
void mc_pool::stop_pool() {
  if (mc_sock>0) {
    // Stop MC listener
    pthread_t thr_id = listener->get_thread_id();
    listener->cancel_thread();
    pthread_join(thr_id, NULL);
    // Stop MC parsers
    for (int i=0; i<size; i++) {
      thr_id = parsers[i]->get_thread_id();
      parsers[i]->cancel_thread();
      pthread_join(thr_id, NULL);
    }
  }
}

/**
 * Add new connection to conn_table and assign it to a parser.
 * The same parser will parse all connection requests.
 * @param mc_connection* conn connection data
 */
void mc_pool::assign_connection(mc_connection* conn) {
  int conn_id = conn_table.add_connection(conn);
  if (conn_id == -1) {
    HSLOG_ERROR("Could not add the connection to conn_table\n");
    delete conn;
  } else {
    conn->conn_id = conn_id;
    // assign parser for connection (round-robin)
    int id = (last++)%size;
    parsers[id]->push_connection(conn);
  }
}

/**
 * Init local socket to accept connections from local MC clients
 * Used when proxy is in the same machine that clients
 * @param char* path path of socket file
 */
void mc_pool::init_unix_socket(const char* path) {
  int sock = -1;
  socklen_t slen = sizeof(sockaddr_un);
  sockaddr_un sun;
  sun.sun_family = AF_UNIX;
  strcpy(sun.sun_path, mc_sock_file.c_str());
  HSLOG_INFO(20, "Using AF_UNIX socket (%s)\n", path);

  // check if sockfile already exists
  struct stat st;
  int status = stat (path, &st);
  if (status == 0 && (st.st_mode & S_IFMT) == S_IFSOCK) {
    status = unlink (path);
    HSLOG_INFO(20, "Unlinking the socket node (%s)\n", path);
  }

  sock = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sock > 0) {
    if (bind(sock, (const sockaddr*) &sun, slen) < 0) {
      HSLOG_ERROR("Error binding MC socket, errno=%i port=%i\n", errno, mc_port);
    } else {
      fcntl(sock, F_SETFL, O_NONBLOCK);
      listen(sock, 256);
      mc_sock = sock;
      HSLOG_INFO(20, "Init TCP listener (fd=[%d])\n", sock);
    }
  }
}

/**
 * Init TCP socket to accept connections from MC clients
 * Clients can be local or remote.
 * @param int port TCP port where we'll listen
 */
void mc_pool::init_inet_socket(int port) {
  int sock = -1;
  socklen_t slen = sizeof(sockaddr_in);
  sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons((unsigned short) port);
  HSLOG_INFO(20, "Using AF_INET socket (port=%d)\n", mc_port);
  sock = socket(AF_INET, SOCK_STREAM, 0);

  int on = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
    HSLOG_ERROR("setsockopt() SO_REUSEADDR failed, errno=%i\n", errno);
  }

  if (sock > 0) {
    if (bind(sock, (const sockaddr*) &sin, slen) < 0) {
      HSLOG_ERROR("Error binding MC socket, errno=%i port=%i\n", errno, mc_port);
    } else {
      fcntl(sock, F_SETFL, O_NONBLOCK);
      listen(sock, 256);
      mc_sock = sock;
      HSLOG_INFO(20, "Init TCP listener (fd=[%d])\n", sock);
    }
  }
}

void mc_pool::init_mcsocket() {
  if (use_unix_sock) {
    init_unix_socket(mc_sock_file.c_str());
  } else {
    init_inet_socket(mc_port);
  }
}
 
/**
 * Send response to the client using conn_id
 * @param int conn_id connection id
 * @param char* buffer response begin
 * @param int bytes response length
 * @return int total bytes sent or -1 if error
 */
int mc_pool::send_response(int conn_id, char* buffer, int bytes) {
  return conn_table.send_response(conn_id, buffer, bytes);
}

int mc_pool::get_open_connections() {
  return conn_table.get_open_connections();
}

int mc_pool::get_opened_connections() {
  return conn_table.get_opened_connections();
}



// MC Listener ------------------------

mc_listener::mc_listener(mc_pool* p) {
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sig_sock) < 0) {
    HSLOG_ERROR("Error opening stream socket pair (sig_sock)\n");
  }
  fcntl(sig_sock[0], F_SETFL, O_NONBLOCK);
  fcntl(sig_sock[1], F_SETFL, O_NONBLOCK);

  base = NULL;
  ev_accept = NULL;
  pool = p;
}

mc_listener::~mc_listener() {
  close(sig_sock[1]);
  close(sig_sock[0]);
  if (ev_accept) {
    event_del(ev_accept);
    event_free(ev_accept);
  }
  if (ev_cancel) {
    event_del(ev_cancel);
    event_free(ev_cancel);
  }
  if (base) event_base_free(base);
}

void mc_listener::start_thread() {
  int rc = pthread_create(&thd_id, NULL, thread_func, static_cast<void*>(this));
  if (rc != 0) {
    HSLOG_ERROR("Error in thread creation (errno = %i)\n", errno);
    return;
  }
}

void* mc_listener::thread_func(void *this_arg) {
  mc_listener *pthis = static_cast<mc_listener*>(this_arg);
  pthis->run();
  return NULL;
}

/**
 * Cancel thread associated to mc_listener instance
 * Trigger a ev_cancel event.
 */
void mc_listener::cancel_thread() {
  char flag = 1;
  send(sig_sock[0], &flag, 1, 0);
}

/**
 * Handler for ev_cancel event (someone calls cancel_thread)
 * Thread exits.
 */
void mc_listener::cancel_handler(evutil_socket_t fd, short events, void *arg) {
  HSLOG_INFO(20, "%lu: MC listener pthread_exit\n", pthread_self());
  pthread_exit(NULL);
}

/**
 * Main loop. MC listener will wait for ev_cancel and ev_accept events.
 */
void mc_listener::run() {
  base = event_base_new();
  event_base_priority_init(base, 2);
  ev_accept = event_new(base, pool->get_mc_sock(), 
      EV_READ|EV_PERSIST, accept_handler, (void*) this);
  ev_cancel = event_new(base, sig_sock[1],
      EV_READ|EV_PERSIST, cancel_handler, (void*) this);
  event_priority_set(ev_cancel, 0); // max priority
  event_priority_set(ev_accept, 1);
  event_add(ev_cancel, NULL);
  event_add(ev_accept, NULL);
  HSLOG_INFO(20, "MC listener thread started\n");
  event_base_dispatch(base);
}

/**
 * Handler executed when ev_accept is triggered (MC client connecting).
 * Create mc_connection struct and assign it to a parser.
 */
void mc_listener::accept_handler(evutil_socket_t fd, short events, void *arg) {
  mc_listener* thr = static_cast<mc_listener*>(arg);
  socklen_t slen = thr->get_sockaddr_size();
  sockaddr* sa = (sockaddr*) malloc(slen);

  int sock = accept(fd, sa, &slen);
  if (sock < 0) {
    HSLOG_ERROR("Error accepting MC connection %d\n", errno);
    free(sa);
  } else if (sock > FD_SETSIZE) {
    free(sa);
    close(sock);
  } else {
    HSLOG_INFO(60, "Accepting connection fd=[%d]\n", sock);
    fcntl(sock, F_SETFL, O_NONBLOCK);
    // Disable Nagle algorithm
    int flag = 1;
    if (!thr->pool->get_use_unix_sock()) {
      if (setsockopt(sock,  IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int)) < 0) {
        HSLOG_ERROR("setsockopt() TCP_NODELAY failed, errno=%i\n", errno);
      }
    }
    mc_connection* conn = new mc_connection(sock, sa);
    thr->pool->assign_connection(conn);
  }
}

int mc_listener::get_sockaddr_size() {
  int size = sizeof(sockaddr_in);
  if (pool->get_use_unix_sock()) {
    size = sizeof(sockaddr_un);
  }
  return size;
}


// MC Parser --------------------------

mc_parser::mc_parser(mc_pool *p) {
  // socketpair to signal exit
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sig_sock) < 0) {
    HSLOG_ERROR("Error opening stream socket pair (sig_sock)\n");
  }
  fcntl(sig_sock[0], F_SETFL, O_NONBLOCK);
  fcntl(sig_sock[1], F_SETFL, O_NONBLOCK);

  // socketpair to signal new connections
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, req_sock) < 0) {
    HSLOG_ERROR("MC Pool: Error opening stream socket pair (req_sock)\n");
  }
  fcntl(req_sock[0], F_SETFL, O_NONBLOCK);
  fcntl(req_sock[1], F_SETFL, O_NONBLOCK);

  base = NULL;
  pool = p;
}

mc_parser::~mc_parser() {
  close(sig_sock[1]);
  close(sig_sock[0]);
  close(req_sock[1]);
  close(req_sock[0]);
  conn_queue.empty_queue();
  if (ev_cancel) {
    event_del(ev_cancel);
    event_free(ev_cancel);
  }
  if (ev_new_conn) {
    event_del(ev_new_conn);
    event_free(ev_new_conn);
  }
  if (base) event_base_free(base);
}

void* mc_parser::thread_func(void *this_arg) {
  mc_parser *pthis = static_cast<mc_parser*>(this_arg);
  pthis->run();
  return NULL;
}

void mc_parser::start_thread() {
  int rc = pthread_create(&thd_id, NULL, thread_func, static_cast<void*>(this));
  if (rc != 0) {
    HSLOG_ERROR("Error in thread creation (errno = %i)\n", errno);
    return;
  }
}

/**
 * Cancel thread associated to mc_listener instance
 * Trigger a ev_cancel event.
 */
void mc_parser::cancel_thread() {
  char flag = 1;
  send(sig_sock[0], &flag, 1, 0);
}

/**
 * Handler for ev_cancel event (someone calls cancel_thread)
 * Thread exits.
 */
void mc_parser::cancel_handler(evutil_socket_t fd, short events, void *arg){
  HSLOG_INFO(20, "%lu: MC parser pthread_exit\n", pthread_self());
  pthread_exit(NULL);
}

/**
 * Main loop. MC listener will wait for ev_cancel and ev_new_conn 
 *  and eventually ev_read events
 */
void mc_parser::run() {
  base = event_base_new();
  event_base_priority_init(base, 3);
  ev_new_conn = event_new(base, req_sock[1],
      EV_READ|EV_PERSIST, new_conn_handler, (void*) this);
  ev_cancel = event_new(base, sig_sock[1],
      EV_READ|EV_PERSIST, cancel_handler, (void*) this);
  event_priority_set(ev_cancel, 0); // max priority
  event_priority_set(ev_new_conn, 1);
  event_add(ev_cancel, NULL);
  event_add(ev_new_conn, NULL);
  HSLOG_INFO(20, "MC parser thread started\n");
  event_base_dispatch(base);
}

/**
 * Assign a connection to the parser
 * Trigger a ev_new_conn event
 */
void mc_parser::push_connection(mc_connection *conn) {
  conn_queue.push(conn);
  char flag = 1;
  send(req_sock[0], &flag, 1, 0);
}

/**
 * Handler for ev_new_conn event
 * New connection has been assigned to parser.
 * Add ev_read event to event_base
 */
void mc_parser::new_conn_handler(evutil_socket_t fd, short events, void *arg) {
  mc_parser* thr = static_cast<mc_parser*>(arg);
  char buffer[1];
  int bytes;
  while ((bytes=recv(fd, buffer, 1, 0))>0) {
    mc_connection *conn = NULL;
    while ((conn = thr->conn_queue.pop())) {
      event* re = event_new(thr->base, conn->sock, EV_READ|EV_PERSIST,
          read_handler, INT_TO_POINTER(conn->conn_id));
      event_priority_set(re, 2);
      conn->set_read_event(re);
      event_add(re, NULL);
    }
  }
}

void mc_parser::read_handler(evutil_socket_t fd, short events, void *arg) {
  mc_parser* thr = static_cast<mc_parser*>(arg);
  int conn_id = POINTER_TO_INT(arg);
  thr->read_mcbuffer(fd, conn_id);
}

/**
 * Process mc buffer contents (parse requests, build hs_requests and send them to proper hs_pools)
 * This method is called when parser receives requests from clients.
 * We conn_table connection when reading from mc buffer
 * @param int fd socket we are going to read from
 * @param int conn_id connection id (we'll append read data to connection buffer)
 */
void mc_parser::read_mcbuffer(int fd, int conn_id) {
  int bytes;
  int pending = 0;

  conn_table.lock_table();
  mc_connection *conn = conn_table.get_connection(conn_id);
  conn_table.unlock_table();
  if (conn == NULL) {
    return; // conn no more exists
  }

  while (1) {
    char* current = conn->buffer + conn->used;
    int empty_size = REQ_BUFFER_SIZE - conn->used;
    if (empty_size==0) {
      HSLOG_ERROR("Error receiving request (MC buffer full) %d bytes\n", conn->used);
      conn_table.close_connection(conn_id);
      return;
    }
    bytes = recv(fd, current, empty_size, 0);
    if (bytes<= 0)
      break;

    conn->used += bytes;
    // Dispatch complete requests (ascii) or complete gets (binary)
    if (conn->used > 0) {
      if (is_ascii_request(conn)) {
        while (pending_ascii_requests(conn)) {
          HSLOG_INFO(90, "Execute ascii request\n");
          pending = dispatch_ascii_request(conn);
          if (pending==-1) break;
          memcpy(conn->buffer, conn->buffer+conn->used-pending, pending);
          conn->used = pending;
          conn->buffer[conn->used] = 0;
        }
      } else { // binary protocol
        HSLOG_INFO(90, "Execute binary request\n");
        if (config_manager::get_instance()->get_mc_quiet_mode()) {
          pending = dispatch_binary_quiet_request(conn); 
        } else {
          pending = dispatch_binary_request(conn);
        }
        if (pending>=0) {
          memcpy(conn->buffer, conn->buffer+conn->used-pending, pending);
          conn->used = pending;
        }
      }
    }
  }

  // Close connection if necessary
  if (bytes==0 || pending==-1) {
    conn_table.close_connection(conn_id);
  } else if (bytes< 0 && errno!=EAGAIN) {
    HSLOG_ERROR("Error recv %d\n", errno);
    conn_table.close_connection(conn_id);
  }
}

/**
 * Detect if client request is using ascci or binary protocol
 * It just checks request first byte
 * @param mc_connection* conn connection data (includes buffer)
 * @return true if is ascii request, false if is binary
 */
bool mc_parser::is_ascii_request(mc_connection *conn) {
  return (uint8_t) conn->buffer[0] != (uint8_t) PROTOCOL_BINARY_REQ; // 0x80
}

/**
 * Return true if buffer contains '\n' (in other words, if there's 
 *  at least a complete request to be dispatched)
 * @param mc_connection* conn connection data (includes buffer)
 * @return true if there is a pending request
 */
bool mc_parser::pending_ascii_requests(mc_connection *conn) {
  conn->buffer[conn->used]=0;
  return (strchr(conn->buffer, '\n')!=NULL);
}

/**
 * Parse MC quiet gets (contained in buffer), build hs_request and push it 
 *  to the proper hs_pool only after receiving noop request (until then, 
 *  mc_connection keeps a pointer to the live request).
 * @param mc_connection* conn connection data (including conn_id, current_req and buffer)
 * @return int pending bytes or -1 if quit received
 */
int mc_parser::dispatch_binary_quiet_request(mc_connection *conn) {
  char *current = conn->buffer;
  char *last = current + conn->used;
  int pending = last-current;
  int header_size = sizeof(protocol_binary_request_header);
  mckey_data *key_data = NULL;

  if (conn->current_req == NULL) {
    conn->current_req = new hs_request(conn->conn_id, MC_PROTOCOL_BINARY);
  }
  hs_request *hs_req = conn->current_req;

  // Read header if we have > 24 bytes
  while (pending >= header_size) {
    protocol_binary_request_header* header = (protocol_binary_request_header*) current;
    uint16_t keylen = ntohs(header->request.keylen);
    uint32_t bodylen = ntohl(header->request.bodylen);
    uint8_t extlen = header->request.extlen;
    DEBUG(100, print_mc_binary_request(log_file, current));

    // Read key if possible
    if (pending >= header_size + (int)bodylen) {
      char* keypos = current + header_size + extlen;
      switch (header->request.opcode) {
        case PROTOCOL_BINARY_CMD_GETQ:
        case PROTOCOL_BINARY_CMD_GETKQ:
          key_data = parse_key_content(keypos, keypos + keylen);
          if (key_data) {
            key_data->req_type = header->request.opcode;
            key_data->opaque = header->request.opaque;
            if (conn->cluster_id == -1) {
              conn->cluster_id = key_data->cluster_id;
            }
            if (conn->cluster_id == key_data->cluster_id) {
              if (hs_req->gets.size() < MAX_KEYS_PER_REQUEST) {
                hs_req->gets.push_back(key_data);
              } else {
                HSLOG_ERROR("Too many keys in request... dropping key\n");
                delete key_data; // ignore key
              }
            } else { // different cluster (send buffered requests)
              if (hs_req->gets.size() > 0) {
                HSLOG_ERROR("Keys from different clusters in the same request\n");
                delete key_data; // ignore key
              }
            }
          }
          break;

        case PROTOCOL_BINARY_CMD_NOOP:
          hs_req->noop = true;
          hs_req->noop_opaque = header->request.opaque;
          HSLOG_INFO(100, "push sender HS request\n");
          hsproxy::get_instance()->push_cluster_request(conn->cluster_id, hs_req);
          conn->cluster_id = -1;
          conn->current_req = new hs_request(conn->conn_id, MC_PROTOCOL_BINARY);
          hs_req = conn->current_req;
          break;

        case PROTOCOL_BINARY_CMD_QUIT:
          // send QUIT response and close connection
          send_command_response(conn->conn_id, MC_COMMAND_QUIT,
              MC_PROTOCOL_BINARY, header->request.opaque);
          return -1;
          break;

        case PROTOCOL_BINARY_CMD_FLUSH:
          // send OK
          send_command_response(conn->conn_id, MC_COMMAND_FLUSH,
              MC_PROTOCOL_BINARY, header->request.opaque);
          break;

        case PROTOCOL_BINARY_CMD_STAT:
          // send stats
          send_command_response(conn->conn_id, MC_COMMAND_STAT,
              MC_PROTOCOL_BINARY, header->request.opaque);
          break;

        default:
          HSLOG_ERROR("Operation not supported (opcode=%d)\n", header->request.opcode);
          break;
      }
      current += header_size + bodylen;
      pending = last - current;
    } else {
      break; // we need more bytes to complete request
    }
  }

  memcpy(conn->buffer, conn->buffer+conn->used-pending, pending);
  conn->used = pending;

  return pending;
}

/**
 * Parse MC gets (contained in buffer), build hs_requests and push them
 *  to the proper hs_pools (depending on cluster_ids)
 * @param mc_connection* conn connection data (including conn_id and buffer)
 * @return int pending bytes or -1 if quit received
 */
int mc_parser::dispatch_binary_request(mc_connection *conn) {
  char *current = conn->buffer;
  char *last = current + conn->used;
  int pending = last-current;
  int header_size = sizeof(protocol_binary_request_header);

  mckey_data *key_data = NULL;
  hs_request *hs_req = new hs_request(conn->conn_id, MC_PROTOCOL_BINARY);

  // Read header if we have > 24 bytes
  while (pending >= header_size) {
    protocol_binary_request_header* header = (protocol_binary_request_header*) current;
    uint16_t keylen = ntohs(header->request.keylen);
    uint32_t bodylen = ntohl(header->request.bodylen);
    uint8_t extlen = header->request.extlen;
    DEBUG(100, print_mc_binary_request(log_file, current));

    // Read key if possible
    if (pending >= header_size + (int)bodylen) {
      char* keypos = current + header_size + extlen;
      switch (header->request.opcode) {
        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_GETK:
          key_data = parse_key_content(keypos, keypos + keylen);
          if (key_data) {
            key_data->req_type = header->request.opcode;
            key_data->opaque = header->request.opaque;
            if (conn->cluster_id == -1) {
              conn->cluster_id = key_data->cluster_id;
            }
            // Flush if different cluster_id or max_keys
            if (conn->cluster_id != key_data->cluster_id ||
                hs_req->gets.size() == MAX_KEYS_PER_REQUEST) {
              HSLOG_INFO(100, "push sender HS request\n");
              hsproxy::get_instance()->push_cluster_request(conn->cluster_id, hs_req);
              hs_req = new hs_request(conn->conn_id, MC_PROTOCOL_BINARY);
            }
            hs_req->gets.push_back(key_data);
            conn->cluster_id = key_data->cluster_id;
          }
          break;

        case PROTOCOL_BINARY_CMD_QUIT:
          // send QUIT response and close connection
          send_command_response(conn->conn_id, MC_COMMAND_QUIT, 
              MC_PROTOCOL_BINARY, header->request.opaque);
          return -1;
          break;

        case PROTOCOL_BINARY_CMD_FLUSH:
          // send OK
          send_command_response(conn->conn_id, MC_COMMAND_FLUSH,
              MC_PROTOCOL_BINARY, header->request.opaque);
          break;

        case PROTOCOL_BINARY_CMD_STAT:
          // send stats
          send_command_response(conn->conn_id, MC_COMMAND_STAT,
              MC_PROTOCOL_BINARY, header->request.opaque);
          break;

        default:
          HSLOG_ERROR("Operation not supported (opcode=%d)\n", header->request.opcode);
          break;
      }
      current += header_size + bodylen;
      pending = last - current;
    } else {
      break; // we need more bytes to complete request
    }
  }

  // Send pending requests
  if (hs_req->gets.size() > 0) {
    HSLOG_INFO(100, "push sender HS request\n");
    hsproxy::get_instance()->push_cluster_request(conn->cluster_id, hs_req);
  } else {
    delete hs_req;
  }

  memcpy(conn->buffer, conn->buffer+conn->used-pending, pending);
  conn->used = pending;

  return pending;
}

/**
 * Get MC command from ASCII request
 * HSProxy only implement get, gets, quit and flush_all
 * @param char* current request begin
 * @return int MC_COMMAND_GET, MC_COMMAND_QUIT... MC_COMMAND_UNKNOWN
 */
int mc_parser::get_ascii_command(char* current) {
  int mc_command = MC_COMMAND_UNKNOWN;
  if (strncmp(current, "gets", 4) == 0) {
    mc_command = MC_COMMAND_GETS;
  } else if (strncmp(current, "get", 3) == 0) {   
    mc_command = MC_COMMAND_GET;
  } else if (strncmp(current, "quit", 4) == 0) { 
    mc_command = MC_COMMAND_QUIT;
  } else if (strncmp(current, "flush_all", 9) == 0) {
    mc_command = MC_COMMAND_FLUSH;
  } else if (strncmp(current, "stats", 5) == 0) {
    mc_command = MC_COMMAND_STAT;
  }
  return mc_command;
}

/**
 * Parse request MC keys and add them to hs_req object
 * @param hs_request* hs_req request data
 * @param char* beginln ascii request begin
 * @param char* endln ascii request end
 * @param int cluster ID where keys are stored
 */
int mc_parser::add_request_keys(hs_request* hs_req, char* beginln, char* endln) {
  int cluster_id = -1;
  char* current = beginln;
  // skip command name
  char* key_start = strchr(current, ' ');
  if (key_start != NULL && key_start<endln) {
    current = key_start+1;
    while (current<endln) {
      char *token_end = strchr(current, ' ');
      if (token_end == NULL || token_end>endln) {
        token_end = endln; // last request token
      }
      mckey_data *key_data = parse_key_content(current, token_end);
      current = token_end+1;
      if (key_data != NULL) {
        // if it is the first key, set cluster id
        if (cluster_id == -1 || cluster_id == key_data->cluster_id) {
          cluster_id = key_data->cluster_id;
          hs_req->gets.push_back(key_data);
        } else {
          // ignore keys from other cluster_id
          delete key_data;
        }
      }
    }
  }
  return cluster_id;
}

/**
 * Parse the whole MC request (contained in buffer), build hs_request
 *  and push it to the proper hs_pool (depending on the cluster_id)
 * @param mc_connection* conn connection data (including conn_id and buffer)
 * @return int pending bytes or -1 if 'quit' received
 */
int mc_parser::dispatch_ascii_request(mc_connection* conn) {
  char *current = conn->buffer;
  char *last = current + conn->used;
  conn->buffer[conn->used]=0;
  int cluster_id = -1;
  hs_request* hs_req = NULL;

  if (conn->used>0) {
    char *endln = NULL;
    // foreach line
    while ((endln=strchr(current, '\r'))!=NULL
          || (endln=strchr(current, '\n'))!=NULL) {
      HSLOG_INFO(100, "Parse request (%d bytes)\n", conn->used);
      HSLOG_INFO(100, ".%s.\n", conn->buffer);

      // Process request
      int mc_command = get_ascii_command(current);
      bool ok = false;
      switch (mc_command) {
        // Get/Gets (0,1,N keys)
        case MC_COMMAND_GETS:
        case MC_COMMAND_GET:
          hs_req = new hs_request(conn->conn_id, MC_PROTOCOL_ASCII);
          hs_req->cas = (mc_command == MC_COMMAND_GETS);
          cluster_id = add_request_keys(hs_req, current, endln);
          // Push HS request
          if (hs_req->gets.size() > 0 && cluster_id != -1) {
            HSLOG_INFO(100, "push sender  (%p) value=%s\n",
                hs_req, hs_req->gets[0]->mc_key.c_str());
            ok = hsproxy::get_instance()->push_cluster_request(cluster_id, hs_req);
          }

          if (!ok) {
            HSLOG_INFO(80, "Error processing request, returning empty response\n");
            char res[6] = "END\r\n";
            conn_table.send_response(conn->conn_id, res, 5);
            delete hs_req;
          }
          break;

        // Quit (just close connection)
        case MC_COMMAND_QUIT:
          HSLOG_INFO(80, "Quit received, closing connection\n");
          return -1;
          break;

        // Flush (just return OK\r\n)
        case MC_COMMAND_FLUSH:
          HSLOG_INFO(80, "Flush received\n");
          send_command_response(conn->conn_id, MC_COMMAND_FLUSH, MC_PROTOCOL_ASCII, 0);
          break;

        // Stats (send stats)
        case MC_COMMAND_STAT:
          HSLOG_INFO(80, "Stats received\n");
          send_command_response(conn->conn_id, MC_COMMAND_STAT, MC_PROTOCOL_ASCII, 0);
          break;

        // Unknown or not supported
        default:
          HSLOG_INFO(80, "Unknown command: %s\n", conn->buffer);
          break;
      }
      // move to endln
      current = endln+1;
      if (*current == '\n') current++;
    }
  }

  int pending = last-current;
  memcpy(conn->buffer, conn->buffer+conn->used-pending, pending);
  conn->used = pending;
  conn->buffer[conn->used] = 0;

  return last-current;
}

/**
 * Build ascii/binary response for stats request
 * @param int protocol (binary or ascii)
 * @param int bytes response length
 * @param uint32_t opaque opaque value to use for binary responses
 * @return char* pointer to buffered response
 */
char* mc_parser::get_stats_response(int protocol, int &bytes, uint32_t opaque) {
  map<string, string> stats = hsproxy::get_instance()->get_stats();
  map<string, string>::iterator it;
  char *res = (char*) malloc(STATS_BUFFER);
  char *curr_stats = res;

  if (protocol==MC_PROTOCOL_ASCII) {
    for (it=stats.begin(); it!=stats.end(); it++) {
      sprintf(curr_stats, "STAT %s %s\r\n", it->first.c_str(), it->second.c_str());
      curr_stats += strlen(curr_stats);
    }
    sprintf(curr_stats, "END\r\n");
    curr_stats += strlen(curr_stats);
  }

  if (protocol==MC_PROTOCOL_BINARY) {
    int header_size = sizeof(protocol_binary_response_header);
    // 1 response per stat
    for (it=stats.begin(); it!=stats.end(); it++) {
      protocol_binary_response_header* header = (protocol_binary_response_header*) curr_stats;
      string stat_key = it->first;
      string stat_value = it->second;
      uint16_t keylen = stat_key.length();
      uint32_t bodylen = keylen + stat_value.length();

      memset(curr_stats, 0, header_size);
      header->response.magic = PROTOCOL_BINARY_RES;
      header->response.opcode = PROTOCOL_BINARY_CMD_STAT;
      header->response.status = PROTOCOL_BINARY_RESPONSE_SUCCESS; 
      header->response.opaque = opaque; // as we receive it
      header->response.keylen = (uint16_t) htons(keylen);
      header->response.bodylen = (uint32_t) htonl(bodylen);

      memcpy(curr_stats + header_size, stat_key.c_str(), stat_key.length());
      memcpy(curr_stats + header_size + keylen, stat_value.c_str(), stat_value.length());

      DEBUG(100, print_mc_binary_response(log_file, curr_stats));
      curr_stats += header_size + bodylen;
    }
    // Empty stat to finish
    protocol_binary_response_header* header = (protocol_binary_response_header*) curr_stats;
    memset(curr_stats, 0, header_size);
    header->response.magic = PROTOCOL_BINARY_RES;
    header->response.opcode = PROTOCOL_BINARY_CMD_STAT;
    header->response.status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    header->response.opaque = opaque; // as we receive it
    DEBUG(100, print_mc_binary_response(log_file, curr_stats));
    curr_stats += header_size;
  }

  bytes = curr_stats - res;
  return res;
}

/**
 * Send a prebuilt response to client for some commands like flush or quit
 * @param int conn_id client connection id
 * @param int command MC command received
 * @param int protocol ascii or binary
 * @param uint32_t request opaque value (used in binary protocol)
 */
void mc_parser::send_command_response(int conn_id, int command, int protocol, uint32_t opaque) {
  if (command==MC_COMMAND_STAT) {
    int bytes = 0;
    char* res = mc_parser::get_stats_response(protocol, bytes, opaque);
    conn_table.send_response(conn_id, res, bytes);
    free(res);

  } else if (protocol==MC_PROTOCOL_ASCII && command==MC_COMMAND_FLUSH) {
    char res[5] = "OK\r\n";
    conn_table.send_response(conn_id, res, strlen(res));

  } else if (protocol==MC_PROTOCOL_BINARY) {
    int header_size = sizeof(protocol_binary_response_header);
    char* res = (char*) calloc(1, sizeof(header_size));
    protocol_binary_response_header* header = 
        (protocol_binary_response_header*) res;
    header->response.magic = PROTOCOL_BINARY_RES;
    header->response.status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    header->response.opaque = opaque;
    switch (command) {
      case MC_COMMAND_FLUSH:
        header->response.opcode = PROTOCOL_BINARY_CMD_FLUSH;
        break;
      case MC_COMMAND_QUIT:
        header->response.opcode = PROTOCOL_BINARY_CMD_QUIT;
        break;
    }
    conn_table.send_response(conn_id, res, header_size);
    DEBUG(100, print_mc_binary_response(log_file, res));
    free(res);
  }
}

/**
 * Parse MCkey and fill mckey_data with key contents
 * @param char* current pointer to mckey begin in char buffer
 * @param char* token_end pointer to mckey end in char buffer
 * @return mckey_data* pointer to struct containing mckey parsed info
 *
 * MC key pattern
 * <target_name>:<partition>@<version>:<pk>
 * note: use '_' to separate values when pk is multicolumn
 *
 * examples:
 * user_basic:27@4:61340027
 * photos:345@2:98765_10345
 */
mckey_data* mc_parser::parse_key_content(char *current, char *token_end) {
  mckey_data* req = new mckey_data();
  const storage_fields* meta;
  config_manager *config = config_manager::get_instance();

  req->mc_key = string(current, token_end-current);
  if (token_end-current > MAX_KEY_LENGTH) {
    HSLOG_ERROR("MC key bigger than MAX_KEY_LENGTH (%d) %s", MAX_KEY_LENGTH, req->mc_key.c_str());
  }

  // Get target_name
  char *pos = strchr(current, ':');
  if (pos==NULL || pos>token_end) {
    delete req;
    return NULL; // Wrong format
  }
  req->target_name = string(current, pos-current);
  current = pos+1;

  // Get partition (and table_name)
  pos = strchr(current, '@');
  if (pos==NULL || pos>token_end) {
    delete req;
    return NULL; // Wrong format
  }
  string partition_str = string(current, pos-current);
  req->table_name = req->target_name + partition_str;
  current = pos+1;

  // Get cluster_id
  req->cluster_id = config->get_table_cluster(req->table_name);
  if (req->cluster_id == 0) {
    HSLOG_INFO(20, "Unknown table %s\n", req->table_name.c_str());
    delete req;
    return NULL; // Unknown table
  }

  // Parse version
  pos = strchr(current, ':');
  if (pos==NULL || pos>token_end) {
    delete req;
    return NULL; // Wrong format
  }
  req->version = atoi(current); // atoi end when finding ':' char
  current = pos+1;

  // Set index_id
  req->index_id = config->get_hs_index_id(req->table_name, req->version);

  // retrieve target metadata
  meta = config->get_target_metadata(req->target_name, req->version);
  size_t num_pk_fields = meta->key_fields.size();

  // Parse PK values
  while (current <= token_end) {
    pos = strchr(current, '_');
    if (pos == NULL || pos > token_end) {
      pos = token_end;
    } 
    req->pk_values.push_back(string(current, pos-current));
    current = pos+1;
  }
  if (req->pk_values.size() != num_pk_fields) {
    delete req;
    return NULL; // Error fetching PK values
  }

  return req;
}
