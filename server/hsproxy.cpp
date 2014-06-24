/**
 * Proxy instance (pointers to all threads)
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#include <hsproxy.hpp>
#include <signal.h>
#include <errno.h>
#include <stdlib.h>

hsproxy* hsproxy::instance = NULL;

hsproxy* hsproxy::get_instance() {
  if (!instance) {
    instance = new hsproxy();
  }
  return instance;
}

void hsproxy::init_proxy(char* config_path, int mc_port_override,
         string db_name_override, string sock_file_override) {
  // disable sigpipe
  signal(SIGPIPE, SIG_IGN);

  // Init proxy
  config = config_manager::get_instance();
  config->init(config_path, mc_port_override, db_name_override, sock_file_override);
  gettimeofday(&start_time, NULL);
}

void hsproxy::init_pools() {
  // start HS pools
  init_hs_pools();

  // start mc listener
  int mc_port = config->get_mc_port();
  int size = config->get_num_parsers();
  if (mc_port>0) {
    mcpool = new mc_pool(mc_port, size);
  } else {
    string sock_file = config->get_mc_sock_file();
    mcpool = new mc_pool(sock_file, size);
  }
  mcpool->init_pool();
}

void hsproxy::stop_pools() {
  // stop mc listener
  if (mcpool) {
    mcpool->stop_pool();
    HSLOG_INFO(20, "Stop mc listener\n");
  }

  // stop hs threads
  stop_hs_pools();
  HSLOG_INFO(20, "Stop HS threads\n");

  // we need to stop HS threads before deleting mcpool
  //  because it contains conn_table
  delete mcpool;
}

void hsproxy::shutdown() {
  stop_pools();
  delete config;
}

void hsproxy::init_hs_pools() {
  vector<int> cluster_ids = config->get_clusters();
  int size = config->get_hs_pool_size();
  // foreach cluster, open TCP connection/s
  for (size_t i=0; i<cluster_ids.size(); i++) {
    int cluster_id = cluster_ids[i];
    hs_pools[cluster_id] = new hs_pool(cluster_id, size);
    hs_pools[cluster_id]->init_pool();
    HSLOG_INFO(20, "Init HS pool for cluster %d\n", cluster_id);
  }
}

void hsproxy::check_config() {
  // check if there were changes in config files
  if (config->reload()) {
    on_config_updated();
  }
}

void hsproxy::on_config_updated() {
  // Stop HS and MC threads
  stop_pools();

  // Read config
  config->read_configuration();

  // Update logging system
  log_level = config->get_log_level();
  if (log_file) {
    fclose(log_file);
  }
  log_file = config->get_log_file();
  HSLOG_INFO(20, "Set log level to %i\n", log_level);

  // Start MC and HS threads
  init_pools();
}

void hsproxy::stop_hs_pools() {
  for (map<int, hs_pool*>::iterator it = hs_pools.begin(); it!=hs_pools.end(); it++) {
    int cluster_id = it->first;
    hs_pool* pool = it->second;
    HSLOG_INFO(20, "Stopping HS pool for cluster %d\n", cluster_id);
    pool->stop_pool();
    delete pool;
  }
  hs_pools.clear(); // empty map
}

bool hsproxy::push_cluster_request(int cluster_id, hs_request *request) {
  if (hs_pools.find(cluster_id) != hs_pools.end() && hs_pools[cluster_id]->is_ready()) {
    hs_pools[cluster_id]->push(request);
	return true;
  } else {
    HSLOG_ERROR("Trying to send request to unknown or not ready cluster_id=[%d]\n", cluster_id);
	return false;
  }
}

int hsproxy::send_mc_response(int conn_id, char* buffer, int bytes) {
  HSLOG_INFO(90, "MC response to conn_id %d (%d bytes)\n",      
      conn_id, bytes);
  return mcpool->send_response(conn_id, buffer, bytes);
}

/**
 * Return a hash (key/value) with stats information
 */
map<string, string> hsproxy::get_stats() {
  map<string, string> res;
  char value[256];

  // Get PID and process status
  sprintf(value, "%d", getpid());
  res["Pid"] = value;
  getProcessStatusValue(value, "VmRSS");
  res["VmRSS"] = value;
  getProcessStatusValue(value, "Threads");
  res["Threads"] = value;
  res["Version"] = HSPROXY_VERSION;

  // Uptime
  timeval now;
  gettimeofday(&now, NULL);
  int seconds = now.tv_sec - start_time.tv_sec;
  sprintf(value, "%d", seconds);
  res["Uptime"] = value;

  // Get mc_stats
  sprintf(value, "%d", mcpool->get_open_connections());
  res["OpenConn"] = value;
  sprintf(value, "%d", mcpool->get_opened_connections());
  res["OpenedConn"] = value;

  // Get hs_pool stats
  long total_gets = 0;
  long total_misses = 0;
  map<int, hs_pool*>::iterator it;
  for (it = hs_pools.begin(); it!=hs_pools.end(); it++) {
    total_gets += it->second->get_total_gets();
    total_misses += it->second->get_total_misses();
  }
  sprintf(value, "%ld", total_gets);
  res["TotalGets"] = value;
  sprintf(value, "%ld", total_misses);
  res["TotalMisses"] = value;

  return res;
}

int hsproxy::getProcessStatusValue(char *dest, const char* needle) {
  FILE* file = fopen("/proc/self/status", "r");
  int result = -1;
  char line[128];
  while (fgets(line, 128, file) != NULL) {
    if (strncmp(line, needle, strlen(needle)) == 0) {
      char *current = line+strlen(needle);
      while (*current<'0' || *current>'9') current++;
      sprintf(dest, "%d", atoi(current));
      result = strlen(dest);
      break;
    }
  }
  fclose(file);
  return result;
}
