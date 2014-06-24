/**
 * Proxy instance (pointers to all threads)
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#ifndef HSPROXY_H
#define HSPROXY_H

#include <hs_request.hpp>
#include <config_manager.hpp>
#include <mc_listener.hpp>
#include <hs_pool.hpp>
#include <debug.hpp>
#include <vector>
#include <map>

#define HSPROXY_VERSION "1.0.0"

using namespace std;

struct hsproxy {
 private:
  static hsproxy* instance;
  mc_pool* mcpool;
  map<int, hs_pool*> hs_pools;
  config_manager* config;
  timeval start_time;
 public:
  static hsproxy* get_instance();
  void init_proxy(char* config_path, int mc_port_override, string db_name_override, string sock_file_override);
  void shutdown();
  void check_config();
  bool push_cluster_request(int cluster_id, hs_request *request);
  void on_config_updated();
  int send_mc_response(int conn_id, char* buffer, int bytes);
  map<string, string> get_stats();
 private:
  hsproxy() : mcpool(NULL), config(NULL) {}
  void init_pools();
  void stop_pools();
  void init_hs_pools();
  void stop_hs_pools();
  int getProcessStatusValue(char* dest, const char* needle);
};

#endif // HSPROXY_H
