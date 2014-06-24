/**
 * config_manager thread:
 * - holds proxy configuration in memory
 * - every second checks if config files were updated
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

/**
 * Definition of structures used to store proxy configuration
 * - db_name: database name where tables are located
 * - cluster_map: hash map with cluster_id as key. We store slave_list and 
 *    index_list per cluster
 * - table_map: hash map with table_name as key. We store cluster_id and 
 *    index_ids per table (we can have as many index_ids as mckey versions)
 * - index_meta: contains index metadata (1 entry per target and mc version)
 *    metadata includes key_fields, field_list, partitioning field...
 *    Needed when opening DB indexes
 * - hsconfig: other proxy params (UDP port, TCP port, connections per slave)
 */

#ifndef CONFIG_MANAGER_H
#define CONFIG_MANAGER_H

#include <arpa/inet.h>
#include <pthread.h>
#include <map>
#include <vector>
#include <string>

using namespace std;

/**
 * Example index_meta
 * 'photos' => {
 *   2 => {
 *     key_fields => {photo_id, uploader_id}
 *     value_fields => {time, url}
 *     num_part => 1000
 *     index => 'PRIMARY'
 *   }
 * 'photos' => {
 *   3 => {
 *     key_fields => {photo_id, uploader_id}
 *     value_fields => {time, url, flags}
 *     num_part => 1000
 *     index => 'PRIMARY'
 *   }
 */
struct storage_fields {
  vector<string> key_fields;
  vector<string> value_fields;
  int num_part;
  string index;
};

typedef map<int, storage_fields> version_fields_t;
typedef map<string, version_fields_t> index_meta_t;

/**
 * Example cluster_map
 * 34000 => {
 *   slaves_list => {d111=>100, d222=>100}
 *   index_list => {
 *     {index_id=>0, target_name=>user_basic0, target_name=>user_basic, version=>4}
 *     ...
 *     {index_id=>199, target_name=>privacy99, target_name=>privacy, version=>2}
 * }
 * 2001 => {
 *   slaves_list => {d555, d666}
 *   index_list => {
 *     {index_id=>0, target_name=>last_times0, target_name=>last_times, version=>3}
 *     ...
 *     {index_id=>99, target_name=>last_times99, target_name=>last_times, version=>3}
 * }
 */
struct index_data {
  int index_id;
  string table_name;
  string target_name;
  int version;
};

struct slave_data {
  in_addr address;
  int weight;
};

struct cluster_map_entry {
  vector<slave_data> slaves_list;
  vector<index_data> index_list;
};

/**
 * Example table_map
 * user_basic22 => {
 *   cluster_id => 34000
 *   index_ids => {4=>22, 5=>122}
 * }
 * user_basic23 => {
 *   cluster_id => 34000
 *   index_ids => {4=>23, 5=>123}
 * }
 * ...
 */
struct table_map_entry {
  int cluster_id;
  map<int, int> index_ids;
};

struct hsconfig_t {
  int hs_port;
  int mc_port;
  string db_name;
  string mc_sock_file;
  int hs_pool_size;
  int num_parsers;
  int max_connections;
  bool mc_binary_protocol;
  bool mc_quiet_mode;
  int log_level;
  string log_file;
  bool miss_ack;
};

struct config_data {
  string db_name;
  map<int, cluster_map_entry> cluster_map;
  map<string, table_map_entry> table_map;
  index_meta_t index_meta;
  hsconfig_t hsconfig;
}; 

/**
 * Base config reader definition.
 * If you want your own config_reader write your own implementation 
 *  of these methods and modify config_manager::get_config_reader
 */
struct base_config_reader {
 string config_path;
 public:
  // reload config, returns true if a reload is needed
  virtual bool reload()=0;
  // return a map with table_name as key and cluster_id as value
  virtual map<string, int> read_target_dbmap(string target_name)=0;
  // return metadata of each target_name
  virtual index_meta_t read_hs_index_metadata_file()=0;
  // return general proxy configuration
  virtual hsconfig_t read_hs_config_file()=0;
  // return cluster slave list
  virtual vector<slave_data> read_cluster_slaves(int cluster_id)=0;
  // set config path
  void set_config_path(string path) {config_path = path;}
};

/**
 * config_manager thread:
 * - holds proxy configuration in memory
 * - every second checks if config files were updated
 *
 */
struct config_manager {
 private:
  static config_manager* instance;
  config_data* current_config;
  base_config_reader* reader;
  char* config_path;
  int mc_port_override;
  string db_name_override;
  string sock_file_override;
 public:
  static config_manager* get_instance();
  ~config_manager();
  int get_version();
  bool reload();
  vector<int> get_clusters();
  vector<slave_data> get_cluster_slaves(int cluster_id);
  in_addr pick_slave(int cluster_id);
  vector<index_data>& get_cluster_indexes(int cluster_id);
  int get_hs_pool_size();
  int get_num_parsers();
  int get_max_connections();
  int get_table_cluster(string& table);
  int get_hs_index_id(string& table, int version);
  void read_configuration();
  const storage_fields* get_target_metadata(string& target_name, int version);
  string get_db_name();
  string get_mc_sock_file();
  FILE* get_log_file();
  int get_log_level();
  bool get_mc_quiet_mode();
  bool get_miss_ack();
  int get_mc_port();
  int get_hs_port();
  void init(char* path, int mc_port, string db_name, string sock_file);
 private:
  config_manager();
  config_data* get_new_config();
  base_config_reader* get_config_reader();
};

#endif // CONFIG_MANAGER_H
