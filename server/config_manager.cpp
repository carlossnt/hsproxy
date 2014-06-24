/**
 * config_manager thread:
 * - holds proxy configuration in memory
 * - every second checks if config files were updated
 *
 * config_manager uses config_reader to get config values
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#include <hsproxy.hpp>
#include <arpa/inet.h>
#include <map>
#include <yaml_config_reader.hpp>
#include <errno.h>
#include <stdlib.h>

/**
 * Returns specific config_reader we want to use. 
 * If you want to use a different/customized config reader
 * overwrite this method.
 */
base_config_reader* config_manager::get_config_reader() {
  if (!reader) {
    reader = new yaml_config_reader();
    if (config_path) {
      reader->set_config_path(string(config_path));
    }
  }
  return reader;
}

config_manager* config_manager::instance = NULL;

config_manager::config_manager() {
  current_config = NULL;
  reader = NULL;
  mc_port_override = -1;
  db_name_override = "";
  sock_file_override = "";
}

config_manager* config_manager::get_instance() {
  if (!instance) {
    instance = new config_manager();
  }
  return instance;
}

config_manager::~config_manager() {
  if (current_config) delete(current_config);
  if (reader) delete reader;
}

/**
 * Return database name (where the indexes we are going to query are).
 * @return string DB name.
 */
string config_manager::get_db_name() {
  if (db_name_override.length()>0) {
    return db_name_override;
  } else {
    return current_config->hsconfig.db_name;
  }
}

/**
 * Return unix sock file (used when MC clients are hosted in the same machine).
 * @return string unix sock file.
 */
string config_manager::get_mc_sock_file() {
  if (sock_file_override.length()>0) {
    return sock_file_override;
  } else {
    return current_config->hsconfig.mc_sock_file;
  }
}

/**
 * Return log file or NULL if not defined.
 * @return FILE* log file.
 */
FILE* config_manager::get_log_file() {
  FILE* ret = NULL;
  const char* filename = current_config->hsconfig.log_file.c_str();
  if (strlen(filename)>0) {
    if ((ret=fopen(filename, "a+"))==NULL) {
      HSLOG_ERROR("Error opening log file (%s)\n", strerror(errno));
    }
  }
  return ret;
}

/**
 * Return log level (the higher the more verbose).
 * @return int log level.
 */
int config_manager::get_log_level() {
  return current_config->hsconfig.log_level;
}

/**
 * Return MC port. MC clients will connect to this TCP port.
 * @return int MC port.
 */
int config_manager::get_mc_port() {
  if (mc_port_override>0) {
    return mc_port_override;
  } else if (sock_file_override.length()>0) {
    return -1;
  } else {
    return current_config->hsconfig.mc_port;
  }
}

/**
 * Return HS port. It's the TCP port enabled in HS servers for readonly operations.
 * @return int HS port.
 */
int config_manager::get_hs_port() {
  return current_config->hsconfig.hs_port;
}

/**
 * Return cluster slave list.
 * @param int cluster_id cluster ID.
 * @return vector slave list.
 */
vector<slave_data> config_manager::get_cluster_slaves(int cluster_id) {
  vector<slave_data> slaves;
  map<int, cluster_map_entry> cluster_map = current_config->cluster_map;
  if (cluster_map.find(cluster_id) != cluster_map.end()) {
    slaves = cluster_map[cluster_id].slaves_list;
  }
  return slaves;
}

/**
 * Get cluster index list.
 * @param int cluster_id cluster id.
 * @return vector index metadata list.
 */
vector<index_data>& config_manager::get_cluster_indexes(int cluster_id) {
  return current_config->cluster_map[cluster_id].index_list;
}

/**
 * Get HS pool size (number of HS connections per DB cluster).
 * There will be a thread per HS connection.
 * @return int number ot connections/threads.
 */
int config_manager::get_hs_pool_size() {
  return current_config->hsconfig.hs_pool_size;
}

/**
 * Get number of MC parsers (threads for MC request parsing).
 * @return int num parsers.
 */
int config_manager::get_num_parsers() {
  return current_config->hsconfig.num_parsers;
}

/**
 * Return maximum number of incoming MC connections.
 * It's the maximum number of concurrent MC clients.
 * @return int max MC connections.
 */
int config_manager::get_max_connections() {
  return current_config->hsconfig.max_connections;
}

/**
 * Return a random cluster slave considering slaves weight.
 * @return in_addr slave IP.
 */
in_addr config_manager::pick_slave(int cluster_id) {
  vector<slave_data> slaves = get_cluster_slaves(cluster_id);
  int total_weight = 0;
  for (size_t i=0; i<slaves.size(); i++) {
    total_weight += slaves[i].weight;
  }
  if (total_weight == 0) {
    // There's no slaves in cluster_id
    in_addr null_addr = {0};
    return null_addr;
  }
  int random_weight = rand() % total_weight +1;

  int weight = 0;
  size_t pos = 0;
  for (pos=0; pos<slaves.size(); pos++) {
    weight += slaves[pos].weight;
    if (weight >= random_weight) break;
  }
  return slaves[pos].address;
}

/**
 * Get cluster_id where a table is hosted.
 * @param string table table name.
 * @return int cluster ID.
 */
int config_manager::get_table_cluster(string& table) {
  return current_config->table_map[table].cluster_id;
}

/**
 * Get index ID of a versioned table.
 * @param string table name.
 * @param int version.
 * @return int index ID.
 */
int config_manager::get_hs_index_id(string& table, int version) {
  return current_config->table_map[table].index_ids[version];
}

/**
 * Get index metadata of a versioned table.
 * @param string table name.
 * @param int version.
 * @return storage_fields* index metadata.
 */
const storage_fields* config_manager::get_target_metadata(string& target_name, int version) {
  return &(current_config->index_meta[target_name][version]);
}

/**
 * Return true if using getq/getkq requests.
 */
bool config_manager::get_mc_quiet_mode() {
  return current_config->hsconfig.mc_quiet_mode;
}

/**
 * Return true if miss ack is enabled (we send ASCII ACK char for misses)
 */
bool config_manager::get_miss_ack() {
  return current_config->hsconfig.miss_ack;
}

/**
 * Get cluster IDs list.
 * @return vector cluster_ids list.
 */
vector<int> config_manager::get_clusters() {
  vector<int> clusters;
  map<int, cluster_map_entry>::iterator it;
  map<int, cluster_map_entry> cluster_map = current_config->cluster_map;
  for (it=cluster_map.begin(); it!=cluster_map.end(); it++) {
    clusters.push_back(it->first);
  }
  return clusters;
}

void config_manager::init(char* path, int mc_port, string db_name, string sock_file) {
  config_path = path;
  mc_port_override = mc_port;
  db_name_override = db_name;
  sock_file_override = sock_file;
}

bool config_manager::reload() {
  return get_config_reader()->reload();
}

/**
 * Reload configuration
 */
void config_manager::read_configuration() {
  delete current_config;
  current_config = get_new_config();
}

/**
 * Return config data with config files contents.
 * @return config_data* configuration data.
 */
config_data *config_manager::get_new_config() {
  map<int, int> index_id_counters;
  config_data *config = new config_data();
  config->hsconfig = get_config_reader()->read_hs_config_file();
  // foreach target name
  config->index_meta = get_config_reader()->read_hs_index_metadata_file();
  HSLOG_INFO(20, "Generate table and cluster indexes\n");

  // foreach target_name
  for (index_meta_t::iterator it=config->index_meta.begin(); 
          it != config->index_meta.end(); it++) {
    string target_name = it->first;
    HSLOG_INFO(50, " target_name=[%s]\n", target_name.c_str());
    version_fields_t version_fields = it->second;
    map<string, int> target_dbmap = get_config_reader()->read_target_dbmap(target_name);
    // foreach clusterId in target_name config
    for (map<string, int>::iterator t_it = target_dbmap.begin();
            t_it != target_dbmap.end(); t_it++) {
      string table_name = t_it->first;
      int cluster_id = t_it->second;
      HSLOG_INFO(50, "  cluster_id=[%d] table_name=[%s]\n", cluster_id, table_name.c_str());
      // if cluster_map[cluster_id] does not exist
      map<int, cluster_map_entry>::iterator cluster_it = config->cluster_map.find(cluster_id);
      if (cluster_it == config->cluster_map.end()) {
        HSLOG_INFO(50, "  cluster_map[%d] add slaves\n", cluster_id);
        cluster_map_entry cme;
        cme.slaves_list = get_config_reader()->read_cluster_slaves(cluster_id);
        config->cluster_map[cluster_id] = cme;
        index_id_counters[cluster_id] = 0;
      }
      table_map_entry table_data;
      table_data.cluster_id = cluster_id;
      // foreach version
      for (version_fields_t::iterator v_it = version_fields.begin();
              v_it != version_fields.end(); v_it++) {
        int version = v_it->first;
        table_data.index_ids[version] = index_id_counters[cluster_id];
        HSLOG_INFO(50, " table_map[%s] version=[%d] cluster=[%d] indexId=[%d]\n",
              table_name.c_str(), version, cluster_id, (index_id_counters[cluster_id]));
   
        index_data index = {index_id_counters[cluster_id], table_name, target_name, version};
        config->cluster_map[cluster_id].index_list.push_back(index);
        index_id_counters[cluster_id]++;
      }
      config->table_map[table_name] = table_data;
    }
  }
  return config;
}
