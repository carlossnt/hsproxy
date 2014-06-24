/**
 * This reader gets config data from YAML config files
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#include <hsproxy.hpp>
#include <yaml_config_reader.hpp>
#include <yaml-cpp/yaml.h>
#include <arpa/inet.h>
#include <map>
#include <errno.h>

/**
 * Involved files:
 *
 * hsMeta.json
 * -----------
 *   "user_basic":{
 *     "1":{
 *       "key_fields":["user_id"],
 *       "value_fields":["name","surname","location_id"],
 *       "num_part":100,
 *       "index":"PRIMARY"
 *     }
 *     "2":{
 *       "key_fields":["user_id"],
 *       "value_fields":["name","surname","location_id","deactivated"],
 *       "num_part":100,
 *       "index":"PRIMARY"
 *     }
 *   }
 *
 * hsConfig.json
 * -------------
 *  "mc_port":9887,
 *  "hs_port":9998,
 *  "db_name":"testdb",
 *  "hs_pool_size":6,
 *  "num_parsers":2,
 *  "log_level":20,
 *
 * databaseServers.json
 * --------------------
 *  "34000":{
 *    "10.151.2.50":100,
 *    "10.151.2.51":100,
 *  },
 *  "2001": {
 *    "10.151.2.52":100,
 *  },
 *
 * databaseConfig.json
 * -------------------
 *  "user_basic":[
 *    [34000,0,99],
 *  ],
 *  "last_time":[
 *    [2001:0,99],
 *  ],  
 */

#define DEFAULT_CONFIG_PATH "/etc/hsproxy/"
#define DBSERVERS_JSON "databaseServers.json"
#define DBCONFIG_JSON "databaseConfig.json"
#define HSCONFIG_JSON "hsConfig.json"
#define HSMETA_JSON "hsMeta.json"
#include <sys/stat.h>

yaml_config_reader::yaml_config_reader() {
  config_path = DEFAULT_CONFIG_PATH;
  config_files.push_back(config_path + HSMETA_JSON);
  config_files.push_back(config_path + HSCONFIG_JSON);
  config_files.push_back(config_path + DBCONFIG_JSON);
  config_files.push_back(config_path + DBSERVERS_JSON);
};

yaml_config_reader::~yaml_config_reader() {};

/**
 * Stat all config files and check them against last reload timestamp
 * @return bool true if the config files have been changed since the
 * last reload.
 */
bool yaml_config_reader::reload() {
  struct stat fstat;
  bool reload = false;

  for (size_t i=0; i < config_files.size(); ++i) {
    string & fname = config_files[i];

    if (stat(fname.c_str(), &fstat) == -1) {
      HSLOG_ERROR("Error checking config file (errno=%d)", errno);
      HSLOG_ERROR(" using config_path %s\n", fname.c_str());
    }

    if (last_check.count(fname) == 0) {
      last_check[fname] = fstat;
      reload = true;
    }

    if (last_check[fname].st_mtime != fstat.st_mtime
        || last_check[fname].st_mtim.tv_nsec != fstat.st_mtim.tv_nsec) {
      last_check[fname] = fstat;
      reload = true;
    }
  }

  return reload;
}

/**
 * Parse hsMeta.json file containing index definitions.
 * @return index_meta_t index metadata definitions.
 */
index_meta_t yaml_config_reader::read_hs_index_metadata_file() {
  string file_name = config_path + '/' + string(HSMETA_JSON);
  YAML::Node config = YAML::LoadFile(file_name.c_str());
  index_meta_t index_meta;

  for (YAML::const_iterator it=config.begin();it!=config.end();++it) {
    string target_name = it->first.as<string>();
    YAML::Node meta = it->second;
    map<int, storage_fields> target_versions;
    for (YAML::const_iterator vit=meta.begin();vit!=meta.end();++vit) {
      int version = vit->first.as<int>();
      YAML::Node data = vit->second;
      storage_fields versioned_fields;
      // key fields
      YAML::Node fields = data["key_fields"];
      for (size_t i=0; i<fields.size(); i++) {
        string field = fields[i].as<string>();
        versioned_fields.key_fields.push_back(field);
      }
      // value fields
      fields = data["value_fields"];
      for (size_t i=0; i<fields.size(); i++) {
        string field = fields[i].as<string>();
        versioned_fields.value_fields.push_back(field);
      }
      versioned_fields.num_part = data["num_part"].as<int>();
      versioned_fields.index = data["index"].as<string>();
      target_versions[version] = versioned_fields;
    }
    if (target_versions.size() > 0) {
      index_meta[target_name] = target_versions;
    }
  }
  return index_meta;
}

/**
 * Parse hsConfig.json file.
 * @return hsconfig_t HS configuration.
 */
hsconfig_t yaml_config_reader::read_hs_config_file() {
  string file_name = config_path + '/' + string(HSCONFIG_JSON);
  YAML::Node config = YAML::LoadFile(file_name.c_str());

  // Set default values
  hsconfig_t hsconfig;
  hsconfig.mc_port = 11211;
  hsconfig.hs_port = 9998;
  hsconfig.hs_pool_size = 4;
  hsconfig.num_parsers = 1;
  hsconfig.max_connections = 256;
  hsconfig.db_name = string("test");
  hsconfig.mc_sock_file = string("");
  hsconfig.log_level = 20;
  hsconfig.log_file = string(""); // stderr (hsproxy) / syslog (hsproxyd)
  hsconfig.mc_quiet_mode = false;
  hsconfig.miss_ack = true;

  // set mc_port or mc unix sock
  if (config["mc_port"]) {
    hsconfig.mc_port = config["mc_port"].as<int>();
  } else {
    if (config["mc_sock_file"]) {
      hsconfig.mc_port = -1;
      hsconfig.mc_sock_file = config["mc_sock_file"].as<string>();
    }
  }

  // other params
  if (config["hs_port"])
    hsconfig.hs_port = config["hs_port"].as<int>();
  if(config["hs_pool_size"])
    hsconfig.hs_pool_size = config["hs_pool_size"].as<int>();
  if(config["num_parsers"])
    hsconfig.num_parsers = config["num_parsers"].as<int>();
  if(config["max_connections"])
    hsconfig.max_connections = config["max_connections"].as<int>();
  if (config["db_name"])
    hsconfig.db_name = config["db_name"].as<string>();
  if (config["log_level"])
    hsconfig.log_level = config["log_level"].as<int>();
  if (config["log_file"])
    hsconfig.log_file = config["log_file"].as<string>();
  if (config["mc_quiet_mode"])
    hsconfig.mc_quiet_mode = !(config["mc_quiet_mode"].as<int>() == 0);

  return hsconfig;
}

/**
 * Generate table map usefull to know which cluster contains each partitioned table.
 * @return map<string, int> table map. E.g. array { 'user_basic0' => 34000, ... 'user_basic99' => 34000 }
 */
map<string, int> yaml_config_reader::read_target_dbmap(string target_name) {
  string file_name = config_path + '/' + string(DBCONFIG_JSON);
  YAML::Node config = YAML::LoadFile(file_name.c_str());
  YAML::Node target_data = config[target_name.c_str()];
  char numstr[12];
  
  map<string, int> target_dbmap;
  for (size_t i=0; i<target_data.size(); i++) {
    int cluster_id = target_data[i][0].as<int>();
    int start_part = target_data[i][1].as<int>();
    int last_part  = target_data[i][2].as<int>();
    for (int i=start_part; i<=last_part; i++) {
      sprintf(numstr, "%d", i);
      string table_name = target_name + numstr;
      target_dbmap[table_name] = cluster_id;
    }
  }
  return target_dbmap;
}

/**
 * Get slaves list for a giver cluster ID.
 * @param int cluster_id cluster ID.
 * @return vector<slave_data> slaves list. E.g. array { (10.151.2.50, 100), (10.151.2.51, 100) }
 */
vector<slave_data> yaml_config_reader::read_cluster_slaves(int cluster_id) {
  string file_name = config_path + '/' + string(DBSERVERS_JSON);
  YAML::Node config = YAML::LoadFile(file_name.c_str());
  vector<slave_data> slaves;

  char cluster[12];
  sprintf(cluster, "%i", cluster_id);
  YAML::Node hosts = config[cluster];

  for (YAML::const_iterator it=hosts.begin();it!=hosts.end();++it) {
    string host = it->first.as<string>();
    int weight = it->second.as<int>();
    if (weight>0) {
      hostent *he = gethostbyname(host.c_str());
      slave_data slave = {*((in_addr*)he->h_addr_list[0]), weight};
      slaves.push_back(slave);
    }
  }
  return slaves;
}

