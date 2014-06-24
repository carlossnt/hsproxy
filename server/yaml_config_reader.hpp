/**
 * This reader gets config data from YAML config files
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#ifndef YAML_CONFIG_READER_H
#define YAML_CONFIG_READER_H

#include <arpa/inet.h>
#include <vector>
#include <string>

using namespace std;

struct yaml_config_reader : base_config_reader {
 private:
  vector<string> config_files;
  map<string, struct stat> last_check;
 public:
  yaml_config_reader();
  virtual ~yaml_config_reader();
  // reload config
  virtual bool reload();
  // return a map with table_name as key and cluster_id as value
  virtual map<string, int> read_target_dbmap(string target_name);
  // return metadata of each target_name
  virtual index_meta_t read_hs_index_metadata_file();
  // return general proxy configuration
  virtual hsconfig_t read_hs_config_file();
  // return cluster slave list
  virtual vector<slave_data> read_cluster_slaves(int cluster_id);
};

#endif // YAML_CONFIG_READER_H
