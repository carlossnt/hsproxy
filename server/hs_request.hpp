/**
 * Structures to store all data contained in MC requests
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#ifndef HS_REQUEST_H
#define HS_REQUEST_H

#include <netinet/in.h>
#include <string>
#include <vector>

#define MC_PROTOCOL_BINARY 0
#define MC_PROTOCOL_ASCII 1

using namespace std;

struct mckey_data {
  string mc_key;
  string target_name;
  int version;
  int cluster_id;
  string table_name;
  vector<string> pk_values;
  int index_id;
  int req_type;
  uint32_t opaque;
};

struct hs_request {
 public:
  vector<mckey_data*> gets;
  int mc_conn_id;
  int mc_protocol;
  bool noop;
  uint32_t noop_opaque;
  bool cas;
 public: // methods
  void print_request();
  hs_request(int c, int p): mc_conn_id(c), mc_protocol(p), noop(false), cas(false) {}
  ~hs_request();
};

#endif // HS_REQUEST_H
