/**
 * Structures to store all data contained in MC requests
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#include <hs_request.hpp>
#include <mc_listener.hpp>
#include <arpa/inet.h>
#include <stdio.h>
#include <debug.hpp>

// HS request

void hs_request::print_request() {
  FILE *dest_file = stderr;
  if (log_file) dest_file = log_file;
  fprintf(dest_file, "HS request -------------------------------\n");
  fprintf(dest_file, "Client: mc_conn_id=[%d] mc_protocol=[%d]\n", 
    mc_conn_id, mc_protocol);
  if (noop) {
    fprintf(dest_file, "NOOP included (opaque=%X)\n", noop_opaque);
  }
  int num_req = gets.size();
  for (int i=0; i<num_req; i++) {
    mckey_data* req = gets[i];
    fprintf(dest_file, "mcKey=[%s] targetName=[%s] version=[%d]\n", 
        req->mc_key.c_str(), req->target_name.c_str(), req->version);
    fprintf(dest_file, "tableName=[%s] indexId=[%d] binaryReq=[%d]\n", 
        req->table_name.c_str(), req->index_id, req->req_type);
    string vlist;
    int num_values = req->pk_values.size();
    for (int i=0; i<num_values; i++) {
      vlist = vlist + " " + req->pk_values[i];
    }
    fprintf(dest_file, "Values:%s\n", vlist.c_str());
  }
  fprintf(dest_file, "-------------------------------\n");
  fflush(dest_file);
}

hs_request::~hs_request() {
  int num_req = gets.size();
  for (int i=0; i<num_req; i++) {
    delete(gets[i]);
  }
}

