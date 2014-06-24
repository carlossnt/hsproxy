/**
 * Debug routines
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */

#include <debug.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>

#include <protocol_binary.h>


int log_level = 20;
FILE* log_file = NULL;


/**
 * Print string between start and end positions into log_file
 * @param FILE* dest log_file, we use stderr if NULL
 * @param char* start string begin
 * @para, char* finish string end
 */
void print_refstring(FILE* dest, char* start, char *finish) {
  FILE* dest_file = stderr;
  if (dest) dest_file = dest;
  char *dup = strndup(start, finish-start);
  fprintf(dest_file, "%s\n", dup);
  fflush(dest_file);
  free(dup);
}

/**
 * Write MC binary request contents into lof_file
 * @param FILE* dest log_file, we use stderr if NULL
 * @param char* req pointer to binary request in buffer
 */
void print_mc_binary_request(FILE* dest, char* req) {
  FILE* dest_file = stderr;
  if (dest) dest_file = dest;
  fprintf(dest_file, "---------- MC binary request ---------\n");
  protocol_binary_request_header* header = (protocol_binary_request_header*) req;
  uint16_t keylen = ntohs(header->request.keylen);
  uint32_t bodylen = ntohl(header->request.bodylen);
  uint8_t extlen = header->request.extlen;

  fprintf(dest_file, "magic %X\n", header->request.magic);
  fprintf(dest_file, "opcode: %X\n", header->request.opcode);
  fprintf(dest_file, "keylen: %d\n", keylen);
  fprintf(dest_file, "extlen: %d\n", extlen);
  fprintf(dest_file, "bodylen: %d\n", bodylen);
  fprintf(dest_file, "opaque: %X\n", header->request.opaque);
  fprintf(dest_file, "cas: %lu\n", header->request.cas);

  int headerlen = sizeof(protocol_binary_request_header);;
  char *key_ptr = req+headerlen+extlen;
  char *dup = strndup(key_ptr, keylen);
  fprintf(dest_file, "key: %s\n", dup);
  fflush(dest_file);
  free(dup);
}

/**
 * Write MC binary response contents into lof_file
 * @param FILE* dest log_file, we use stderr if NULL
 * @param char* req pointer to binary response in buffer
 */
void print_mc_binary_response(FILE* dest, char* res) {
  FILE* dest_file = stderr;
  if (dest) dest_file = dest;
  fprintf(dest_file, "---------- MC binary response ---------\n");
  protocol_binary_response_header* header = (protocol_binary_response_header*) res;
  uint16_t keylen = ntohs(header->response.keylen);
  uint32_t bodylen = ntohl(header->response.bodylen);
  uint8_t extlen = header->response.extlen;

  fprintf(dest_file, "magic: %X\n", header->response.magic);
  fprintf(dest_file, "opcode: %X\n", header->response.opcode);
  fprintf(dest_file, "keylen: %d\n", keylen);
  fprintf(dest_file, "extlen: %d\n", header->response.extlen);
  fprintf(dest_file, "datatype: %d\n", header->response.datatype);
  fprintf(dest_file, "status: %X\n", (uint16_t) ntohs(header->response.status));
  fprintf(dest_file, "bodylen: %d\n", bodylen);
  fprintf(dest_file, "opaque: %X\n", header->response.opaque);
  fprintf(dest_file, "cas: %lu\n", header->response.cas);

  int headerlen = sizeof(protocol_binary_response_header);;
  char *key_ptr = res+headerlen+extlen;
  char *dup = strndup(key_ptr, keylen);
  fprintf(dest_file, "key: %s\n", dup);
  free(dup);

  char *body_ptr = key_ptr+keylen;
  dup = strndup(body_ptr, bodylen-extlen-keylen);
  fprintf(dest_file, "valuelen: %d\n", bodylen-extlen-keylen);
  fprintf(dest_file, "body: %s\n", dup);
  fflush(dest_file);
  free(dup);
}

