/**
 * Debug routines
 *
 * LICENSE: This file can only be stored on servers belonging to
 * Tuenti Technologies S.L.
 *
 * @author Carlos Santos <csantosl@tuenti.com>
 * @copyright  2013, (c) Tuenti Technologies S.L.
 */


#ifndef HS_DEBUG_H
#define HS_DEBUG_H

#define PROFILE(...) // if(log_file) {print_utime(log_file); do{fprintf(log_file,  __VA_ARGS__ ); fflush(log_file);} while(0);} else {print_utime(stderr); do{fprintf(stderr, __VA_ARGS__ );} while(0);}

#define DEBUG(level, x)  if(log_level>=level) x;

#define HSLOG_ERROR(...) do{ if(!log_file) daemon_log(LOG_ERR, __VA_ARGS__); else fprintf(log_file, __VA_ARGS__); fflush(log_file);} while(0);
#define HSLOG_INFO(level, ...) do {if(log_level>=level) {if (log_file) {fprintf(log_file, __VA_ARGS__ ); fflush(log_file);} \
  else {fprintf(stderr, __VA_ARGS__ );}}} while(0);

#include <stdio.h>
#include <libdaemon/dlog.h>
#include <sys/time.h>


using namespace std;

extern int log_level;
extern FILE* log_file;

void print_refstring(FILE* dest, char* start, char *finish);
void print_mc_binary_request(FILE* dest, char* req);
void print_mc_binary_response(FILE* dest, char* res);

inline void print_utime(FILE* dest) {
  timeval t1;
  gettimeofday(&t1, NULL);
  fprintf(dest, "[%ld.%06ld] ", t1.tv_sec, t1.tv_usec);
}


#endif // HS_DEBUG_H
