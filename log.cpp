/*
 * log.cpp
 *
 *  Created on: Aug 20, 2015
 *      Author: blevinson
 */

#include <stdarg.h>
#include <errno.h>
#include <cstdio>
#include <cstring>

#include "log.h"

FILE *logfile = stderr ;
debugLevel logLevel = info ;

int log(debugLevel l, const char *format, ...) {
	if( l < logLevel ) {
		return 0 ;
	}

	va_list arg ;
	int done;

	va_start(arg,format) ;
	done = vfprintf(logfile,format,arg) ;
	va_end(arg) ;

	fflush(logfile) ; // make sure it gets logged right away.

	return( done ) ;
}

void logPerror(const char *str) {
	log(error, str ) ;
	log(error, ": ") ;
	log(error, strerror(errno)) ;
	log(error, "\n") ;
}


