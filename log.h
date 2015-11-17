/*
 * log.h
 *
 *  Created on: Aug 20, 2015
 *      Author: blevinson
 */

#ifndef LOG_H_
#define LOG_H_


enum debugLevel {
	crazyDebug,
	debug,
	verbose,
	info,
	error
} ;

int log(debugLevel l, const char *format, ...) ;
void logPerror(const char *) ;

extern FILE *logfile;
extern debugLevel logLevel;

#endif /* LOG_H_ */
