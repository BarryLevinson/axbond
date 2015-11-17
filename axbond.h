/*
 * axbond.h
 *
 *  Created on: Aug 19, 2015
 *      Author: blevinson
 */

#ifndef AXBOND_H_
#define AXBOND_H_

class Header {
public:
	unsigned short len ;
	char msgType ;
	unsigned short seq ;
	unsigned short seqAck ;
};


class connectionDetail {
public:
	char *sourceIp ;
	char *destHost ;
	unsigned short destPort ;
	int sock ;
	bool connected ;
	bool isClient ;
	unsigned long lastRead ;
	unsigned long lastWrite ;
	bool gotHeader;
	Header header ;
};

int openOutgoingConnection(connectionDetail * ) ;


class ReconnectTimer {
public:
	unsigned long timeout ;
	int connectionIndex ;

	void onTimeout(connectionDetail *, struct pollfd *, int) ;
};

// Message types:
#define MSG_TYPE_DATA   'S'
#define MSG_TYPE_HBEAT  'H'
#define MSG_TYPE_LOGOUT 'O'
#define MSG_TYPE_GAPFIL 'G'





#endif /* AXBOND_H_ */
