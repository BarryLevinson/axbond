//============================================================================
// Name        : axbond.cpp
// Author      : Barry Levinson
// Version     : .1
// Copyright   : Copyright 2015 Barry Levinson
// Description : axbond tunnels stdin/stdout over multiple TCP connections.
//============================================================================

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <list>

#include "axbond.h"
#include "log.h"

using namespace std;

#define RBUF_SIZE  (1024*1024*10) /* NOTE: needs to be large enough for re-requests */
#define MAX_BUF_SIZE 32768   /* max chunk of data to send.  Must be less than Max Unsigned Short Bytes */
#define IN_FDI 0
#define OUT_FDI 1
#define MAX_CONNCECTIONS 8

#define POLL_TIMEOUT 100 /* milliseconds - make sure < HEARTBEAT_TIMEOUT/2 to avoid a race */

#define RECONNECT_TIMEOUT 5000 /* milliseconds */

#define HEARTBEAT_TIMEOUT 250 /* milliseconds */

#define HEARTBEAT_SEND_TIME 100 /* milliseconds - should be < HEARTBEAT_TIMEOUT - POLL_TIMEOUT plus some extra room for races */

#define SEQ_GAP_TIMEOUT 250 /* milliseconds */

#define HEADER_SIZE (sizeof(short) + sizeof(char) + sizeof(short) + sizeof(short))

void ReconnectTimer::onTimeout(connectionDetail *cp, struct pollfd *fdp, int curTime) {
	int sock = openOutgoingConnection(cp) ;
	cp->connected = true ;
	cp->lastRead = curTime ;
	cp->lastWrite = (long)-1 ; //don't send heartbeat until first "real" write
	cp->gotHeader = false ;

	cp->sock = sock ;

	fdp->fd = sock ;
	fdp->events = POLLIN ;
}


class LenBuf {
public:
	char buf[MAX_BUF_SIZE] ;
	unsigned short pos ;
	unsigned short len ;

	LenBuf(void) ;
};

LenBuf::LenBuf() {
	len=0 ;
	pos=0 ;
}


class BufNode {

public:
	LenBuf buf ;
	Header header;
};


list <BufNode *> fromPipeNodeBufs ;
list <BufNode *> toPipeNodeBufs ;
list <BufNode *> retransNodeBufs ;

int numfds = 2 ;
struct pollfd fds[MAX_CONNCECTIONS+2] ;

list <ReconnectTimer> timerList ;

int roundRobinLast=0 ;

int numConnections = 0 ;
int firstPipeConnection ;

unsigned long lastSeqTime = 0 ;

connectionDetail connections[MAX_CONNCECTIONS] ;

bool shutdownInProgress = false ;
bool sentLogouts = false ;
bool mayNeedToSendGapfill = false ;

// TODO: If/when the server supports multiple separate client connection sets, these will
//       need to be part of some server structure:
unsigned short expectedIncommingSeq = 0 ;
unsigned short nextOutgointSeq = 0 ;

int openOutgoingConnection(connectionDetail *connection ) {
	log(verbose, "Attempting to connect from %s to %s:%d\n", connection->sourceIp, connection->destHost, connection->destPort) ;

	int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if( sock < 0 ) {
		logPerror("ERROR: could not open socket") ;
		exit(1) ;
	}

	// make the connect non-blocking
	// connect may fail with errno set to EINPROGRESS - it will
	// continue to retry.  If it ultimately fails our logic should
	// automatically retry.
	fcntl(sock, F_SETFL, O_NONBLOCK ) ;

	// Bind to the source address:
	struct sockaddr_in bind_addr ;
	bind_addr.sin_port = htons(0) ; // any port
	bind_addr.sin_family = AF_INET;

	if( !inet_aton(connection->sourceIp,&bind_addr.sin_addr) ) {
		fprintf(stderr, "Invalid source address : %s\n", connection->sourceIp) ;
		exit(1) ;
	}

	if( bind(sock, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) < 0 ) {
		logPerror("bind failed.") ;
		exit(1) ;
	}

	struct hostent *proxy = gethostbyname(connection->destHost) ;
	if( proxy == NULL ) {
		fprintf(stderr, "unknown host %s\n", connection->destHost);
		exit(1) ;
	}
	struct sockaddr_in pip ;

	pip.sin_family = AF_INET ;

	// copy in address:
	bcopy((char *)proxy->h_addr, (char *)&pip.sin_addr.s_addr,proxy->h_length) ;

	// port:
	pip.sin_port = htons(connection->destPort) ;

	if( (connect(sock, (struct sockaddr *)&pip, sizeof(pip)) < 0) && (errno != EINPROGRESS) ) {
		logPerror("ERROR: connect failed.") ;
		exit(1) ;
	}

	log(debug, "Connected from %s to %s:%d\n", connection->sourceIp, connection->destHost, connection->destPort) ;

	return( sock ) ;
}



unsigned int howMuchCouldWeWrite(int sock) {
	// check available space on the socket
	unsigned int numbuffed ;
	if( ioctl(sock, TIOCOUTQ, &numbuffed) < 0 ) {
		fprintf(stderr, "writeToPipe: failed to determine TCP write space left.") ;
		exit(1) ;
	}

	unsigned int sendBufSize ;
	socklen_t optlen = sizeof(sendBufSize);
	getsockopt(sock, SOL_SOCKET, SO_SNDBUF, (void *)&sendBufSize, &optlen) ;

	return(sendBufSize - numbuffed) ;
}


int writeHeader(int sock, unsigned short len, char msgType, unsigned short seq) {
	int count ;
	int ret ;

	if( (count = write(sock, &len, sizeof(len))) < 0 ) {
		return( count ) ;
	}
	ret=count ;

	if( (count = write(sock, &msgType, sizeof(msgType))) < 0 ) {
		return( count ) ;
	}
	ret+=count ;

	if( (count = write(sock, &seq, sizeof(seq))) < 0 ) {
		return( count ) ;
	}
	ret+=count ;

	// Send the next sequence we're expecting/waiting for
	// so that the other side know's what we've received so far
	if( (count = write(sock, &expectedIncommingSeq, sizeof(expectedIncommingSeq))) < 0 ) {
		return( count ) ;
	}
	ret+=count ;


	return( ret  ) ;
}


int writeHeader(int sock, Header *header) {
	return( writeHeader(sock, header->len, header->msgType, header->seq) ) ;
}


int readHeader(int sock, Header *header) {
	int count ;
	int ret ;
	if( (count = read(sock, &header->len, sizeof(header->len))) < 0 ) {
		logPerror("ERROR reading header len") ;
		return( count ) ;
	}

	if( count != sizeof(header->len) ) {
		log(error, "Couldn't read entire header len.  Exiting.\n") ;
		exit(1) ;
	}
	ret=count ;

	if( (count = read(sock, &header->msgType, sizeof(header->msgType))) < 0 ) {
		logPerror("ERROR reading header type") ;
		return( count ) ;
	}

	if( count != sizeof(header->msgType) ) {
		log(error, "Couldn't read entire header msgType.  Exiting.\n") ;
		exit(1) ;
	}
	ret+=count ;

	if( (count = read(sock, &header->seq, sizeof(header->seq))) < 0 ) {
		logPerror("ERROR reading header seq") ;
		return( count ) ;
	}

	if( count != sizeof(header->seq) ) {
		log(error, "Couldn't read entire header seq.  Exiting.\n") ;
		exit(1) ;
	}
	ret+=count ;

	if( (count = read(sock, &header->seqAck, sizeof(header->seqAck))) < 0 ) {
		logPerror("ERROR reading header seqAck") ;
		return( count ) ;
	}

	if( count != sizeof(header->seqAck) ) {
		log(error, "Couldn't read entire header seqAck.  Exiting.\n") ;
		exit(1) ;
	}
	ret+=count ;

	log(crazyDebug, "read header, %d bytes.  len : %u.  type : '%c'.  seq : %u.  seqAck : %u\n", ret, header->len, header->msgType, header->seq, header->seqAck ) ;

	return(ret);
}


bool sendHeartbeat(int sock ) {
	unsigned int sqleft = howMuchCouldWeWrite(sock) ;

	if( sqleft >= HEADER_SIZE ) {
		if( writeHeader(sock, 0,MSG_TYPE_HBEAT,0) != HEADER_SIZE ) {

			logPerror("Failed to write header!");
			exit(1) ;
		}
		return true ;
	} else {
		return false ;
	}

}

void onHeartbeatTimeout(int connectionIndex) {
	shutdown(fds[connectionIndex].fd, SHUT_RDWR) ;
	connections[connectionIndex].connected = false ; // mark as shutdown
	connections[connectionIndex].gotHeader = false ;
}


bool sendGapFill(int sock, unsigned short cur, unsigned short expected) {
	unsigned int sqleft = howMuchCouldWeWrite(sock) ;

	unsigned long header ;
	if( sqleft >= (sizeof(header)+sizeof(cur)+sizeof(expected)) ) {
		if( writeHeader(sock, 0,MSG_TYPE_HBEAT,0) != sizeof(header)) {
			logPerror("Failed to write header!");
			exit(1) ;
		}

		if( write(sock, &cur, sizeof(cur)) != sizeof(cur)) {
			logPerror("Failed to write cur!");
			exit(1) ;
		}

		if( write(sock, &expected, sizeof(expected)) != sizeof(expected)) {
			logPerror("Failed to write expected!");
			exit(1) ;
		}

		return( true ) ;
	} else {
		return( false ) ;
	}

}


bool sendGapFill(unsigned short cur, unsigned short expected) {
	log(debug, "attempting to send a gap fill.  cur: %u, expected : %u", cur, expected) ;

	// Find a connection we can write on:
	int np;
	if( (np = poll( fds, numfds, 0)) < 0 && errno != EINTR ) {
		logPerror("sendGapFill(): poll error") ;
		log(error, "np : %d\n",np) ;
		exit(1) ;
	}

	if(np) {
		for( int i=OUT_FDI+1 ; i < numfds ; i++ ) {
			if( fds[i].revents | POLLOUT ) {
				if( sendGapFill(fds[i].fd, cur, expected) ) {
					log(debug, "sent gap fill to %d",i) ;
					return true ;
				}
			}
		}
	}

	// nothing available to send
	return( false ) ;
}


bool allPipesClosed() {
	// see if this is the last connection:
	for(int i=firstPipeConnection ; i < numfds ; i++ ) {
		if( connections[i].connected) {
			log(debug, "allPipesClosed(): pipe %d still connected.\n", i) ;
			return false ;
		}
	}

	return true ;
}


void shutdownConnection(int connectionIndex) {
	log(debug,"shutdownConnection(%d)\n", connectionIndex) ;
	shutdown(connections[connectionIndex].sock,SHUT_RDWR) ;
	connections[connectionIndex].connected = false ;

	fds[connectionIndex].fd = -fds[connectionIndex].fd ; // De-register from all poll events

	if( shutdownInProgress ) {
		if( allPipesClosed() ) {
			log(error, "All pipes closed.  Exiting normally.\n") ;
			exit(0) ;
		}
	} else {
		// Only the client re-connects
		if( connections[connectionIndex].isClient ) {
			// setup a new timer:
			ReconnectTimer connTimer ;
			connTimer.connectionIndex = connectionIndex;
			connTimer.timeout = RECONNECT_TIMEOUT ;
			timerList.push_back(connTimer) ;
			log(debug, "read from pipe socket %d closed.  Will restart.\n", connectionIndex) ;
		} else {
			log(debug, "read from pipe server socket closed.  Client will reopen (we trust).\n") ;
		}
	}

	mayNeedToSendGapfill = true ;
}

uunsigned short isSeqLessThanExpected(unsigned short seq, unsigned short nextExpected) {
	// Check to see if the node's sequence is "less" than the ack
	// Problem: seq is an unsigned short that can wrap
	// Solution: see if the distance > half max short value (SHRT_MAX)
	//           this works for all cases except where we gap on 32768
	//           or > packets, so its good enough.
    unsigned short diff = seq - nextExpected ;
    return (diff > SHRT_MAX) ;
}

void readHeader(int sock, int connectionIndex, unsigned long curTime) {
	unsigned int availableToRead ;
	ioctl(sock, FIONREAD, &availableToRead) ;

	if( availableToRead == 0 ) {
		// Socket Closed:
		log(error, "readHeader() : socket close detected.  Shutting down connections") ;
		shutdownConnection(connectionIndex) ;
		return ;
	}

	log(crazyDebug, "readHeader() : available to read : %d (%d)\n", availableToRead, HEADER_SIZE ) ;
	if( availableToRead >= HEADER_SIZE ) {

		Header *header = &connections[connectionIndex].header;
		if( readHeader(sock, header) != HEADER_SIZE ) {
			logPerror("Failed to read header") ;
			exit(1) ;
		}

		connections[connectionIndex].lastRead = curTime ;

		switch( header->msgType ) {
		case MSG_TYPE_HBEAT:
			// Nothing needed to be done
			break;
		case MSG_TYPE_LOGOUT:
			log(debug, "Got logout.  Shutting down connection.\n") ;
			shutdownInProgress = true ;
			shutdownConnection(connectionIndex) ;
			break;
		case MSG_TYPE_DATA:
			connections[connectionIndex].gotHeader = true ;
			break ;
		case MSG_TYPE_GAPFIL:
			// TODO
			break ;
		default:
			log(error, "Unknown msg type received: 0x%x.  Exiting.\n", header->msgType) ;
			exit(1) ;
		}

		log(crazyDebug, "Got header : len : %d (0x%x), type : %d (0x%x) seq : %d (0x%x)\n",
				header->len, header->len, header->msgType, header->msgType, header->seq, header->seq) ;

		// Drop all acked nodes from our retrans list:
		int count=0 ;
		for (list<BufNode *>::iterator itor = retransNodeBufs.begin();
				itor != retransNodeBufs.end(); ++itor) {
			BufNode *tnode = *itor;

			// Can't hurt to check jic.
			if (tnode == NULL) {
				log(error,
						"ERROR: retransNodeBufs list iterator points to a NULL element\n");
				exit(1);
			}

			log(crazyDebug,
					"Checking retransNodeBufs #%d, looking for all nodes with seq < %d.  node seq : %d\n",
					count++, header->seqAck, tnode->header.seq);

			if( isSeqLessThanExpected(tnode->header.seq, header->seqAck) ) {
				itor = retransNodeBufs.erase(itor);
			}
		}
	}
}


int readDataFromPipe(int sock, int connectionIndex, unsigned long curTime, BufNode *node, unsigned short len) {
	unsigned int availableToRead ;
	ioctl(sock, FIONREAD, &availableToRead) ;

	// See if socket was closed
	if( availableToRead == 0 ) {
		shutdownConnection(connectionIndex) ;
		return(0) ;
	}

	if( availableToRead < len ) {
		return 0 ;
	}

	if( len > sizeof(node->buf.buf)) {
		log(error, "readDataFromPipe() : ERROR: invalid read len : %d > max : %d.  Exiting!\n", len, sizeof(node->buf.buf)) ;
		exit(1) ;
	}

	int nr = read(sock, node->buf.buf, len) ;
	if( nr > 0 ) {
		node->buf.len=nr ;
	}

	return( nr ) ;
}


bool writeToPipe(int sock, int connectionIndex, long curTime, BufNode *node) {
	unsigned int sqleft = howMuchCouldWeWrite(sock) ;

	if( sqleft <  (HEADER_SIZE + node->buf.len) ) {
		log(debug, "not enough room to write msg\n") ;
		return false ;
	}

	log(crazyDebug, "sqleft : %u, incommingSeq : %d, len : %d, type : %c\n", sqleft, node->header.seq, node->header.len, node->header.msgType ) ;

	if( node->buf.len > sizeof(node->buf.buf) ) {
		log(error, "ERROR: invalid buf len : %d.  Exiting.\n", node->buf.len ) ;
		exit(1) ;
	}

	connections[connectionIndex].lastWrite=curTime ;

	if( writeHeader(sock, &node->header) != HEADER_SIZE) {
		logPerror("Failed to write header!");
		exit(1) ;
	}

	log(crazyDebug, "Wrote header to sock\n") ;

	// now the data:
	unsigned int nw ;
	if( (nw = write(sock, node->buf.buf, node->buf.len)) < 0 ) {
		logPerror( "writeToPipe: error writing to pipe.") ;
		exit(1) ;
	}

	if( nw != node->buf.len ) {
		log(error, "Unexpected error writing to pipe (%d:%d).\n",nw, node->buf.len) ;
		exit(1) ;
	}

	log(crazyDebug, "Wrote : %d bytes to sock\n", node->buf.len) ;

	return( true ) ;
}


int readFromInbound(int fd, int connectionIndex, unsigned long curTime, BufNode *node) {
	int n = read(fd, node->buf.buf, sizeof(node->buf.buf)) ;
	log(crazyDebug, "readFromInbound(): read %d bytes from connection %d\n",n,connectionIndex) ;

	if( n == 0 ) {
		// Input closed
		shutdown(fd, SHUT_RD) ;
	} else if( n < 0 ) {
		logPerror("read from input returned error") ;
		exit(1) ;
	} else {
		node->buf.len = n ;
	}

	return(n) ;
}


void writeToOutbound(int sock, int connectionIndex, long curTime, BufNode *node) {
	if( node->buf.pos >= node->buf.len ) {
		log(error, "writeToOutbound(): ERROR: invalid buf pos : %d.  len : %d.  Exiting!\n", node->buf.pos, node->buf.len ) ;
		exit(1) ;
	}

	if( node->buf.len > sizeof(node->buf.buf) ) {
		log(error, "writeToOutbound(): ERROR: invalid buf len : %d.  pos : %d.  Exiting!\n", node->buf.len, node->buf.pos ) ;
		exit(1) ;
	}

	int nw = write(sock, &node->buf.buf[node->buf.pos], node->buf.len-node->buf.pos) ;

	if( nw < 0 ) {
		logPerror("Couldn't write full buf outbound.  Exiting!\n") ;
		exit(1) ;
	}

	node->buf.pos += nw ;
}


void updateRoundRobin() {
	roundRobinLast++ ;
}

int walkOrder(int count, int max) {
	return( (count + roundRobinLast) % max ) ;
}

long getCurtime() {
	// Get the time:
	struct timeval tv ;
	if( gettimeofday(&tv, NULL) < 0 ) {
		logPerror("Couldn't get time of day") ;
		exit(1) ;
	}
	return( tv.tv_sec*1000L + tv.tv_usec/1000 ) ;
}


int getFreeConnectionIndex() {
	// see if we have a connection we can reuse:
	for( int i=firstPipeConnection ; i < numfds ; i++ ) {
		if( !connections[i].connected ) {
			return(i) ;
		}
	}

	if( numfds > MAX_CONNCECTIONS ) {
		log(error, "ERROR: no free connections.  Max connections : %d.  Exiting!", MAX_CONNCECTIONS ) ;
		exit(1) ;
	}
	return( numfds++ ) ;
}

void processPipeInput(int i, unsigned long curtime) {
	log(crazyDebug, "Reading from pipe : %d\n", i);

	// From Pipe to Outbound

	if (connections[i].gotHeader) {
		log(crazyDebug, "Reading data from pipe : %d\n", i);

		BufNode *nodep;
		// We may have already allocated an empty node at the end of fromPipeNodeBufs
		// But if there's not an empty node there, add one:
		if (fromPipeNodeBufs.empty()
				|| (fromPipeNodeBufs.back()->buf.len != 0)) {
			nodep = new BufNode();
			fromPipeNodeBufs.push_back(nodep);
		} else {
			nodep = fromPipeNodeBufs.back();
		}

		unsigned short lenToRead = connections[i].header.len;
		if (lenToRead > sizeof(nodep->buf.buf)) {
			log(error, "ERROR: header length : %u > sizeof(buf) : %u\n",
					lenToRead, sizeof(nodep->buf.buf));
			exit(1);
		}

		int n = readDataFromPipe(fds[i].fd, i, curtime, nodep,
				lenToRead);
		if (n < 0) {
			logPerror("read failed");
			exit(1);
		}

		if (n != 0) {
			log(crazyDebug, "Read %d bytes from pipe : %d\n", n, i);

			// copy header to node:
			nodep->header.len = connections[i].header.len;
			nodep->header.msgType = connections[i].header.msgType;
			nodep->header.seq = connections[i].header.seq;

			connections[i].gotHeader = false; // we're on to the next message

			// Check for dupes:
			if( isSeqLessThanExpected( nodep->header.seq, expectedIncommingSeq ) ) {
				log(debug, "Duped on incoming node : seq : %u, expected : %u\n", nodep->header.seq, expectedIncommingSeq) ;
				fromPipeNodeBufs.remove(nodep) ;  // Drop it
			} else {
				// Make sure that we're registered to write outbound:
				fds[OUT_FDI].events |= POLLOUT;
			}
		}
	} else {
		log(crazyDebug, "Reading header from pipe : %d\n", i);
		readHeader(fds[i].fd, i, curtime);
	}
}

void processInboundInput(unsigned long curtime) {
	log(crazyDebug, "Reading Inbound\n");
	// From Inbound to Pipe
	BufNode *nodep = new BufNode();

	int nr = readFromInbound(fds[IN_FDI].fd, IN_FDI, curtime, nodep);
	if (nr == 0) {
		log(debug,
				"Detected end of input (0 bytes read). Now waiting for all input data to be written to pipes.\n");
		fds[IN_FDI].fd = -1; // de-register from poll
		shutdownInProgress = true;

		// Special case where we are shutting down connections, and we read 0 bytes from closed input.
		delete nodep;
	} else {
		log(crazyDebug, "Read %d bytes from Inbound\n", nr);
		toPipeNodeBufs.push_back(nodep);

		// Make sure we're registered to see when we can write:
		for (int index = firstPipeConnection; index < numfds; index++) {
			log(crazyDebug, "setting fds[%d] to accept POLLOUT\n",
					index);
			fds[index].events |= POLLOUT;
		}
	}
}

void processPipeOutput(int i, unsigned long curtime) {
	// To Pipe from Inbound
	if (!toPipeNodeBufs.empty()) {
		BufNode *nodep = toPipeNodeBufs.front();

		log(crazyDebug, "Setting up header to send to pipe %d\n", i);
		nodep->header.len = nodep->buf.len;
		nodep->header.msgType = MSG_TYPE_DATA;
		nodep->header.seq = nextOutgointSeq;

		// Double check jic
		if ((nodep->header.len <= 0)
				|| (nodep->header.len > sizeof(nodep->buf.buf))) {
			log(error, "ERROR: Invalid header length : %d.  Exiting!\n",
					nodep->header.len);
			exit(1);
		}

		if (writeToPipe(fds[i].fd, i, curtime, nodep)) {
			log(crazyDebug, "Wrote %u bytes to pipe %d\n",
					nodep->buf.len, i);
			nextOutgointSeq++;

			toPipeNodeBufs.remove(nodep);
			retransNodeBufs.push_back(nodep);
		} else {
			log(debug,
					"Pipe %d was writable but not enough for our buffer of length : %u\n",
					i, nodep->buf.len);
		}

	} else {
		// wait for something to send:
		log(crazyDebug, "Waiting for input to send to a pipe\n");
		fds[i].events &= ~POLLOUT;
	}
}


void processOutboundOutput(unsigned long curtime) {
	// To Outbound from Pipe
	log(crazyDebug, "Checking if we have next outbound sequence\n");

	// See if we have next expected sequence:
	BufNode *node = NULL;
	int count = 0;
	for (list<BufNode *>::iterator itor = fromPipeNodeBufs.begin();
			itor != fromPipeNodeBufs.end(); ++itor) {
		BufNode *tnode = *itor;

		// Can't hurt to check jic.
		if (tnode == NULL) {
			log(error,
					"ERROR: list iterator points to a NULL element\n");
			exit(1);
		}

		log(crazyDebug,
				"Checking fromPipeNodeBufs #%d, looking for seq %d.  node seq : %d\n",
				count++, expectedIncommingSeq, tnode->header.seq);
		if (tnode->buf.len && (tnode->header.seq == expectedIncommingSeq)) {
			node = tnode;
			break;
		}
	}

	if (node != NULL) {
		log(crazyDebug, "Sending from pipe : %d\n", count);
		writeToOutbound(fds[OUT_FDI].fd, OUT_FDI, curtime, node);
		if (node->buf.pos == node->buf.len) {
			log(crazyDebug, "Sent seq %u from pipe : %d.  %u bytes\n",
					expectedIncommingSeq, count, node->buf.len);
			expectedIncommingSeq++;
			fromPipeNodeBufs.remove(node); // Its sent, we're done with it.
			delete node;

			if (fromPipeNodeBufs.empty()) {
				log(crazyDebug,
						"Nothing left for OUT, de-registering POLLOUT\n");
				fds[OUT_FDI].events &= ~POLLOUT;
			}
		}
	} else {
		log(crazyDebug,
				"Don't have next sequence for pipe : %d.  incommingSeq : %d\n",
				count, expectedIncommingSeq);

		// Need to wait for the next expected sequence.
		fds[OUT_FDI].events &= ~POLLOUT;
	}
}

int main(int argc, char *argv[]) {
	char usage[100] ;
	sprintf(usage,
			"Usage: %s <-p port> [-s <server ip>] [-c <servier ip> -s <source IP 1> [-s <source IP 2> ... -s <SOURCE IP n>]]\n"
			"       additional options:\n"
			"          -f <host:port>\n"
			"          -d <debug level> (all, debug, info, error)\n"
			"          -l <log file>",
			argv[0]) ;

	char *hostname = NULL ;
	char *sIps[MAX_CONNCECTIONS] ;
	char isServer = 1 ;
	int port = 0 ;
	char *forwardHost = NULL ;
	int forwardPort = 0 ;
	char isForwardOpened = 0 ;

	int opt ;
	while( (opt= getopt(argc, argv, "s:c:p:f:d:l:")) != -1 ) {
		switch(opt) {
		case 'p' :
			port = atoi(optarg) ;
			break ;
		case 's' :
			sIps[numConnections++] = optarg ;
			break ;
		case 'c':
			isServer = 0 ; // We're a client!
			hostname = optarg ;
			break ;
		case 'f' :
			char *colon ;
			colon = strchr(optarg,':') ;
			if( colon == NULL ) {
				fprintf(stderr, usage) ;
				exit(1) ;
			}

			forwardPort=atoi(colon+1) ;

			int hostLen ;
			hostLen = colon-optarg ;
			forwardHost=(char *)malloc(hostLen+1) ;

			strncpy(forwardHost, optarg, hostLen) ;
			forwardHost[hostLen]='\0' ;
			break ;
		case 'd' :
			if( strcasecmp( optarg, "all") == 0 ) {
				logLevel = crazyDebug ;
			} else if( strcasecmp( optarg, "debug") == 0 ) {
				logLevel = debug ;
			} else if( strcasecmp( optarg, "verbose") == 0 ) {
				logLevel = verbose ;
			} else if( strcasecmp( optarg, "info") == 0 ) {
				logLevel = info ;
			} else if( strcasecmp( optarg, "info") == 0 ) {
				logLevel = error ;
			} else {
				fprintf(stderr, "Unknown debug level '%s'\n", optarg) ;
				exit(1) ;
			}
			break ;
		case 'l' :
			if( (logfile = fopen(optarg, "a" )) == NULL ) {
				fprintf(stderr, "Unable to open logfile : '%s'\n", optarg ) ;
				exit(1) ;
			}
			break ;
		case '?' :
			fprintf(stderr, usage) ;
			exit(1) ;
			break ;
		}
	}

	if( !port ) {
		fprintf(stderr, usage) ;
		exit(1) ;
	}

	if( isServer && numConnections != 1 ) {
		fprintf(stderr, usage) ;
		exit(1) ;
	}

	if(isServer) {
		hostname = sIps[0] ;
	}

	fds[IN_FDI].fd = forwardHost ? -1 : STDIN_FILENO ;
	fds[IN_FDI].events = POLLIN ;
	fds[OUT_FDI].fd = forwardHost ? -1 : STDOUT_FILENO ;
	fds[OUT_FDI].events = 0 ;

	int servSockFdi = 0 ;
	if( isServer ) {
		log(debug, "Setting up server connections\n") ;
		int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if( sock < 0 ) {
			logPerror("ERROR: could not open socket") ;
			exit(1) ;
		}

		int optval = 1;
		setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&optval, sizeof(optval)) ;

		struct sockaddr_in bind_addr ;
		bind_addr.sin_port = htons(port) ;
		bind_addr.sin_family = AF_INET;

		if( !inet_aton(hostname,&bind_addr.sin_addr) ) {
			fprintf(stderr, "Invalid source address : %s\n", hostname) ;
			exit(1) ;
		}

		if( bind(sock, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) < 0 ) {
			logPerror("bind failed.") ;
			exit(1) ;
		}

		// start listening:
		listen(sock,MAX_CONNCECTIONS) ;  // MAX_CONNECTIONS seems reasonable for backlog

		log(verbose, "Listening on %s:%d\n",hostname,port) ;

		fds[numfds].fd = sock ;
		fds[numfds].events = POLLIN ;

		connections[servSockFdi].connected = false ;
		connections[servSockFdi].sock = sock ;

		servSockFdi = numfds++ ;
		firstPipeConnection = numfds ;
	} else {
		log(debug, "Setting up client connections\n") ;
		firstPipeConnection = numfds ;
		for(int i=0 ; i < numConnections ; i++ ) {
			log(debug, "client connection #%d.  fdnum : %d\n",i,numfds) ;
			connections[numfds].isClient = true ;
			connections[numfds].sourceIp = sIps[i] ;
			connections[numfds].destHost = hostname ;
			connections[numfds].destPort = port ;

			int sock = openOutgoingConnection(&connections[numfds]) ;

			connections[numfds].sock = sock ;
			connections[numfds].connected = true ;
			connections[numfds].lastRead = getCurtime() ;
			connections[numfds].lastWrite = getCurtime() ;
			connections[numfds].gotHeader = false ;

			fds[numfds].fd = sock ;
			fds[numfds].events = POLLIN ;

			numfds++ ;
		}
	}

	// Main Loop:
	unsigned long curtime ; // millis
	while(1) {

		int np;
		if( (np = poll( fds, numfds, POLL_TIMEOUT)) < 0 && errno != EINTR ) {
			logPerror("poll error") ;
			log(error, "np : %d\n",np) ;
			exit(1) ;
		}

		curtime = getCurtime() ;

		// 0 polls means timeout:
		if( np == 0 ) {
			log(crazyDebug, "processing heartbeats and timers.  curtime : %ld.  numConnections : %d\n",curtime, numConnections) ;

			// Do heartbeat timeout processing:
			for(int i=firstPipeConnection ; i < numfds ; i++ ) {
				if( connections[i].connected ) {
					if( (connections[i].lastRead + HEARTBEAT_TIMEOUT) < curtime ) {
						log(error, "Failed to get heartbeat on connection %d\n", i) ;
						onHeartbeatTimeout(i) ;
						continue ; // we just shutdown this connection, don't process any more
					}

					/* See if we have nothing going out, we may need to send a heartbeat */
					if( (connections[i].lastWrite + HEARTBEAT_SEND_TIME ) < curtime ) {
						log(crazyDebug, "Sending heartbeat on connection %d.  lastWrite : %lu\n",i, connections[i].lastWrite) ;
						if( sendHeartbeat(fds[i].fd) ) {
							connections[i].lastWrite = curtime ;
						}
					}
				}
			}

			// process timers:
			for( list<ReconnectTimer>::iterator itor = timerList.begin() ; itor != timerList.end() ; ++itor) {
				ReconnectTimer t = *itor ;
				if( (t.timeout != 0) && curtime > t.timeout ) {
					log(debug, "timer for connection #%d expired\n", t.connectionIndex) ;

					t.onTimeout(&connections[t.connectionIndex], &fds[t.connectionIndex], curtime) ;

					// remove the timer.  We're done with it:
					itor = timerList.erase(itor) ;
				}
			}
			continue ;
		}

		log(crazyDebug, "poll returned : %d\n",np) ;

		bool somePipeWriteable = false ;
		for(int count=0 ; count < numfds ; count++ ) {
			int i = walkOrder(count, numfds) ;

			short int revents = fds[i].revents ;

			// Check for new connections (Server only):
			if( isServer && (servSockFdi == i) && (revents == POLLIN) ) {
				log(verbose,"Accepting connection (%d).  numfds: %d.\n",i,numfds ) ;

				int sock = accept(fds[i].fd, NULL, NULL) ;

				int nci = getFreeConnectionIndex() ;

				log(debug, "Using free connection #%d\n",nci) ;

				connections[nci].isClient = false ;
				connections[nci].sock = sock ;
				connections[nci].connected = true ;
				connections[nci].lastRead = curtime ;
				connections[nci].lastWrite = curtime ;

				fds[nci].fd = sock ;
				fds[nci].events = POLLIN | POLLOUT ;

				// If we are forwarding and this is the first accepted connection,
				// then open our forwarding port:
				if( forwardHost && !isForwardOpened ) {
					log(verbose, "Opening connection to forward Host : %s:%d\n", forwardHost, forwardPort);
					isForwardOpened = 1 ;

					int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
					if( sock < 0 ) {
						logPerror("ERROR: could not open socket") ;
						exit(1) ;
					}

					struct hostent *fhadd = gethostbyname(forwardHost) ;
					if( fhadd == NULL ) {
						fprintf(stderr, "unknown host %s", forwardHost);
						exit(1) ;
					}
					struct sockaddr_in fip ;

					fip.sin_family = AF_INET ;

					// copy in address:
					bcopy((char *)fhadd->h_addr, (char *)&fip.sin_addr.s_addr,fhadd->h_length) ;

					// port:
					fip.sin_port = htons(forwardPort) ;

					if( connect(sock, (struct sockaddr *)&fip, sizeof(fip)) < 0 ) {
						logPerror("ERROR: connect failed.") ;
						exit(1) ;
					}

					fds[IN_FDI].fd = sock ;
					fds[IN_FDI].events = POLLIN ;
					fds[OUT_FDI].fd = sock ;
					fds[OUT_FDI].events = POLLOUT ;
				}
				continue ;
			}

			if (revents & POLLIN) {
				if ( IN_FDI == i) {
					processInboundInput(curtime) ;
				} else {
					processPipeInput(i,curtime) ;
				}
			}

			if (revents & POLLOUT) {
				if ( OUT_FDI == i) {
					somePipeWriteable = true ;
					processOutboundOutput(curtime) ;
				} else {
					processPipeOutput(i,curtime) ;
				}
			}

			// Check for disconnections:
			if (revents & (POLLERR | POLLHUP)) {
				if (i == OUT_FDI) {
					log(error, "Unexpected ERROR on output : 0x%x.  Exiting!\n",
							revents);
					exit(1);
				} else if (i == IN_FDI) {
					if ((revents & POLLIN) != 0) {
						log(crazyDebug,
								"Detected input close (0x%x), but still more to read.  Looping.\n",
								revents);
					} else {
						log(debug,
								"Detected end of input (0x%x). Now waiting for all input data to be written to pipes.\n",
								revents);
						fds[IN_FDI].fd = -1; // de-register from poll
						shutdownInProgress = true;
					}
				} else {
					log(debug,
							"fds[%d] : revents ERROR : 0x%x.  Will shutdown connection.\n",
							i, revents);
					shutdownConnection(i);
				}
			}
		}

		// When pipes out are full, or not connected, we we want to:
		//   - Not use up memory unnecessarily
		//   - Avoid potential buffer bloat
		//   - Provide push back feedback/flow control from pipes to Inbound
		if( !somePipeWriteable ) {
			// As long as some pipe is writable, we should be reading Inbound:
			fds[IN_FDI].events |= POLLIN ;
		} else {
			// Stop reading more inbound input until we have somewhere to write it:
			fds[IN_FDI].events &= ~POLLIN ;
		}

		updateRoundRobin() ;


		if( shutdownInProgress && !sentLogouts && toPipeNodeBufs.empty() ) {
			sentLogouts = true ;

			log(debug, "We're shutting down, and done sending input.  Logging out all connections.  Exit after connections shutdown\n") ;

			// input done.  Shutdown all pipes when they are done writing:
			for(int opIndex = firstPipeConnection ; opIndex < numfds ; opIndex++ ) {
				if( connections[opIndex].connected ) {
					log(debug, "Sending logout on connection %d\n",opIndex) ;
					writeHeader(connections[opIndex].sock,0,MSG_TYPE_LOGOUT,0) ;
				}
			}
		}
	}

	return 0;
}
