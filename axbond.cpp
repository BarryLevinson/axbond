//============================================================================
// Name        : axbond.cpp
// Author      : Barry Levinson
// Version     : .1
// Copyright   : Copyright 2015 Barry Levinson
// Description : axbond tunnels stdin/stdout over multiple TCP connections.
//============================================================================

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

#define RBUF_SIZE  5 /*(1024*1024*10)*/
#define STDIN_FDI 0
#define STDOUT_FDI 1
#define MAX_CONNCECTIONS 8

// Message types:
#define MSG_TYPE_DATA  'S'
#define MSG_TYPE_HBEAT 'H'

struct ppipe {
	char *buf ;
	int head ;
	int tail ;
	int read_fdi[MAX_CONNCECTIONS] ;
	int num_read_connections ;
	int write_fdi[MAX_CONNCECTIONS] ;
	int num_write_connections ;
};

struct bufInfo {
	char weHasIt ;
	unsigned int len ;
	char msgType ;
	unsigned short seq ;
};

unsigned short toSeq = 0 ;
unsigned short fromSeq = 0 ;


int readFromPipe(int fd, char *buf, unsigned int remaining, struct bufInfo *bi) {
	unsigned int availableToRead ;
	ioctl(fd, FIONREAD, &availableToRead) ;

	if( bi->weHasIt ) {
		// TODO : need to check remaining!
		if( availableToRead >= bi->len ) {
			bi->weHasIt = 0 ;
			return( read(fd, buf, remaining) ) ;
		} else {
			return(0) ;
		}
	} else {
		long header ;
		if( availableToRead > sizeof(header) ) {
			if( read(fd, &header, sizeof(header)) != sizeof(header) ) {
				perror("Failed to read header") ;
				exit(1) ;
			}
			bi->len = header >> (sizeof(int)*8) ;
			bi->msgType = (header >> (sizeof(short))) && 0xFF ;
			bi->seq = header ;
			bi->weHasIt = 1 ;
		}
		return(0) ;
	}
}


int writeToPipe(int fd, char *buf, unsigned int available, struct bufInfo *bi ) {
	// check available space on the socket
	int numbuffed ;
	if( ioctl(fd, TIOCOUTQ, &numbuffed) < 0 ) {
		fprintf(stderr, "writeToPipe: failed to determine TCP write space left.") ;
		exit(1) ;
	}

	int sendBufSize ;
	socklen_t optlen = sizeof(sendBufSize);
	getsockopt(fd, SOL_SOCKET, SO_SNDBUF, (void *)&sendBufSize, &optlen) ;

	unsigned int sqleft = sendBufSize - numbuffed ;

	char msgType = MSG_TYPE_DATA ;

	unsigned int len ;
	if( sqleft <= (unsigned int)(sizeof(toSeq) + sizeof(len) + sizeof(msgType)) ) {
		return 0 ;
	}

	len = sqleft - sizeof(long);

	if( available < len ) {
		len = available ;
	}

	// pack the header into a long to save some space, yet still pad so we can read it back out easily.
	long header = (long)len << (sizeof(int)*8) | msgType << (sizeof(short)*8) | toSeq ;
	if( write(fd, &header, sizeof(header)) != sizeof(header)) {
		perror("Failed to write header!");
		exit(1) ;
	}
	toSeq++ ;

	// now the data:
	unsigned int nw ;
	if( (nw = write(fd, buf, len)) < 0 ) {
		perror( "writeToPipe: error writing to pipe.") ;
		exit(1) ;
	}

	if( nw != (unsigned int) len ) {
		fprintf(stderr, "Unexpected error writing to pipe (%d:%d).\n",nw, len) ;
		exit(1) ;
	}

	return( nw ) ;
}


int readFromStdin(int fd, char *buf, unsigned int remaining, struct bufInfo *bi) {
	return( read(fd, buf, remaining) ) ;
}


int writeToStdin(int fd, char *buf, unsigned int available, struct bufInfo *bi) {
	return( write(fd, buf, available) ) ;
}

int main(int argc, char *argv[]) {
	char usage[100] ;
	sprintf(usage, "Usage: %s <-p port> [-s <server ip>] [-c <servier ip> -s <source IP 1> [-s <source IP 2> ... -s <SOURCE IP n>]]\n", argv[0]) ;

	char *hostname = NULL ;
	char *sIps[MAX_CONNCECTIONS] ;
	int numSIps = 0 ;
	char isServer = 1 ;
	int port = 0 ;

	int opt ;
	while( (opt= getopt(argc, argv, "s:c:p:")) != -1 ) {
		switch(opt) {
		case 'p' :
			port = atoi(optarg) ;
			break ;
		case 's' :
			sIps[numSIps++] = optarg ;
			break ;
		case 'c':
			isServer = 0 ; // We're a client!
			hostname = optarg ;
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

	if( isServer && numSIps != 1 ) {
		fprintf(stderr, usage) ;
		exit(1) ;
	}

	if(isServer) {
		hostname = sIps[0] ;
	}

	struct ppipe toProxy ;
	toProxy.buf = (char *)malloc( RBUF_SIZE ) ;
	toProxy.head = 0 ;
	toProxy.tail = 0 ;
	toProxy.read_fdi[0] = STDIN_FDI ;
	toProxy.num_read_connections = 1 ;
	toProxy.num_write_connections = 0 ;

	struct ppipe fromProxy ;
	fromProxy.buf = (char *)malloc( RBUF_SIZE ) ;
	fromProxy.head = 0 ;
	fromProxy.tail = 0 ;
	fromProxy.num_read_connections = 0 ;
	fromProxy.write_fdi[0] = STDOUT_FDI ;
	fromProxy.num_write_connections = 1 ;

	int numfds = 2 ;
	struct pollfd fds[MAX_CONNCECTIONS+2] ;

	fds[STDIN_FDI].fd = STDIN_FILENO ;
	fds[STDIN_FDI].events = POLLIN ;
	fds[STDOUT_FDI].fd = STDOUT_FILENO ;
	fds[STDOUT_FDI].events = 0 ;

	struct bufInfo bufInfos[MAX_CONNCECTIONS+2] ;
	memset(bufInfos,0,sizeof(bufInfos)) ;

	int servSockFdi = 0 ;
	if( isServer ) {
		int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if( sock < 0 ) {
			perror("ERROR: could not open socket") ;
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
			perror("bind failed.") ;
			exit(1) ;
		}

		// start listening:
		listen(sock,MAX_CONNCECTIONS) ;  // MAX_CONNECTIONS seems reasonable for backlog

		fds[numfds].fd = sock ;
		fds[numfds].events = POLLIN ;
		servSockFdi = numfds++ ;
	} else {
		for(int i=0 ; i < numSIps ; i++ ) {
			int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
			if( sock < 0 ) {
				perror("ERROR: could not open socket") ;
				exit(1) ;
			}

			// Bind to the source address:
			struct sockaddr_in bind_addr ;
			bind_addr.sin_port = htons(0) ; // any port
			bind_addr.sin_family = AF_INET;

			if( !inet_aton(sIps[i],&bind_addr.sin_addr) ) {
				fprintf(stderr, "Invalid source address : %s\n", sIps[i]) ;
				exit(1) ;
			}

			if( bind(sock, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) < 0 ) {
				perror("bind failed.") ;
				exit(1) ;
			}

			struct hostent *proxy = gethostbyname(hostname) ;
			if( proxy == NULL ) {
				fprintf(stderr, "unknown host %s", hostname);
				exit(1) ;
			}
			struct sockaddr_in pip ;

			pip.sin_family = AF_INET ;

			// copy in address:
			bcopy((char *)proxy->h_addr, (char *)&pip.sin_addr.s_addr,proxy->h_length) ;

			// port:
			pip.sin_port = htons(port) ;

			if( connect(sock, (struct sockaddr *)&pip, sizeof(pip)) < 0 ) {
				perror("ERROR: connect failed.") ;
				exit(1) ;
			}

			fds[numfds].fd = sock ;
			fds[numfds].events = POLLIN ;

			toProxy.write_fdi[toProxy.num_write_connections++] = numfds ;
			fromProxy.read_fdi[fromProxy.num_read_connections++] = numfds ;
			numfds++ ;
		}
	}

	while(1) {

		int np;
		if( (np = poll( fds, numfds, -1)) < 0 && errno != EINTR ) {
			perror("poll error") ;
			fprintf(stderr, "np : %d\n",np) ;
			exit(1) ;
		}

		if( np == 0 ) {
			continue ;
		}

		for(int i=0 ; i < numfds ; i++ ) {

			short int revents = fds[i].revents ;
			if( revents & (POLLERR|POLLHUP) ) {
				perror("poll error") ;
				exit(1) ;
			}

			if( isServer && (servSockFdi == i) && (revents == POLLIN) ) {
				int sock = accept(fds[i].fd, NULL, NULL) ;

				fds[numfds].fd = sock ;
				fds[numfds].events = POLLIN ;

				toProxy.write_fdi[toProxy.num_write_connections++] = numfds ;
				fromProxy.read_fdi[fromProxy.num_read_connections++] = numfds ;
				numfds++ ;
				continue ;
			}

			if( revents & POLLIN ) {
				int (*readFuncPtr)(int, char*, unsigned int, struct bufInfo*) ;
				struct ppipe *pipe ;
				if( STDIN_FDI == i ) {
					pipe = &toProxy ;
					readFuncPtr = &readFromStdin ;
				} else {
					pipe = &fromProxy ;
					readFuncPtr = &readFromPipe ;
				}

				// If our buf is full, don't read any more until its got some room:
				if( ((pipe->tail+1) % RBUF_SIZE) == pipe->head ) {
					fds[i].events &= ~POLLIN ;
				}

				int maxRead = (RBUF_SIZE-1) - ((RBUF_SIZE + pipe->tail - pipe->head) % RBUF_SIZE);
				if( pipe->tail + maxRead > RBUF_SIZE ) {
					maxRead = RBUF_SIZE - pipe->tail ;
				}

				int n = (*readFuncPtr)(fds[i].fd, &pipe->buf[pipe->tail], maxRead, &bufInfos[i] ) ;
				if( n < 0 ) {
					perror("read failed") ;
					exit( 1 ) ;
				}
				pipe->tail = (pipe->tail + n) % RBUF_SIZE ;

				// Make sure we're registered to see when we can write:
				for(int index = 0 ; index < pipe->num_write_connections  ; index ++ ) {
					fds[pipe->write_fdi[index]].events |= POLLOUT ;
				}
			}

			if( revents & POLLOUT ) {
				int (*writeFuncPtr)(int, char*, unsigned int, struct bufInfo*) ;
				struct ppipe *pipe ;
				if( STDOUT_FDI == i ) {
					pipe = &fromProxy ;
					writeFuncPtr = &writeToStdin ;
				} else {
					pipe = &toProxy ;
					writeFuncPtr = &writeToPipe ;
				}

				// If we have nothing left to write, de-register from writable event
				if(pipe->head == pipe->tail) {
					fds[i].events &= ~POLLOUT ;
					continue ;
				}

				int maxWrite = (RBUF_SIZE + pipe->tail - pipe->head) % RBUF_SIZE ;
				if( pipe->head + maxWrite >= RBUF_SIZE ) {
					maxWrite = RBUF_SIZE - pipe->head ;
				}

				int n = (*writeFuncPtr)(fds[i].fd, &pipe->buf[pipe->head], maxWrite, &bufInfos[i] ) ;
				if( n < 0 ) {
					perror("write failed") ;
					exit( 1 ) ;
				}
				pipe->head = (pipe->head + n) % RBUF_SIZE ;

				// Make sure we're registered to see what can be read:
				for(int index = 0 ; index < pipe->num_read_connections  ; index ++ ) {
					fds[pipe->read_fdi[index]].events |= POLLIN ;
				}
			}
		}
	}



	return 0;
}
