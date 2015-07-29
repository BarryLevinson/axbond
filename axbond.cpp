//============================================================================
// Name        : axbond.cpp
// Author      : Barry Levinson
// Version     : .1
// Copyright   : Copyright 2015 Barry Levinson
// Description : axbond tunnels stdin/stdout over multiple TCP connections.
//============================================================================

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std;

#define RBUF_SIZE  5 /*(1024*1024*10)*/
#define STDIN_FDI 0
#define STDOUT_FDI 1
#define MAX_CONNCECTIONS 8

struct ppipe {
	char *buf ;
	int head ;
	int tail ;
	int read_fdi[MAX_CONNCECTIONS] ;
	int num_read_connections ;
	int write_fdi[MAX_CONNCECTIONS] ;
	int num_write_connections ;
};

int main(int argc, char *argv[]) {

	if( argc < 4 ) {
		fprintf(stderr,"Usage: %s <proxy ip> <proxy port> <source IP 1> [<source IP 2> ... <SOURCE IP n>]\n", argv[0]) ;
		return 1 ;
	}

	char *hostname = argv[1] ;
	char **sIps = argv + 3 ;
	int numSIps = argc -3 ;

	int port = atoi(argv[2]) ;

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
	struct pollfd fds[numfds] ;

	fds[STDIN_FDI].fd = STDIN_FILENO ;
	fds[STDIN_FDI].events = POLLIN ;
	fds[STDOUT_FDI].fd = STDOUT_FILENO ;
	fds[STDOUT_FDI].events = 0 ;

	for(int i=0 ; i < numSIps ; i++ ) {
		int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if( sock < 0 ) {
			perror("ERROR: could not open socket") ;
			exit(1) ;
		}

		// Bind to the source address:
		struct sockaddr_in bind_addr ;
		bind_addr.sin_port = htons(0) ; // any port

		if( !inet_aton(sIps[i],&bind_addr.sin_addr) ) {
			fprintf(stderr, "Invalid source address : %s\n", sIps[i]) ;
			exit(1) ;
		}
		bind(sock, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) ;

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

    while(1) {

    	int np;
		if( (np = poll( fds, numfds, -1)) < 0 && errno != EINTR ) {
			perror("poll error") ;
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

			if( revents & POLLIN ) {
				struct ppipe *pipe ;
				if( STDIN_FDI == i ) {
					pipe = &toProxy ;
				} else {
					pipe = &fromProxy ;
				}


		      int maxRead = (RBUF_SIZE-1) - ((RBUF_SIZE + pipe->tail - pipe->head) % RBUF_SIZE);
			  if( pipe->tail + maxRead > RBUF_SIZE ) {
				  maxRead = RBUF_SIZE - pipe->tail ;
			  }

			  int n = read(fds[i].fd, &pipe->buf[pipe->tail], maxRead ) ;
			  if( n < 1 ) {
				  perror("read failed") ;
				  exit( 1 ) ;
			  }
			  pipe->tail = (pipe->tail + n) % RBUF_SIZE ;

			  // Make sure we're registered to see when we can write:
			  for(int index = 0 ; index < pipe->num_write_connections  ; index ++ ) {
				  fds[pipe->write_fdi[index]].events |= POLLOUT ;
			  }

			  // If our buf is full, don't read any more until its got some room:
			  if( ((pipe->tail+1) % RBUF_SIZE) == pipe->head ) {
				  fds[i].events &= ~POLLIN ;
			  }
			}

			if( revents & POLLOUT ) {
				struct ppipe *pipe ;
				if( STDOUT_FDI == i ) {
					pipe = &fromProxy ;
				} else {
					pipe = &toProxy ;
				}

				int maxWrite = (RBUF_SIZE + pipe->tail - pipe->head) % RBUF_SIZE ;
				if( pipe->head + maxWrite >= RBUF_SIZE ) {
					maxWrite = RBUF_SIZE - pipe->head ;
				}

				int n = write(fds[i].fd, &pipe->buf[pipe->head], maxWrite  ) ;
				if( n < 1 ) {
					perror("write failed") ;
					exit( 1 ) ;
				}
				pipe->head = (pipe->head + n) % RBUF_SIZE ;

				// Make sure we're registered to see what can be read:
				for(int index = 0 ; index < pipe->num_read_connections  ; index ++ ) {
					fds[pipe->read_fdi[index]].events |= POLLIN ;
				}

				// If we have nothing left to write, de-register from writable event
				if(pipe->head == pipe->tail) {
					fds[i].events &= ~POLLOUT ;
				}
			}
    	}
    }



	return 0;
}
