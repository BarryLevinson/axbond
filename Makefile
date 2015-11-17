CFLAGS=-O3 -Wall

all: axbond

axbond: axbond.cpp axbond.h log.cpp log.h
	$(CXX) $(CFLAGS) -o axbond axbond.cpp log.cpp

clean:
	$(RM) axbond
