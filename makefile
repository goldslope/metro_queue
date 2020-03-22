CC=gcc
CXX=g++ -std=c++14
RM=rm -f
#CPPFLAGS=-g $(shell root-config --cflags)
#LDFLAGS=-g $(shell root-config --ldflags)
#LDLIBS=$(shell root-config --libs)

SRCS=test.cpp
OBJS=$(subst .cpp,.o,$(SRCS))

all: clean test

test: $(OBJS)
	$(CXX) $(LDFLAGS) -o test $(OBJS) $(LDLIBS)

test.o: $(SRCS)

clean:
	$(RM) $(OBJS)
