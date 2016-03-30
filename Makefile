#CC=g++
CC=g++-5
#CFLAGS=-Wall -g -std=c++0x
CFLAGS=-Wall -g -std=c++14

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
#INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./mica/src

#CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -O3
CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Wno-unused-function -O3
#LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++0x -O3 -ljemalloc
#LDFLAGS = -Wall -L. -L./libs -L./mica/build -pthread -g -lrt -std=c++14 -lcommon -lnuma -ljemalloc -O3
LDFLAGS = -Wall -L. -L./libs -L./mica/build -pthread -g -lrt -std=c++14 -lcommon -lnuma -O3
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS) ./mica/build/libcommon.a
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)
