# Makefile for LMDB with Spreads extensions
# win target to build on Windows with MinGW
# lin target to build for Linux from WSL
CC = gcc  # clang++, g++ or just gcc if not compiling Snappy (C++ code)
AR	= ar
W	= -W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized
THREADS = -pthread
OPT ?= -O2 -g #-DNDEBUG
#XCFLAGS = -DMDB_VL32 -D_FILE_OFFSET_BITS=64
CFLAGS = -DMDB_MAXKEYSIZE=0 $(THREADS) $(OPT) $(W) $(XCFLAGS) 
LDFLAGS = 
LDLIBS	= 
SOLIBS	= 
SOURCES = spreadsdb.c
HEADERS = spreadsdb.h
CFLAGS += -I.

NAME = libspreads_lmdb
LIBRARY = $(NAME).a
SHARED_W = $(NAME).dll
SHARED_L = $(NAME).so

# Add LMDB files
HEADERS += $(wildcard ../../lmdb/libraries/liblmdb/*.h)
CFLAGS += -I../../lmdb/libraries/liblmdb
SOURCES += ../../lmdb/libraries/liblmdb/mdb.c ../../lmdb/libraries/liblmdb/midl.c

LIBOBJECTS = $(SOURCES:.c=.o)

all: $(SHARED_W)

clean:
	rm -rf *.exe *.dll *.lib *.def *.[ao] *.[ls]o $(LIBOBJECTS)
	
win : $(SHARED_W)
	
$(SHARED_W) :	$(SOURCES)
	$(CC)  $(CFLAGS) -fPIC $(LDFLAGS) -shared -Wl,--output-def=$(NAME).def -Wl,--out-implib=$(NAME).lib -Wl,-soname -Wl,--dll $(SOURCES) -o $@ $(LDLIBS) $(THREADS) -s 
	
lin : $(SHARED_L)

$(SHARED_L) :	$(SOURCES)
	$(CC)  $(CFLAGS) -fPIC $(LDFLAGS) -shared -Wl,-soname -Wl,--dll $(SOURCES) -o $@ $(LDLIBS) $(THREADS) -s 
	

	
	