# Makefile for LMDB with Spreads extensions
# win target to build on Windows with MinGW
# lin target to build for Linux from WSL
CC = gcc
AR	= ar
W	= -W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized
THREADS = -pthread
OPT ?= -O2 -g #-DNDEBUG
#XCFLAGS = -DMDB_VL32 -D_FILE_OFFSET_BITS=64
CFLAGS = -DMDB_DEBUG=0 -DMDB_MAXKEYSIZE=0 $(THREADS) $(OPT) $(W) $(XCFLAGS) 
LDFLAGS = 
LDLIBS	= 
SOLIBS	= 
SOURCES = spreadsdb.c
HEADERS = spreadsdb.h
CFLAGS += -I.

NAME = libspreads_lmdb
SOEXT	= .so

# Add LMDB files
HEADERS += $(wildcard lmdb/libraries/liblmdb/*.h)
CFLAGS += -Ilmdb/libraries/liblmdb
SOURCES += lmdb/libraries/liblmdb/mdb.c lmdb/libraries/liblmdb/midl.c

LIBOBJECTS = $(SOURCES:.c=.o)

all: $(NAME)$(SOEXT)

clean:
	rm -rf *.exe *.dll *.lib *.def *.dylib *.[ao] *.[ls]o $(LIBOBJECTS)
	
$(NAME).dll :	$(SOURCES)
	$(CC)  $(CFLAGS) -fPIC $(LDFLAGS) -shared -Wl,--output-def=$(NAME).def -Wl,--out-implib=$(NAME).lib $(SOURCES) -o $@ $(LDLIBS) $(THREADS)

$(NAME).so  :	$(SOURCES)
	$(CC)  $(CFLAGS) -fPIC $(LDFLAGS) -shared  $(SOURCES) -o $@ $(LDLIBS) $(THREADS)

$(NAME).dylib  :	$(SOURCES)
	$(CC)  $(CFLAGS) -fPIC $(LDFLAGS) -shared $(SOURCES) -o $@ $(LDLIBS) $(THREADS)
