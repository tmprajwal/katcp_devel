KATCP ?= ../katcp

include ../Makefile.inc

INC = -I$(KATCP)
LIB = -L$(KATCP) -lkatcp
BUILD = roach

EXE = new-client-example client-example server-example 

all: $(EXE)

new-client-example: new-client-example.c
	$(CC) -DBUILD=\"$(BUILD)\" $(CFLAGS) $(INC) -o $@ $^ $(LIB)

client-example: client-example.cxx
	$(CC) -DBUILD=\"$(BUILD)\" $(CFLAGS) $(INC) -o $@ $^ $(LIB)

server-example: server-example.c
	$(CC) -DBUILD=\"$(BUILD)\" $(CFLAGS) $(INC) -o $@ $^ $(LIB)

clean: 
	$(RM) $(EXE)

install: $(EXE)
	$(ECHO) "not installing examples"
