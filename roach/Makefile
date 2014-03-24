KATCP ?= ../katcp

include ../Makefile.inc

INC = -I$(KATCP)
LIB = -L$(KATCP) -lkatcp
BUILD = unknown-0.1

EXE = new-client-roach client-roach server-roach 

all: $(EXE)

new-client-example: new-client-roach.c
	$(CC) -DBUILD=\"$(BUILD)\" $(CFLAGS) $(INC) -o $@ $^ $(LIB)

client-example: client-roach.c
	$(CC) -DBUILD=\"$(BUILD)\" $(CFLAGS) $(INC) -o $@ $^ $(LIB)

server-example: server-roach.c
	$(CC) -DBUILD=\"$(BUILD)\" $(CFLAGS) $(INC) -o $@ $^ $(LIB)

clean: 
	$(RM) $(EXE)

install: $(EXE)
	$(ECHO) "not installing any!"

