# Makefile for C program to read and process a TMG file

PROGRAM=pthreadclosest-bygraph
CFILES=tmggraph.c stringfuncs.c sll.c timer.c $(PROGRAM).c
OFILES=$(CFILES:.c=.o)
CC=gcc
CFLAGS=-g

$(PROGRAM):	$(OFILES)
	$(CC) $(CFLAGS) -o $(PROGRAM) $(OFILES) -lreadline -lm -lpthread

clean::
	/bin/rm $(PROGRAM) $(OFILES)
