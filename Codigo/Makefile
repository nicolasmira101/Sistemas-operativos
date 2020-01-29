# Proyecto 2 - Makefile para generar 3 ejecutables

all: analogp analogh manager

analogp: analogp.c
	gcc -o analogp analogp.c -w -lm

analogh: analogh.c 
	gcc -o analogh analogh.c -w -lpthread -lm

manager: manager.c
	gcc -o manager manager.c -w -lm

clean:
	rm analogp analogh manager
