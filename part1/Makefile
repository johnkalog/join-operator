OBJS     = main.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o
OUT      = run
CC       = gcc
FLAGS    = -g -c
# -g for  debugging  mode ; -c for  objects  file  creation

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@

main.o:main.c
	$(CC) $(FLAGS) main.c

list_result.o:list_result.c
	$(CC) $(FLAGS) list_result.c

RadixHashJoin.o:RadixHashJoin.c
	$(CC) $(FLAGS) RadixHashJoin.c

RadixFunctions.o:RadixFunctions.c
	$(CC) $(FLAGS) RadixFunctions.c

Relation.o:Relation.c
	$(CC) $(FLAGS) Relation.c

clean:
	rm -f $(OBJS) $(OUT)
