OBJS     = main.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o ThreadFunctions.o JobSheduler.o Threads.o
OUT      = run
CC       = gcc
FLAGS    = -g -c -lm -pthread
# -g for  debugging  mode ; -c for  objects  file  creation

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@ -lm -pthread

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

ThreadFunctions.o:ThreadFunctions.c
	$(CC) $(FLAGS) ThreadFunctions.c

JobSheduler.o:JobSheduler.c
	$(CC) $(FLAGS) JobSheduler.c

Threads.o:Threads.c
	$(CC) $(FLAGS) Threads.c

clean:
	rm -f $(OBJS) $(OUT)
