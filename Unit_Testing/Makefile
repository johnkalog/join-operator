OBJS     = testprogram.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o Full_Relation.o Sql_queries.o Sql_functions.o ThreadFunctions.o Threads.o statistics.o
OUT      = run
CC       = gcc
FLAGS    = -g -c -lm -pthread
# -g for  debugging  mode ; -c for  objects  file  creation

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@ -lm -pthread -lcunit

testprogram.o:testprogram.c
	$(CC) $(FLAGS) testprogram.c

list_result.o:../RHJ/list_result.c
	$(CC) $(FLAGS) ../RHJ/list_result.c

RadixHashJoin.o:../RHJ/RadixHashJoin.c
	$(CC) $(FLAGS) ../RHJ/RadixHashJoin.c

RadixFunctions.o:../RHJ/RadixFunctions.c
	$(CC) $(FLAGS) ../RHJ/RadixFunctions.c

statistics.o:../Queries/statistics.c
	$(CC) $(FLAGS) ../Queries/statistics.c

Threads.o:../RHJ/Threads.c
	$(CC) $(FLAGS) ../RHJ/Threads.c

ThreadFunctions.o:../RHJ/ThreadFunctions.c
	$(CC) $(FLAGS) ../RHJ/ThreadFunctions.c

Relation.o:../RHJ/Relation.c
	$(CC) $(FLAGS) ../RHJ/Relation.c

Full_Relation.o:../Queries/Full_Relation.c
	$(CC) $(FLAGS) ../Queries/Full_Relation.c

Sql_queries.o:../Queries/Sql_queries.c
	$(CC) $(FLAGS) ../Queries/Sql_queries.c

Sql_functions.o:../Queries/Sql_functions.c
	$(CC) $(FLAGS) ../Queries/Sql_functions.c

clean:
	rm -f $(OBJS) $(OUT)
