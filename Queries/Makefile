OBJS     = main.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o Full_Relation.o Sql_queries.o Sql_functions.o ThreadFunctions.o
OUT      = run
CC       = gcc
FLAGS    = -g -c -lm
# -g for  debugging  mode ; -c for  objects  file  creation

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@ -lm

main.o:main.c
	$(CC) $(FLAGS) main.c

list_result.o:../RHJ/list_result.c
	$(CC) $(FLAGS) ../RHJ/list_result.c

RadixHashJoin.o:../RHJ/RadixHashJoin.c
	$(CC) $(FLAGS) ../RHJ/RadixHashJoin.c

RadixFunctions.o:../RHJ/RadixFunctions.c
	$(CC) $(FLAGS) ../RHJ/RadixFunctions.c

Relation.o:../RHJ/Relation.c
	$(CC) $(FLAGS) ../RHJ/Relation.c

Full_Relation.o:Full_Relation.c
	$(CC) $(FLAGS) Full_Relation.c

Sql_queries.o:Sql_queries.c
	$(CC) $(FLAGS) Sql_queries.c

Sql_functions.o:Sql_functions.c
	$(CC) $(FLAGS) Sql_functions.c

ThreadFunctions.o:../RHJ/ThreadFunctions.c
	$(CC) $(FLAGS) ../RHJ/ThreadFunctions.c

clean:
	rm -f $(OBJS) $(OUT)
