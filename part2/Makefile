OBJS     = main.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o Full_Relation.o Sql_queries.o
OUT      = run
CC       = gcc
FLAGS    = -g -c
# -g for  debugging  mode ; -c for  objects  file  creation

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@

main.o:main.c
	$(CC) $(FLAGS) main.c

list_result.o:../part1/list_result.c
	$(CC) $(FLAGS) ../part1/list_result.c

RadixHashJoin.o:../part1/RadixHashJoin.c
	$(CC) $(FLAGS) ../part1/RadixHashJoin.c

RadixFunctions.o:../part1/RadixFunctions.c
	$(CC) $(FLAGS) ../part1/RadixFunctions.c

Relation.o:../part1/Relation.c
	$(CC) $(FLAGS) ../part1/Relation.c

Full_Relation.o:Full_Relation.c
	$(CC) $(FLAGS) Full_Relation.c

Sql_queries.o:Sql_queries.c
	$(CC) $(FLAGS) Sql_queries.c

clean:
	rm -f $(OBJS) $(OUT)
