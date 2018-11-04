run:main.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o
	gcc -g -o run main.o list_result.o RadixHashJoin.o RadixFunctions.o Relation.o -lm

main.o:main.c
	gcc -c main.c

list_result.o:list_result.c
	gcc -c list_result.c

RadixHashJoin.o:RadixHashJoin.c
	gcc -c RadixHashJoin.c

RadixFunctions.o:RadixFunctions.c
	gcc -c RadixFunctions.c

Relation.o:Relation.c
	gcc -c Relation.c

clean:
	rm -f ./run ./main.o ./list_result.o ./RadixHashJoin.o ./RadixFunctions.o ./Relation.o
