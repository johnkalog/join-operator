run:main.o functions.o RadixHashJoin.o
	gcc -g -o run main.o functions.o RadixHashJoin.o -lm

main.o:main.c
	gcc -c main.c

functions.o:functions.c
	gcc -c functions.c

RadixHashJoin.o:RadixHashJoin.c
	gcc -c RadixHashJoin.c

clean:
	rm -f ./run ./main.o ./functions.o ./RadixHashJoin.o
