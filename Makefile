run:main.o functions.o
	gcc -g -o run main.o functions.o -lm

main.o:main.c
	gcc -c main.c

functions.o:functions.c
	gcc -c functions.c

clean:
	rm -f ./run ./main.o ./functions.o
