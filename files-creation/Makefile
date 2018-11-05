OBJS     = file_creation.o
OUT      = create
CC       = gcc
FLAGS    = -g -c
# -g for  debugging  mode ; -c for  objects  file  creation

$(OUT): $(OBJS)
	$(CC) -g $(OBJS) -o $@

file_creation.o:file_creation.c
	$(CC) $(FLAGS) file_creation.c

clean:
	rm -f $(OBJS) $(OUT)
