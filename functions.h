#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define bufferRows 1024*1024/8

typedef struct result_node{
  int **buffer,pos;  //h sto heap?
  struct result_node *next;
}result_node;

typedef struct result{
  int size;
  result_node *Head,*Tail;
}result;


result* result_init();
void insert(result *,int,int);
result_node *node_init();
void result_print(result *);
void result_free(result *);
void result_node_free(result_node *);
