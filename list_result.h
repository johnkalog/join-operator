#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>


typedef struct result_node{
  int **buffer,pos;  // 2d array me ta rowids
  struct result_node *next;
}result_node;

typedef struct result{
  int size; // arithmos kombwn
  result_node *Head,*Tail; // arxh kai telos listas
}result;


result* result_init();
void insert(result *,int,int);
result_node *node_init();
void result_print(result *);
void result_free(result *);
void result_node_free(result_node *);
