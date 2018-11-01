#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>


#define bufferRows 1024*1024/8

typedef struct tuple{
  int32_t key;
  int32_t payload;
} tuple;

typedef struct relation{
  tuple *tuples;
  uint32_t num_tuples;
} relation;

typedef struct result_node{
  int **buffer,pos;  //h sto heap?
  struct result_node *next;
}result_node;

typedef struct result{
  int size;
  result_node *Head,*Tail;
}result;

void relation_creation(relation *A,relation *B,char *argv[]);
void free_memory(relation *A);
result* result_init();
void insert(result *,int,int);
result_node *node_init();

result* RadixHashJoin(relation *relR, relation *relS);
