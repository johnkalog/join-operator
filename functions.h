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

typedef struct result{
  int buffer[bufferRows][2];  //h sto heap?
  struct result *next;
}result;

typedef struct resultInfo{
  int size;
  result *Head;
}resultInfo;

void relation_creation(relation *A,relation *B,char *argv[]);
void free_memory(relation *A);

result* RadixHashJoin(relation *relR, relation *relS);
