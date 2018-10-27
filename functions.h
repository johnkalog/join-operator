#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

typedef struct tuple{
  int32_t key;
  int32_t payload;
} tuple;

typedef struct relation{
  tuple *tuples;
  uint32_t num_tuples;
} relation;

void relation_creation(relation *A,relation *B,char *argv[]);
void free_memory(relation *A,relation *B);
