#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include "../RHJ/ThreadFunctions.h"

#define N 50000000

typedef struct statistics{
  uint64_t min;
  uint64_t max;
  uint64_t f;
  int count;
}statistics;

typedef struct metadata{
  uint64_t num_tuples,num_columns;
  statistics *statistics_array; //gia kathe column statistics
} metadata;

typedef struct full_relation{
  metadata my_metadata;
  relation *my_relations;
} full_relation;

unsigned int line_count(FILE *);

full_relation *full_relation_creation(char *,unsigned int *);

uint64_t calculate_min(tuple *,int);

uint64_t calculate_max(tuple *,int);

int calculate_distinct(tuple *,int,uint64_t,uint64_t);

void print_relation(full_relation);

void free_structs(full_relation *,unsigned int);
