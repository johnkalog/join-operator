#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include "../RHJ/hash.h"

typedef struct statistics{
  uint64_t min;
  uint64_t max;
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

void print_relation(full_relation);

void free_structs(full_relation *,unsigned int);
