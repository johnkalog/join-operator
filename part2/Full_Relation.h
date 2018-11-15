
#include <string.h>
#include "../src/hash.h"

typedef struct metadata{
  uint64_t num_tuples,num_columns;
  //statistics
} metadata;

typedef struct full_relation{
  metadata my_metadata;
  relation *my_relations;
} full_relation;

unsigned int line_count(FILE *);

void full_relation_creation(full_relation *,char *,unsigned int *);

void print_relation(full_relation);

void free_structs(full_relation *,unsigned int);
