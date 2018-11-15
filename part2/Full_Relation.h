
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include "../part1/hash.h"

typedef struct metadata{
  uint64_t num_tuples,num_columns;
  //statistics
} metadata;

typedef struct full_relation{
  metadata my_metadata;
  relation *my_relations;
} full_relation;

unsigned int line_count(FILE *);


full_relation *full_relation_creation(char *,unsigned int *);


void print_relation(full_relation);

result* RHJcaller(full_relation *,int,int,int,int,int);

void free_structs(full_relation *,unsigned int);
