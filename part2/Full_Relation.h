
#include <string.h>
#include "../src/hash.h"

typedef struct metadata{
  int num_tuples,num_columns;
  //statistics
} metadata;

typedef struct full_relation{
  metadata my_metadata;
  relation *my_relations;
} full_relation;

unsigned int line_count(FILE *);

void full_relation_creation(full_relation *s_array,tuple **tuple_arrays,char *argv[],unsigned int *num_lines);

void free_structs(full_relation *,tuple**,unsigned int);
