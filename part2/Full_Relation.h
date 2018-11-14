#include "../src/hash.h"

typedef struct metadata{
  int num_tuples,num_columns;
  //statistics
} metadata;

typedef struct full_relation{
  metadata my_metadata;

} full_relation;

unsigned int line_count(FILE *);
