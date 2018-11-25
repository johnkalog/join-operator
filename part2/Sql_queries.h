#include "Full_Relation.h"

typedef struct point{
  int row;
  int column;
}point;

typedef struct predicate{
  point left;
  point right;
  char operation;
  int metric;
  int flag; //compare with number 0 or join 1
}predicate;

void sql_queries(char *,full_relation *);

full_relation **string2rel_pointers(full_relation *,char *);
