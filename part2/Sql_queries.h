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
  int flag; //compare with join 0 number 1 if in left or 2
  int number;
}predicate;

void sql_queries(char *,full_relation *);

full_relation **string2rel_pointers(full_relation *,char *,int *);

predicate *string2predicate(char *,int *);

point *string2rel_selection(char *,int *);

void calculate_metric(predicate,full_relation **);
