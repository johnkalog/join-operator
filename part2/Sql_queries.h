#include "Full_Relation.h"

typedef struct condition{
  int relA,relB,operator;
}condition;

typedef struct selectors{
  int rel,column;
}selectors;

void sql_queries(char *,full_relation *);
