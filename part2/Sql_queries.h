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

typedef struct list {
    int val;
    struct list *next;
} list;

void sql_queries(char *,full_relation *);

full_relation **string2rel_pointers(full_relation *,char *,int *);

predicate *string2predicate(char *,int *);

point *string2rel_selection(char *,int *);

int findNextPredicate(predicate*,int,list *);

void push_list(list **,int);

int search_list(list *,int);

int empty_list(list *);

void freeList(list *);

void calculate_metric(predicate *,full_relation **);

full_relation *subcpy_full_relation(full_relation **,int );
