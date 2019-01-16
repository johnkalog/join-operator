#include "Full_Relation.h"

typedef struct point{
  int row;
  int column;
}point;

typedef struct predicate{
  point left; //index ston pinaka rel_predicate
  point right;
  char operation; //>,<,=
  int metric;
  int flag; //compare with join 0,number 1 if in left or 2
  int number;
}predicate;

typedef struct list {
    uint64_t val;
    int flag; // -1 gia allgh grammhs
    struct list *next;
} list;

void sql_queries(char *,full_relation *);

full_relation **string2rel_pointers(full_relation *,char *,int *);

predicate *string2predicate(char *,int *);

point *string2rel_selection(char *,int *);

int findNextPredicate(predicate*,int,list *);

void push_list(list **,uint64_t);

void push_list2(list **,uint64_t,int);

int search_list(list *,int);

int empty_list(list *);

void freeList(list *);

void print_list(list *);

void calculate_metric(predicate *,full_relation *);

full_relation *subcpy_full_relation(full_relation **,int );

metadata *metadata_array_creation(full_relation **,int);

void free_metadata_array(metadata *,int);

uint64_t calculate_sum(int **,int,full_relation *,int,int);

relation *keys2relation(int *,int ,relation *);

void result2keys(result *,int **,int ,int ,int );

int check_if_in(metadata *,predicate *,relation *,uint64_t);

uint64_t update_metadata_array(metadata *,predicate *);
