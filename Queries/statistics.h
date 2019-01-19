#include "Sql_queries.h"

// intermidiate_results exei mia lista me visited
//ta statistika metadata kai to a8roisma twn intermmediate
typedef struct intermidiate_results {
  list *visited;
  int cur_sum;
  metadata *I_metadata;
} intermidiate_results;

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))


int *enumeration(predicate *,int,full_relation **,int);

int allready_inside(int *,int ,int );

uint64_t update_metadata_array(metadata *,predicate *);

intermidiate_results *cpy_intermmediate(intermidiate_results *,int);

metadata *cpy_metadata(metadata *,int);

int findNextPredicate(predicate*,int,list *);

uint64_t calculate_sum(int **,int,full_relation *,int,int);

void calculate_metric(predicate *,full_relation *);
