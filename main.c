#include <stdio.h>
#include <stdlib.h>
#include "functions.h"
#include "hash.h"
void relation_print(relation *);

int main(int argc, char *argv[])
{
  relation *A=malloc(sizeof(relation)),*B=malloc(sizeof(relation));
  relation_creation(A,B,argv);

  //relation_print(A);
  result *Result=RadixHashJoin(A,B);

  free_memory(A);
  free_memory(B);

  result_print(Result);
  result_free(Result);
  return 0;
}


void relation_print(relation *A) {
  //printf("num of tuples %d\n",A->num_tuples);
  int i;
  for(i=0;i<A->num_tuples;i++) {
    printf("%d -/-/- %d \n",A->tuples[i].key,A->tuples[i].payload );
  }

}
