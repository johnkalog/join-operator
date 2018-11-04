#include <stdio.h>
#include <stdlib.h>
#include "hash.h"

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
