#include <stdio.h>
#include <stdlib.h>
#include "hash.h"

int main(int argc, char *argv[])
{
  if ( argc<3 ){
    printf("Less arguments\n");
    return 1;
  }
  else if ( argc>3 ){
    printf("More argments\n");
    return 1;
  }
  relation *A=malloc(sizeof(relation)),*B=malloc(sizeof(relation));
  relation_creation(A,B,argv);

  //relation_print(A);
  //relation_print(B);
  result *Result=RadixHashJoin(A,B);

  free_memory(A);
  free_memory(B);

  //result_print(Result);
  result_free(Result);
  return 0;
}
