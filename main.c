#include <stdio.h>
#include <stdlib.h>
#include "functions.h"

int main(int argc, char *argv[])
{
  relation *A=malloc(sizeof(relation)),*B=malloc(sizeof(relation));
  relation_creation(A,B,argv);
  free_memory(A,B);
  return 0;
}
