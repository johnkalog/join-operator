#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char *argv[]) //creation of filename that conations num random numbers euros?
{
  int num=atoi(argv[2]);
  char *filename=argv[1];
  FILE *fp=fopen(filename,"w");
  srand(time(NULL));
  for ( int i=0; i<num; i++ ){
    fprintf(fp,"%d\n",rand());
  }
  fclose(fp);
  return 0;
}
