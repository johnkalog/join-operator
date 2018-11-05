#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char *argv[]) //creation of filename that conations num random numbers between specific range argv 3,4
{
  if ( argc<5 ){
    printf("Less arguments\n");
    return 1;
  }
  else if ( argc>5 ){
    printf("More argments\n");
    return 1;
  }
  int num=atoi(argv[2]),min=atoi(argv[3]),max=atoi(argv[4]);
  char *filename=argv[1];
  FILE *fp=fopen(filename,"w");
  if ( fp==NULL ){
    printf("Error in fopen\n");
    exit(1);
  }
  srand(time(NULL));
  for ( int i=0; i<num; i++ ){
    fprintf(fp,"%d\n",rand()%(max-min+1)+min);
  }
  fclose(fp);
  return 0;
}
