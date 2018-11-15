#include "hash.h"

void relation_creation(relation *A,relation *B,char *argv[]){ //diabasma apo arxeio twn payload
  int num1=0,num2=0,tmp,i;
  char *filename1=argv[1],*filename2=argv[2];
  FILE *fp1=fopen(filename1,"r"),*fp2=fopen(filename2,"r");
  if ( fp1==NULL || fp2==NULL ){
    printf("Error in fopen\n");
    exit(1);
  }
  while ( fscanf(fp1,"%d",&tmp)!=EOF ) {
    num1 ++;
  }
  while ( fscanf(fp2,"%d",&tmp)!=EOF ) {
    num2 ++;
  }
  fseek(fp1, 0, SEEK_SET);  //sthn arxh tou areiou
  fseek(fp2, 0, SEEK_SET);
  //----------------------------A relation---------------------------------
  A->num_tuples = num1;
  A->tuples = malloc(A->num_tuples*sizeof(tuple));
  // while ( fscanf(fp1,"%d",&tmp)!=EOF ) {
  //   printf("%d\n",tmp);
  // }
  for ( i=0; i<A->num_tuples; i++ ){
    A->tuples[i].key = i+1;  //aritmhsh apo 1
    fscanf(fp1,"%ld",&A->tuples[i].payload);
  }
  //---------------------------B relation------------------------------------
  B->num_tuples = num2;
  B->tuples = malloc(B->num_tuples*sizeof(tuple));
  for ( i=0; i<B->num_tuples; i++ ){
    B->tuples[i].key = i+1;
    fscanf(fp2,"%ld",&B->tuples[i].payload);
  }
  // for ( i=0; i<A->num_tuples; i++ ){
  //   printf("%d %d\n",A->tuples[i].key,A->tuples[i].payload);
  // }
  fclose(fp1);
  fclose(fp2);
}

void relation_print(relation *A) {
  //printf("num of tuples %d\n",A->num_tuples);
  int i;
  for(i=0;i<A->num_tuples;i++) {
    printf("%d -/-/- %ld \n",A->tuples[i].key,A->tuples[i].payload );
  }
}

void free_memory(relation *A){
  free(A->tuples);
  free(A);
}
