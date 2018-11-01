#include "functions.h"

void relation_creation(relation *A,relation *B,char *argv[]){
  int num1=0,num2=0,tmp,i;
  char *filename1=argv[1],*filename2=argv[2];
  FILE *fp1=fopen(filename1,"r"),*fp2=fopen(filename2,"r");
  while ( fscanf(fp1,"%d",&tmp)!=EOF ) {
    num1 ++;
  }
  while ( fscanf(fp2,"%d",&tmp)!=EOF ) {
    num2 ++;
  }
  fseek(fp1, 0, SEEK_SET);
  fseek(fp2, 0, SEEK_SET);
  //----------------------------A relation---------------------------------
  A->num_tuples = num1;
  A->tuples = malloc(A->num_tuples*sizeof(tuple));
  // while ( fscanf(fp1,"%d",&tmp)!=EOF ) { h for?
  //   printf("%d\n",tmp);
  // }
  for ( i=0; i<A->num_tuples; i++ ){
    A->tuples[i].key = i+1;  //aritmhsh apo 1?
    fscanf(fp1,"%d",&A->tuples[i].payload);
  }
  //---------------------------B relation------------------------------------
  B->num_tuples = num2;
  B->tuples = malloc(B->num_tuples*sizeof(tuple));
  for ( i=0; i<B->num_tuples; i++ ){
    B->tuples[i].key = i+1;
    fscanf(fp2,"%d",&B->tuples[i].payload);
  }
  // for ( i=0; i<A->num_tuples; i++ ){
  //   printf("%d %d\n",A->tuples[i].key,A->tuples[i].payload);
  // }
  fclose(fp1);
  fclose(fp2);
}

void free_memory(relation *A){
  free(A->tuples);
  free(A);
}

void insert(result *Result,int key1,int key2){
  if ( Result->Head==NULL ){
    Result->Head = node_init();
    Result->Head->buffer[0][Result->Head->pos] = key1;
    Result->Head->buffer[1][Result->Head->pos] = key2;
    Result->Head->pos ++;
    Result->Tail = Result->Head;
    Result->size ++;
  }
  else{
    if ( Result->Tail->pos<bufferRows ){
      Result->Tail->buffer[0][Result->Tail->pos] = key1;
      Result->Tail->buffer[1][Result->Tail->pos] = key2;
    }
    else{
      Result->Tail->next = node_init();
      Result->Tail->next->buffer[0][Result->Tail->next->pos] = key1;
      Result->Tail->next->buffer[1][Result->Tail->next->pos] = key2;
      Result->Tail->next->pos ++;
      Result->size ++;
      Result->Tail = Result->Tail->next;
    }
  }
}

result_node *node_init(){
  int i;
  result_node *node=malloc(sizeof(result_node));
  node->buffer = malloc(2*sizeof(int *));
  for ( i=0; i<2; i++ ){
    node->buffer[i] = malloc(bufferRows*sizeof(int));
  }
  node->pos = 0;
  return node;
}

result* result_init(){
  result *Result=malloc(sizeof(result));
  Result->size = 0;
  Result->Head = NULL;
  Result->Tail = NULL;
  return Result;
}
