#include "list_result.h"

#define bufferRows 1024*1024/8

void insert(result *Result,int key1,int key2){  //negative numbers?
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
      Result->Tail->pos++;
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
  node->next = NULL;
  return node;
}

result* result_init(){
  // arxikopohsh result
  result *Result=malloc(sizeof(result));
  Result->size = 0;
  Result->Head = NULL;
  Result->Tail = NULL;
  return Result;
}

void result_print(result *Result){
  int i=0,j;
  result_node *tmp=Result->Head;
  while ( tmp!=NULL ){
    printf("---------------node %d--------------------\n",i);
    printf("tmp pos %d\n",tmp->pos );
    for ( j=0; j<tmp->pos; j++ ){
      printf("index in array %d, elements %d %d\n",j,tmp->buffer[0][j],tmp->buffer[1][j]);
    }
    tmp = tmp->next;
    i++;
  }
}

void result_free(result *Result){
  if ( Result->Head==NULL ){
    return;
  }
  result_node *tmp=Result->Head;
  while ( tmp!=NULL ){
    Result->Head = tmp->next;
    result_node_free(tmp);
    Result->size --;
    tmp = Result->Head;
  }
  free(Result);
}

void result_node_free(result_node *tmp){
  int i;
  for ( i=0; i<2; i++ ){
    free(tmp->buffer[i]);
  }
  free(tmp->buffer);
  free(tmp);
}
