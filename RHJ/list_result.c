#include "list_result.h"

#define bufferRows 1024*1024/8  //plhthos eggrafwn result_node

void insert(result *Result,int key1,int key2){
  if ( Result->Head==NULL ){  //kanenas kombos sthn lista
    Result->Head = node_init();
    Result->Head->buffer[0][Result->Head->pos] = key1;
    Result->Head->buffer[1][Result->Head->pos] = key2;
    Result->Head->pos ++;
    Result->total_records ++;
    Result->Tail = Result->Head;
    Result->size ++;
  }
  else{
    if ( Result->Tail->pos<bufferRows ){  //an o pinakas tou kombou exei gemisei
      Result->Tail->buffer[0][Result->Tail->pos] = key1;
      Result->Tail->buffer[1][Result->Tail->pos] = key2;
      Result->Tail->pos++;
      Result->total_records ++;
    }
    else{
      Result->Tail->next = node_init();
      Result->Tail->next->buffer[0][Result->Tail->next->pos] = key1;
      Result->Tail->next->buffer[1][Result->Tail->next->pos] = key2;
      Result->Tail->next->pos ++;
      Result->total_records ++;
      Result->size ++;
      Result->Tail = Result->Tail->next;
    }
  }
}

result_node *node_init(){ //arxikopoihsh result_node
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
  Result->total_records = 0;
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
      printf("In node: %d, with index in array %d, elements %d %d\n",i,j,tmp->buffer[0][j],tmp->buffer[1][j]);
    }
    tmp = tmp->next;
    i++;
  }
}

void result_free(result *Result){
  if ( Result->Head==NULL ){
    free(Result);
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
