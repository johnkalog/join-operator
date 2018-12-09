#include <stdlib.h>
#include "Sql_queries.h"
#define bufferRows 1024*1024/8 //plhthos eggrafwn result_node
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

void sql_queries(char *filepath,full_relation *relations_array){
    char *line=NULL,*query=NULL;
    size_t len = 0;
    ssize_t nread;
    FILE *fp;
    fp=fopen(filepath,"r");
    if (fp == NULL) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    int count=0;
    list *adds=NULL;  //list for results print

    while((nread = getline(&line, &len, fp)) != -1){
        if(line[0]=='F'){
            print_list(adds);
            if(adds!=NULL)      freeList(adds);

            //printf("It's F, new batch!\n");
            adds=NULL;  //list for results print

        }
        else{
            //printf("///////////////////////%s",line );
            char *tok1 = strtok(line,"|");
            char *tok2 = strtok(NULL,"|");
            char *tok3 = strtok(NULL,"|");

            int i;
            //----------------------------from--------------------------
            int rel_num;
            full_relation **rel_pointers=string2rel_pointers(relations_array,tok1,&rel_num);
            for ( i=0; i<rel_num; i++ ) {
              //printf("relation number columns: %ld\n",rel_pointers[i]->my_metadata.num_columns);

            }
            full_relation *cpy_tuple_array = subcpy_full_relation(rel_pointers,rel_num);
            free(rel_pointers);
            //--------------------------where--------------------------------
            int condition_num;
            predicate *rel_predicate = string2predicate(tok2,&condition_num);

            for(i=0;i<condition_num;i++){
              calculate_metric(&rel_predicate[i],cpy_tuple_array);
             // printf("]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]metric here is: %d\n",rel_predicate[i].metric);
            }
            list *head = NULL;

            int **keys;
            keys = malloc(rel_num*sizeof(int*));
            for(i=0;i<rel_num;i++) {
              keys[i] = NULL;
            }
            int cur_size = 0;
             for ( i=0; i<condition_num; i++ ) {
               int best_pos = findNextPredicate(rel_predicate,condition_num,head);
               //best_pos = i;
               // printf("best next pos is %d\n",best_pos );
               if(rel_predicate[best_pos].flag == 0) {
                 if ( rel_predicate[best_pos].left.row==rel_predicate[best_pos].right.row){
                    if(keys[rel_predicate[best_pos].left.row] == NULL) {
                      result *Result=Simple_Scan_Tables(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     if(Result->size == 0 ) {
                       int k;
                       for(k=0;k<rel_num;k++) {
                         if(keys[k] != NULL) {
                           free(keys[k]);
                           keys[k] = NULL;
                         }
                       }
                       result_free(Result);
                       break;
                     }
                     cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result_free(Result);
                   }
                   else {
                     relation *new_rel = keys2relation(keys[rel_predicate[best_pos].left.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column]);
                     relation *new_rel2 = keys2relation(keys[rel_predicate[best_pos].right.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     result *Result=Simple_Scan_Tables(new_rel,new_rel2);
                     if(Result->size == 0 ) {
                       int k;
                       for(k=0;k<rel_num;k++) {
                         if(keys[k] != NULL) {
                           free(keys[k]);
                           keys[k] = NULL;
                         }
                       }
                       free(new_rel->tuples);
                       free(new_rel);
                       free(new_rel2->tuples);
                       free(new_rel2);
                       result_free(Result);
                       break;
                     }
                     cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                     free(new_rel2->tuples);
                     free(new_rel2);
                   }
                 }
                 else{
                 //printf("%d.%d %c %d.%d\n",rel_predicate[best_pos].left.row,rel_predicate[best_pos].left.column,rel_predicate[best_pos].operation,rel_predicate[best_pos].right.row,rel_predicate[best_pos].right.column);
                   if(keys[rel_predicate[best_pos].left.row] == NULL && keys[rel_predicate[best_pos].right.row] == NULL) {
                     printf("BOTH NULL\n" );
                     result *Result=RadixHashJoin(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     if(Result->size == 0 ) {
                       printf("EMPTY?\n" );
                       if(Result->size == 0 ) {
                         int k;
                         for(k=0;k<rel_num;k++) {
                           if(keys[k] != NULL) {
                             free(keys[k]);
                             keys[k] = NULL;
                           }
                         }
                       }
                       result_free(Result);
                       break;
                     }
                     cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                     //printf("size result is %d\n",cur_size );
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);

                     result_free(Result);
                   }
                   else if(keys[rel_predicate[best_pos].left.row] == NULL && keys[rel_predicate[best_pos].right.row] != NULL) {
                     //printf("LEFT NULL\n" );
                     relation *new_rel = keys2relation(keys[rel_predicate[best_pos].right.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     result *Result=RadixHashJoin(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],new_rel);
                     if(Result->size == 0 ) {
                       int k;
                       for(k=0;k<rel_num;k++) {
                         if(keys[k] != NULL) {
                           free(keys[k]);
                           keys[k] = NULL;
                         }
                       }
                       result_free(Result);
                       free(new_rel->tuples);
                       free(new_rel);
                       break;
                     }
                     cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);


                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                   }
                   else if(keys[rel_predicate[best_pos].left.row] != NULL && keys[rel_predicate[best_pos].right.row] == NULL){
                     //printf("RIGHT NULL\n" );
                     relation *new_rel = keys2relation(keys[rel_predicate[best_pos].left.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column]);
                     result *Result=RadixHashJoin(new_rel,&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     if(Result->size == 0 ) {
                       int k;
                       for(k=0;k<rel_num;k++) {
                         if(keys[k] != NULL) {
                           free(keys[k]);
                           keys[k] = NULL;
                         }
                       }
                       result_free(Result);
                       free(new_rel->tuples);
                       free(new_rel);
                       break;
                     }
                     cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);


                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                   }
                   else if(keys[rel_predicate[best_pos].left.row] != NULL && keys[rel_predicate[best_pos].right.row] != NULL) {
                     printf("NONE NULL\n" );
                   }
                }
               }
               else if(rel_predicate[best_pos].flag == 1) {
                 //printf("%d.%d %c %d\n",rel_predicate[best_pos].left.row,rel_predicate[best_pos].left.column,rel_predicate[best_pos].operation,rel_predicate[best_pos].number);
                 result *Result=Simple_Scan(&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column],rel_predicate[best_pos].number,rel_predicate[best_pos].operation);
                 if(Result->size == 0 ) {
                   int k;
                   for(k=0;k<rel_num;k++) {
                     if(keys[k] != NULL) {
                       free(keys[k]);
                       keys[k] = NULL;
                     }
                   }
                   result_free(Result);
                   break;
                 }
                 cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                 result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);
                 result_free(Result);
               }
               else if(rel_predicate[best_pos].flag == 2) {
                 //printf("%d.%d %c %d\n",rel_predicate[best_pos].left.row,rel_predicate[best_pos].left.column,rel_predicate[best_pos].operation,rel_predicate[best_pos].number);
                 //printf("op: %c number %d\n",rel_predicate[best_pos].operation,rel_predicate[best_pos].number );
                 if(keys[rel_predicate[best_pos].left.row] == NULL) {
                   result *Result=Simple_Scan(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],rel_predicate[best_pos].number,rel_predicate[best_pos].operation);
                   if(Result->size == 0 ) {
                     int k;
                     for(k=0;k<rel_num;k++) {
                       if(keys[k] != NULL) {
                         free(keys[k]);
                         keys[k] = NULL;
                       }
                     }
                     result_free(Result);
                     break;
                   }
                   cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                   result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                   result_free(Result);
                 }
                 else {
                   relation *new_rel = keys2relation(keys[rel_predicate[best_pos].left.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column]);
                   result *Result=Simple_Scan(new_rel,rel_predicate[best_pos].number,rel_predicate[best_pos].operation);
                   if(Result->size == 0 ) {
                     int k;
                     for(k=0;k<rel_num;k++) {
                       if(keys[k] != NULL) {
                         free(keys[k]);
                         keys[k] = NULL;
                       }
                     }
                     free(new_rel->tuples);
                     free(new_rel);
                     result_free(Result);
                     break;
                   }
                   cur_size = (Result->size-1)*bufferRows + Result->Tail->pos;
                   result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                   result_free(Result);
                   free(new_rel->tuples);
                   free(new_rel);
                 }
               }
               if(rel_predicate[best_pos].flag == 0) {
                 push_list(&head,rel_predicate[best_pos].left.row);
                 push_list(&head,rel_predicate[best_pos].right.row);
               }
               else if(rel_predicate[best_pos].flag == 1) {
                 push_list(&head,rel_predicate[best_pos].right.row);
               }
               else {
                 push_list(&head,rel_predicate[best_pos].left.row);
               }
               rel_predicate[best_pos].metric = -1;
             }

            free(rel_predicate);
            freeList(head);
            //-----------------------------select------------------------------
            int selection_num;
            point *rel_selection=string2rel_selection(tok3,&selection_num);
            //printf("Num of selection %d\n",selection_num);
             uint64_t add;
             //printf("Selection num %d\n",selection_num );
            for(i=0;i<selection_num;i++) {
            //   // printf("row %d column %d\n",rel_selection[i].row,rel_selection[i].column);
              add = calculate_sum(keys,cur_size,cpy_tuple_array,rel_selection[i].row,rel_selection[i].column);
              //printf("add is %d\n",add );
               push_list2(&adds,add,1);
            //  // printf("the sum %ld\n",add);
             }
             push_list2(&adds,1,-1);           //  ----->  -1 gia allgh grammhs
            printf("\n");
            //freeList(adds);
            free(rel_selection);
            free_structs(cpy_tuple_array,rel_num);

            for(i=0;i<rel_num;i++) {
              if(keys[i] != NULL) {
                free(keys[i]);
              }
            }
            free(keys);
            //exit(1);
        }
    }
    free(line);
    fclose(fp);
}

void result2keys(result *Result,int **keys,int row,int index,int rel_num) {
  int size = (Result->size-1)*bufferRows + Result->Tail->pos; // Error-fixed

  if(keys[row] == NULL) {
    //printf("row %d doesn't exist yet\n",row );
    keys[row] = malloc(size*sizeof(int));

    int pos = 0;
    result_node *tmp=Result->Head;
    while ( tmp!=NULL ){
      //printf("---------------node %d--------------------\n",i);
      //printf("tmp pos %d\n",tmp->pos );
      int j;
      for ( j=0; j<tmp->pos; j++ ){
        //printf("In node: %d, with index in array %d, elements %d %d\n",row,j,tmp->buffer[index][j],tmp->buffer[index][j]);
        keys[row][pos] = tmp->buffer[index][j];
        pos++;
      }
      tmp = tmp->next;
    }
  }
  else {

    int **new_keys = malloc(rel_num*sizeof(int*));
    int i;
    for(i=0;i<rel_num;i++) {
      if(keys[i] != NULL) {
        new_keys[i] = malloc(size*sizeof(int));
      }
    }
    int pos = 0;
    //printf("size is %d\n",size );
    result_node *tmp=Result->Head;
    while ( tmp!=NULL ){
      //printf("---------------node %d--------------------\n",i);
      //printf("tmp pos %d\n",tmp->pos );
      int j;
      for ( j=0; j<tmp->pos; j++ ){
        //printf("In node: %d, with index in array %d, elements %d %d\n",i,j,tmp->buffer[0][j],tmp->buffer[1][j]);
        //int row_i;
        for(i=0;i<rel_num;i++) {
          if(keys[i] != NULL) {
            //printf("pos is %d\n",pos );
            new_keys[i][pos] = keys[i][tmp->buffer[index][j]-1];
          }
          else {
            new_keys[i] = NULL;
          }
        }
        pos++;
      }
      tmp = tmp->next;
    }
    for(i=0;i<rel_num;i++) {
      free(keys[i]);
      keys[i] = new_keys[i];
    }
    free(new_keys);
  } //else
}

relation *keys2relation(int *keys,int size,relation *OldRelation) {
  relation *new_rel = malloc(sizeof(relation));
  new_rel->num_tuples = size;
  new_rel->tuples = malloc(size*sizeof(tuple));
  int i;
  for(i=0;i<size;i++) {
    new_rel->tuples[i].key = i+1; // gia na kseroume to endiameso index
    new_rel->tuples[i].payload = OldRelation->tuples[keys[i]-1].payload;
  }
  return new_rel;
}


predicate *string2predicate(char* str,int *condition_num) {

  char *cp_tok2 = strdup(str);

  *condition_num=0;
  char *condition=strtok(cp_tok2,"&");
  while ( condition!=NULL ) {
    (*condition_num)++;
    condition = strtok(NULL,"&");
  }

  predicate *rel_predicate = malloc((*condition_num)*sizeof(predicate));
  bzero(rel_predicate,(*condition_num)*sizeof(predicate));

  char **rel_condition = NULL;
  rel_condition = malloc((*condition_num)*sizeof(char *));
  //bzero(rel_condition,(*condition_num)*sizeof(char *));
  int i;

  condition = strtok(str,"&");
  for ( i=0; i<(*condition_num); i++ ) {
    if ( condition==NULL ){
      printf("condition not NULL\n");
    }
    rel_condition[i] = strdup(condition);
    //printf("rel_condition %s\n",rel_condition[i] );

    condition = strtok(NULL,"&");
  }

  for ( i=0; i<(*condition_num); i++ ) {
    if(strchr(rel_condition[i],'=')!=NULL) {
      rel_predicate[i].operation = '=';
    }
    else if(strchr(rel_condition[i],'<')!=NULL) {
      rel_predicate[i].operation = '<';
    }
    else if(strchr(rel_condition[i],'>')!=NULL) {
      rel_predicate[i].operation = '>';
    }
    else{
      printf("Wrong operation \n");
    }

    char *rest=NULL;
    char *tok = strtok_r(rel_condition[i],&rel_predicate[i].operation,&rest);
    if(strchr(tok,'.')!=NULL) {
      rel_predicate[i].left.row = atoi(strtok(tok,"."));
      rel_predicate[i].left.column = atoi(strtok(NULL,"."));
      rel_predicate[i].flag = 0;
      rel_predicate[i].metric = 0;
    }
    else {
      rel_predicate[i].number = atoi(tok);
      rel_predicate[i].flag = 1;
      rel_predicate[i].metric = 1000;
    }

    if(strchr(rest,'.')!=NULL) {
      tok = strtok(rest,".");
      rel_predicate[i].right.row = atoi(tok);
      //tok = strtok(NULL,".");
      rel_predicate[i].right.column = atoi(strtok(NULL,"."));

      //rel_pointers pointers idios deikths//

    }
    else {
      rel_predicate[i].number = atoi(rest);
      //printf("rest is %d\n",rel_predicate[i].number );
      rel_predicate[i].flag = 2;
      rel_predicate[i].metric = 1000;
    }

  }

  for(i=0;i<*condition_num;i++){
      free(rel_condition[i]);
  }

  free(cp_tok2);
  free(rel_condition);

  return rel_predicate;
}

void result2relation(result *Result,full_relation *cpy_tuple_array,predicate *rel_predicate) {
  int left_row = rel_predicate->left.row;
  int right_row = rel_predicate->right.row;
  //printf("Result->size: %d\n",Result->size );
  if(Result->size == 0) {
    cpy_tuple_array[left_row].my_metadata.num_tuples = 0;
    cpy_tuple_array[right_row].my_metadata.num_tuples = 0;
    free(cpy_tuple_array[left_row].my_relations[0].tuples);
    free(cpy_tuple_array[right_row].my_relations[0].tuples);
    free(cpy_tuple_array[left_row].my_relations);
    free(cpy_tuple_array[right_row].my_relations);
    free(cpy_tuple_array[left_row].my_metadata.statistics_array);
    free(cpy_tuple_array[right_row].my_metadata.statistics_array);
    //printf("EMPTY\n" );
    return;
  }
  //printf("Result->Tail->pos: %d\n",Result->Tail->pos );
  int size = (Result->size-1)*bufferRows + Result->Tail->pos; // Error-fixed
  //printf("size is %d\n",size );
  tuple *cpy_tuple_array_left = malloc(size*cpy_tuple_array[left_row].my_metadata.num_columns*sizeof(tuple));
  tuple *cpy_tuple_array_right = malloc(size*cpy_tuple_array[right_row].my_metadata.num_columns*sizeof(tuple));

  cpy_tuple_array[left_row].my_metadata.num_tuples = size;
  cpy_tuple_array[right_row].my_metadata.num_tuples = size;


  //printf("left: %d ,right: %d\n",left_row,right_row );

  int i=0,j;
  int pos_left = 0;
  int pos_right = 0;
  result_node *tmp=Result->Head;
  while ( tmp!=NULL ){
    //printf("---------------node %d--------------------\n",i);
    //printf("tmp pos %d\n",tmp->pos );
    for ( j=0; j<tmp->pos; j++ ){
      //printf("In node: %d, with index in array %d, elements %d %d\n",i,j,tmp->buffer[0][j],tmp->buffer[1][j]);
      int c;
      for(c=0;c<cpy_tuple_array[left_row].my_metadata.num_columns;c++) {

        cpy_tuple_array_left[c*size+pos_left].payload = cpy_tuple_array[left_row].my_relations[c].tuples[tmp->buffer[0][j]-1].payload;
        cpy_tuple_array_left[c*size+pos_left].key = pos_left+1;
        cpy_tuple_array[left_row].my_relations[c].num_tuples = size;
      }
      pos_left++;
      for(c=0;c<cpy_tuple_array[right_row].my_metadata.num_columns;c++) {
        cpy_tuple_array_right[c*size+pos_right].payload = cpy_tuple_array[right_row].my_relations[c].tuples[tmp->buffer[1][j]-1].payload;
        cpy_tuple_array_right[c*size+pos_right].key = pos_right+1;
        cpy_tuple_array[right_row].my_relations[c].num_tuples = size;
      }
      pos_right++;

    }
    tmp = tmp->next;
    i++;
  }
  free(cpy_tuple_array[left_row].my_relations[0].tuples);
  free(cpy_tuple_array[right_row].my_relations[0].tuples);

  int l;
  for ( l=0; l<cpy_tuple_array[left_row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[left_row].my_relations[l].num_tuples = size;
    cpy_tuple_array[left_row].my_relations[l].tuples = &cpy_tuple_array_left[l*size];
  }

  for ( l=0; l<cpy_tuple_array[left_row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[left_row].my_metadata.statistics_array[l].min = calculate_min(cpy_tuple_array[left_row].my_relations[l].tuples,cpy_tuple_array[left_row].my_metadata.num_tuples);
    cpy_tuple_array[left_row].my_metadata.statistics_array[l].max = calculate_max(cpy_tuple_array[left_row].my_relations[l].tuples,cpy_tuple_array[left_row].my_metadata.num_tuples);
    //printf("column %d left min %ld max %ld\n",l,cpy_tuple_array[left_row].my_metadata.statistics_array[l].min,cpy_tuple_array[left_row].my_metadata.statistics_array[l].max);
  }

  for ( l=0; l<cpy_tuple_array[right_row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[right_row].my_relations[l].num_tuples = size;
    cpy_tuple_array[right_row].my_relations[l].tuples = &cpy_tuple_array_right[l*size];
  }

  for ( l=0; l<cpy_tuple_array[right_row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[right_row].my_metadata.statistics_array[l].min = calculate_min(cpy_tuple_array[right_row].my_relations[l].tuples,cpy_tuple_array[right_row].my_metadata.num_tuples);
    cpy_tuple_array[right_row].my_metadata.statistics_array[l].max = calculate_max(cpy_tuple_array[right_row].my_relations[l].tuples,cpy_tuple_array[right_row].my_metadata.num_tuples);
    //printf("column %d right min %ld max %ld\n",l,cpy_tuple_array[right_row].my_metadata.statistics_array[l].min,cpy_tuple_array[right_row].my_metadata.statistics_array[l].max);
  }
}

void result2relation_simple(result *Result,full_relation *cpy_tuple_array,int row){
  if(Result->size == 0) {
    cpy_tuple_array[row].my_metadata.num_tuples = 0;
    free(cpy_tuple_array[row].my_relations[0].tuples);
    free(cpy_tuple_array[row].my_relations);
    free(cpy_tuple_array[row].my_metadata.statistics_array);
    //printf("EMPTY\n" );
    return;
  }
  int size = (Result->size-1)*bufferRows + Result->Tail->pos; // Error-fixed
  //printf("size is %d\n",size );
  tuple *cpy_tuple_array_inside = malloc(size*cpy_tuple_array[row].my_metadata.num_columns*sizeof(tuple));

  cpy_tuple_array[row].my_metadata.num_tuples = size;

  int i=0,j,pos=0;
  result_node *tmp=Result->Head;
  while ( tmp!=NULL ){
    //printf("---------------node %d--------------------\n",i);
    //printf("tmp pos %d\n",tmp->pos );
    for ( j=0; j<tmp->pos; j++ ){
      //printf("In node: %d, with index in array %d, elements %d %d\n",i,j,tmp->buffer[0][j],tmp->buffer[1][j]);
      int c;
      for(c=0;c<cpy_tuple_array[row].my_metadata.num_columns;c++) {

        cpy_tuple_array_inside[c*size+pos].payload = cpy_tuple_array[row].my_relations[c].tuples[tmp->buffer[0][j]-1].payload;
        cpy_tuple_array_inside[c*size+pos].key = pos+1;
        cpy_tuple_array[row].my_relations[c].num_tuples = size;
      }
      pos++;

    }
    tmp = tmp->next;
    i++;
  }
  free(cpy_tuple_array[row].my_relations[0].tuples);

  int l;
  for ( l=0; l<cpy_tuple_array[row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[row].my_relations[l].num_tuples = size;
    cpy_tuple_array[row].my_relations[l].tuples = &cpy_tuple_array_inside[l*size];
  }

  for ( l=0; l<cpy_tuple_array[row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[row].my_metadata.statistics_array[l].min = calculate_min(cpy_tuple_array[row].my_relations[l].tuples,cpy_tuple_array[row].my_metadata.num_tuples);
    cpy_tuple_array[row].my_metadata.statistics_array[l].max = calculate_max(cpy_tuple_array[row].my_relations[l].tuples,cpy_tuple_array[row].my_metadata.num_tuples);
    //printf("column %d left min %ld max %ld\n",l,cpy_tuple_array[row].my_metadata.statistics_array[l].min,cpy_tuple_array[row].my_metadata.statistics_array[l].max);
  }

}


full_relation *subcpy_full_relation(full_relation **rel_pointers,int rel_num){
  full_relation *relations_cpy = malloc(rel_num*sizeof(full_relation));

  int i;
  for(i=0;i<rel_num;i++) {
    relations_cpy[i].my_metadata.num_tuples = rel_pointers[i]->my_metadata.num_tuples;
    relations_cpy[i].my_metadata.num_columns = rel_pointers[i]->my_metadata.num_columns;

    relations_cpy[i].my_metadata.statistics_array = malloc(relations_cpy[i].my_metadata.num_columns*sizeof(statistics));
    int j;
    for(j=0;j<relations_cpy[i].my_metadata.num_columns;j++) {
      relations_cpy[i].my_metadata.statistics_array[j].min = rel_pointers[i]->my_metadata.statistics_array[j].min;
      relations_cpy[i].my_metadata.statistics_array[j].max = rel_pointers[i]->my_metadata.statistics_array[j].max;
    }

    // copy relations

    relations_cpy[i].my_relations =  malloc(relations_cpy[i].my_metadata.num_columns*sizeof(relation));
    tuple *cpy_tuple_array = malloc(relations_cpy[i].my_metadata.num_tuples*relations_cpy[i].my_metadata.num_columns*sizeof(tuple));
    int k;
    for(k=0;k<relations_cpy[i].my_metadata.num_columns;k++) {
      for(j=0;j<relations_cpy[i].my_metadata.num_tuples;j++) {
        cpy_tuple_array[(k*relations_cpy[i].my_metadata.num_tuples)+j].key = rel_pointers[i]->my_relations[k].tuples[j].key;
        cpy_tuple_array[(k*relations_cpy[i].my_metadata.num_tuples)+j].payload = rel_pointers[i]->my_relations[k].tuples[j].payload;
      }
    }

    for(k=0;k<relations_cpy[i].my_metadata.num_columns;k++) {
      relations_cpy[i].my_relations[k].num_tuples = relations_cpy[i].my_metadata.num_tuples;
      relations_cpy[i].my_relations[k].tuples =   &cpy_tuple_array[k*relations_cpy[i].my_relations[k].num_tuples];
    }

  }


  return relations_cpy;
}




full_relation **string2rel_pointers(full_relation *relations_array,char *tok1,int *rel_num){
  int i;
  *rel_num = 0;
  char *cp_tok1 = strdup(tok1);
  full_relation **rel_pointers=NULL;
  char *rel_id=strtok(cp_tok1," ");
  while ( rel_id!=NULL ) {
    (*rel_num) ++;
    rel_id = strtok(NULL," ");
  }
  free(cp_tok1);
  rel_pointers = malloc((*rel_num)*sizeof(full_relation *));
  bzero(rel_pointers,(*rel_num)*sizeof(full_relation *));
  rel_id=strtok(tok1," ");
  for ( i=0; i<(*rel_num); i++ ) {
    rel_pointers[i] = &relations_array[atoi(rel_id)];
    rel_id = strtok(NULL," ");
  }
  return rel_pointers;
}

point *string2rel_selection(char *tok3,int *selection_num){
  int i;
  *selection_num = 0;
  char *cp_tok3 = strdup(tok3);
  char *selection=strtok(cp_tok3," ");
  while ( selection!=NULL ) {
    (*selection_num) ++;
    selection = strtok(NULL," ");
  }
  free(cp_tok3);
  point *rel_selection = malloc((*selection_num)*sizeof(point));
  selection = strtok(tok3," ");
  for ( i=0; i<(*selection_num); i++ ) {
    rel_selection[i].row = atoi(&selection[0]);
    rel_selection[i].column = atoi(&selection[2]);
    selection = strtok(NULL," ");
  }

  return rel_selection;
}

void calculate_metric(predicate *the_predicate,full_relation *subcpy_full_relation){

    int min,max;
    double tmp=1;
    if ( the_predicate->flag==0 ){


      if ( the_predicate->left.row==the_predicate->right.row){
    //    printf("same-----------------------\n");
        the_predicate->metric += 500;
        return;
      }
      else {
        int min1 = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].min;
        int max1 = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].max;
        int min2 = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].min;
        int max2 = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].max;
        tmp = (double)(MIN(max1,max2)-MAX(min1,min2)) / ((double)(MAX(max1,max2)-MIN(min1,min2)));
    //    printf("min1: %d,min2: %d,max1: %d,max2: %d\n",min1,min2,max1,max2 );
    //    printf("tmp for join is %f\n",tmp );
      }
    }
    else{
      if ( the_predicate->flag==1 ){
        min = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].min;
        max = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].max;
    //    printf("min: %d ,max: %d ,number: %d\n",min,max,the_predicate->number );
        if ( the_predicate->operation=='>' ){
          tmp = (double)(the_predicate->number-min)/(double)(max-min);
          if ( tmp<=0 ){  //kanena stoixeio den pernaei elegexos eta?
            tmp = 1;
          }
          else if( tmp>1 ){
            tmp = 0;
            return;
          }
        }
        else if ( the_predicate->operation=='<' ){
          tmp = (double)(the_predicate->number-min)/(double)(max-min);
          if ( tmp>=1 ){
            tmp = 1;
          }
          else if( tmp<0 ){
            tmp = 0;
            return;
          }
        }
      }
      else if ( the_predicate->flag==2 ){
        //printf("left row :%d left column:%d number:%d flag:%d operation%c\n",the_predicate->left.row,the_predicate->left.column,the_predicate->number,the_predicate->flag,the_predicate->operation);
        min = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].min;
        max = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].max;
       // printf("min: %d ,max: %d ,number: %d ",min,max,the_predicate->number );
        if ( the_predicate->operation=='>' ){
          tmp = (double)(the_predicate->number-min)/(double)(max-min);
    //      printf(",tmp: %f\n",tmp );
          if ( tmp>=1 ){
            tmp = 1;
          }
          else if( tmp<0 ){
            tmp = 0;
            return;
          }
        }
        else if ( the_predicate->operation=='<'){
          tmp = (double)(the_predicate->number-min)/(double)(max-min);
    //      printf(",tmp: %f\n",tmp );
          if ( tmp<=0 ){
            tmp = 1;
          }
          else if( tmp>1 ){
            tmp = 0;
            return;
          }
        }
      }
    }

    //printf("2  the_predicate->metri %d\n",the_predicate->metric );
    int x = (1.0-tmp)*500;
    //printf("x is %d\n",x );
    the_predicate->metric += x;
//  }
}

uint64_t calculate_sum(int **keys,int size,full_relation *cpy_tuple_array,int row,int column){
  if(keys[row] == NULL){
    // printf("ffffffffffffffffffffff %ld\n",cpy_tuple_array[row].my_relations[0].num_tuples);
    return 0;
  }
  int i;
  relation *rel = keys2relation(keys[row],size,&cpy_tuple_array[row].my_relations[column]);
  uint64_t sum=0;
  for ( i=0; i<size; i++ ){
    sum += rel->tuples[i].payload;
  }
  free(rel->tuples);
  free(rel);
  return sum;
}
