#include <stdlib.h>
#include "Sql_queries.h"
#define bufferRows 1024*1024/8  //plhthos eggrafwn result_node
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
    while((nread = getline(&line, &len, fp)) != -1){
        if(line[0]=='F'){
            printf("It's F, new batch!\n");
        }
        else{
            printf("///////////////////////%s",line );
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
             for ( i=0; i<condition_num; i++ ) {
               int best_pos = findNextPredicate(rel_predicate,condition_num,head);
               //printf("rel_predicate i operation : %c left: %d,%d and the flag %d\n",rel_predicate[best_pos].operation,rel_predicate[best_pos].left.row,rel_predicate[best_pos].left.column,rel_predicate[best_pos].flag);
              // printf("best next pos is %d\n",best_pos );
               if(rel_predicate[best_pos].flag == 0) {
                // if ( count<5 ){
                 result *Result=RadixHashJoin(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                 //result_print(Result);
                 result2relation(Result,cpy_tuple_array,&rel_predicate[best_pos]);
                 result_free(Result);
                // count ++;
                //}
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
            for ( i=0; i<selection_num; i++ ) {
              //printf("row %d column %d\n",rel_selection[i].row,rel_selection[i].column);
            }
            free(rel_selection);
            free_structs(cpy_tuple_array,rel_num);
            //exit(1);
        }
    }
    free(line);
    fclose(fp);
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


  printf("left: %d ,right: %d\n",left_row,right_row );

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
  //
  // for ( l=0; l<cpy_tuple_array[left_row].my_metadata.num_columns; l++ ){
  //   cpy_tuple_array[left_row].my_metadata.statistics_array[l].min = calculate_min(cpy_tuple_array[left_row].my_relations[l].tuples,cpy_tuple_array[left_row].my_metadata.num_tuples);
  //   cpy_tuple_array[left_row].my_metadata.statistics_array[l].max = calculate_max(cpy_tuple_array[left_row].my_relations[l].tuples,cpy_tuple_array[left_row].my_metadata.num_tuples);
  // }

  for ( l=0; l<cpy_tuple_array[right_row].my_metadata.num_columns; l++ ){
    cpy_tuple_array[right_row].my_relations[l].num_tuples = size;
    cpy_tuple_array[right_row].my_relations[l].tuples = &cpy_tuple_array_right[l*size];
  }
  //
  // for ( l=0; l<cpy_tuple_array[right_row].my_metadata.num_columns; l++ ){
  //   cpy_tuple_array[right_row].my_metadata.statistics_array[l].min = calculate_min(cpy_tuple_array[right_row].my_relations[l].tuples,cpy_tuple_array[right_row].my_metadata.num_tuples);
  //   cpy_tuple_array[right_row].my_metadata.statistics_array[l].max = calculate_max(cpy_tuple_array[right_row].my_relations[l].tuples,cpy_tuple_array[right_row].my_metadata.num_tuples);
  // }



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
    //    printf("min: %d ,max: %d ,number: %d ",min,max,the_predicate->number );
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
