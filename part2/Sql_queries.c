#include <stdlib.h>
#include "Sql_queries.h"

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
              printf("relation number columns: %ld\n",rel_pointers[i]->my_metadata.num_columns);

            }
            free(rel_pointers);
            //--------------------------where--------------------------------
            int condition_num;
            predicate* rel_predicate = string2predicate(tok2,&condition_num);

            list *head = NULL;
            for ( i=0; i<condition_num; i++ ) {
              printf("rel_predicate i operation : %c left: %d,%d and the flag %d\n",rel_predicate[i].operation,rel_predicate[i].left.row,rel_predicate[i].left.column,rel_predicate[i].flag);
              int best_pos = findNextPredicate(rel_predicate,condition_num,head);
              printf("best next pos is %d\n",best_pos );

              if(rel_predicate[best_pos].flag == 0) {
                push_list(&head,rel_predicate[best_pos].left.row);
                push_list(&head,rel_predicate[best_pos].right.row);
              }
              else if(rel_predicate[best_pos].flag == 1) {
                push_list(&head,rel_predicate[best_pos].left.row);
              }
              else {
                push_list(&head,rel_predicate[best_pos].right.row);
              }
              rel_predicate[best_pos].metric = -1;
            }
            for(i=0;i<condition_num;i++){
                //free(rel_condition[i]);
            }
            for(i=0;i<condition_num;i++){
                //calculate_metric(rel_predicate[i],rel_pointers);
                printf("]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]metric here is: %d\n",rel_predicate[i].metric);
            }
            free(rel_predicate);
            freeList(head);
            //-----------------------------select------------------------------
            int selection_num;
            point *rel_selection=string2rel_selection(tok3,&selection_num);
            printf("Num of selection %d\n",selection_num);
            for ( i=0; i<selection_num; i++ ) {
              printf("row %d column %d\n",rel_selection[i].row,rel_selection[i].column);
            }
            free(rel_selection);
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
    printf("rel_condition %s\n",rel_condition[i] );

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

void calculate_metric(predicate the_predicate,full_relation **rel_pointers){
  int tmp,min,max;
  if ( the_predicate.flag==0 ){
    // if ( rel_pointers[the_predicate.left.row]==rel_pointers[the_predicate.right.row] ){
    //   printf("same----------------------\n");
    //   the_predicate.metric += 500;
    // }
  }
  else{
    if ( the_predicate.flag==1 ){
      min = rel_pointers[the_predicate.right.row]->my_metadata.statistics_array[the_predicate.right.column].min;
      max = rel_pointers[the_predicate.right.row]->my_metadata.statistics_array[the_predicate.right.column].max;
      if ( the_predicate.operation=='>' ){
        tmp = (the_predicate.number-min)/(max-min);
        if ( tmp<=0 ){  //kanena stoixeio den pernaei elegexos meta?
          tmp = 1;
        }
        else if( tmp>1 ){
          tmp = 0;
          return;
        }
      }
      else if ( the_predicate.operation=='<' ){
        tmp = (max-the_predicate.number)/(max-min);
        if ( tmp>=1 ){
          tmp = 1;
        }
        else if( tmp<0 ){
          tmp = 0;
          return;
        }
      }
    }
    else if ( the_predicate.flag==2 ){
      printf("left row :%d left column:%d number:%d flag:%d operation%c\n",the_predicate.left.row,the_predicate.left.column,the_predicate.number,the_predicate.flag,the_predicate.operation);
      min = rel_pointers[the_predicate.left.row]->my_metadata.statistics_array[the_predicate.left.column].min;
      max = rel_pointers[the_predicate.left.row]->my_metadata.statistics_array[the_predicate.left.column].max;
    //   if ( the_predicate.operation=='>' ){
    //     tmp = (max-the_predicate.number)/(max-min);
    //     if ( tmp>=1 ){
    //       tmp = 1;
    //     }
    //     else if( tmp<0 ){
    //       tmp = 0;
    //       return;
    //     }
    //   }
    //   else if ( the_predicate.operation=='<'){
    //     tmp = (the_predicate.number-min)/(max-min);
    //     if ( tmp<=0 ){
    //       tmp = 1;
    //     }
    //     else if( tmp>1 ){
    //       tmp = 0;
    //       return;
    //     }
    //   }
     }
  }
  the_predicate.metric += (1.0/(double)tmp)*500.0;

}
