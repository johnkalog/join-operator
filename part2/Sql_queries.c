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

            for ( i=0; i<condition_num; i++ ) {
              printf("rel_predicate i operation : %c left: %d,%d and the flag %d\n",rel_predicate[i].operation,rel_predicate[i].left.row,rel_predicate[i].left.column,rel_predicate[i].flag);
            }
            for(i=0;i<condition_num;i++){
                //free(rel_condition[i]);
            }
            free(rel_predicate);
            //-----------------------------select------------------------------
            int selection_num;
            point *rel_selection=string2rel_selection(tok3,&selection_num);
            printf("Num of selection %d\n",selection_num);
            for ( i=0; i<selection_num; i++ ) {
              printf("row %d column %d\n",rel_selection[i].row,rel_selection[i].column);
            }
            free(rel_selection);
            //int relAindex=atoi(argv[3]),relBindex=atoi(argv[4]),relAcol=atoi(argv[5]),relBcol=atoi(argv[6]);
            //result *Result=RHJcaller(relations_array,relAindex,relBindex,relAcol,relBcol);
            // result_print(Result);
            //
            // result_free(Result);
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

  char **rel_condition = NULL;
  rel_condition = malloc((*condition_num)*sizeof(char *));
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

    char *rest;
    char *tok = strtok_r(rel_condition[i],&rel_predicate[i].operation,&rest);
    if(strchr(tok,'.')!=NULL) {
      rel_predicate[i].left.row = atoi(strtok(tok,"."));
      rel_predicate[i].left.column = atoi(strtok(NULL,"."));
      rel_predicate[i].flag = 0;
    }
    else {
      rel_predicate[i].number = atoi(tok);
      rel_predicate[i].flag = 1;
    }

    if(strchr(rest,'.')!=NULL) {
      tok = strtok(rest,".");
      rel_predicate[i].right.row = atoi(tok);
      //tok = strtok(NULL,".");
      rel_predicate[i].right.column = atoi(strtok(NULL,"."));

      //rel_predicate[i].flag = 1;
    }
    else {
      rel_predicate[i].number = atoi(rest);
      rel_predicate[i].flag = 2;
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
