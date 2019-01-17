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

            adds=NULL;

        }
        else{ //gia kathe query
            char *tok1 = strtok(line,"|");
            char *tok2 = strtok(NULL,"|");
            char *tok3 = strtok(NULL,"|");

            int i;
            //----------------------------from--------------------------
            int rel_num;
            full_relation **rel_pointers=string2rel_pointers(relations_array,tok1,&rel_num);  //pinakas deitkwv sthn arxikh domh
                                                                    // twn sxesewn pou uparxoun sto query
            // for ( i=0; i<rel_num; i++ ) {
            //   printf("relation number columns: %ld\n",rel_pointers[i]->my_metadata.num_columns);
            // }
            full_relation *cpy_tuple_array = subcpy_full_relation(rel_pointers,rel_num);  //upopinakas ths arxikhs domhs mono twn sxesewn tou
                                                                                //rel_pointers

            //metadata *metadata_array=metadata_array_creation(rel_pointers,rel_num);

            //--------------------------where--------------------------------
            int condition_num;
            predicate *rel_predicate = string2predicate(tok2,&condition_num);



            int *best_order = enumeration(rel_predicate,condition_num,rel_pointers,rel_num);

            for(i=0;i<condition_num;i++) {
              printf("%d\n",best_order[i] );
            }


            for(i=0;i<condition_num;i++){
              calculate_metric(&rel_predicate[i],cpy_tuple_array);  //upologismos metrikhs timhs
            }
            list *head = NULL;  //gia thn findNextPredicate oses sxesies exoun xrhsimopoihthei

            int **keys; //pinakas endiameswn apotelesamtwn
            keys = malloc(rel_num*sizeof(int*));
            for(i=0;i<rel_num;i++) {
              keys[i] = NULL;
            }
            int cur_size = 0;
             for ( i=0; i<condition_num; i++ ) {
               int best_pos = findNextPredicate(rel_predicate,condition_num,head);
               //metadata *metadata_array=metadata_array_creation(rel_pointers,rel_num);
               //printf("ddddddddd %ld\n",update_metadata_array(metadata_array,&rel_predicate[best_pos]));
               //free_metadata_array(metadata_array,rel_num);

               if(rel_predicate[best_pos].flag == 0) {
                 if ( rel_predicate[best_pos].left.row==rel_predicate[best_pos].right.row){ //isothta se idia sxesh
                    if(keys[rel_predicate[best_pos].left.row] == NULL) {  //den uparxei sta  endiamesa apotelesamta
                      result *Result=Simple_Scan_Tables(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     if(Result->size == 0 ) { //ola ta apotelesmata ginontai 0 giati kseroume oti prwta ekteleitai to > < an uparxei kai meta to join me thn sxesh pou summeteixe se auto
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
                     cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result_free(Result);
                   }
                   else { //kaloume thn sunarthsh Simple_Scan_Tables gia to relation pou prokuptei apo ta keys sta endiamesa
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
                     cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                     free(new_rel2->tuples);
                     free(new_rel2);
                   }
                 }
                 else{  //join
                   if(keys[rel_predicate[best_pos].left.row] == NULL && keys[rel_predicate[best_pos].right.row] == NULL) {  //den uparxei sxesh sta endiamesa
                     result *Result=RadixHashJoin(&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column],&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     if(Result->size == 0 ) {
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
                     cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;  //gia desmeush tou neou pinaka sto kon keys

                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);

                     result_free(Result);
                   }
                   else if(keys[rel_predicate[best_pos].left.row] == NULL && keys[rel_predicate[best_pos].right.row] != NULL) {
                     //an uparxei mia sxesh sta endiamesa allazei prwta gia na allaxtoun oi upoloipew kai epeita topotheteitai h allh
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
                     cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);

                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                   }
                   else if(keys[rel_predicate[best_pos].left.row] != NULL && keys[rel_predicate[best_pos].right.row] == NULL){
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
                     cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result2keys(Result,keys,rel_predicate[best_pos].right.row,1,rel_num);


                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                   }
                   else if(keys[rel_predicate[best_pos].left.row] != NULL && keys[rel_predicate[best_pos].right.row] != NULL) {
                     // uparxoun kai oi 2 sta endiamesa
                     relation *new_rel = keys2relation(keys[rel_predicate[best_pos].left.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].left.row].my_relations[rel_predicate[best_pos].left.column]);
                     relation *new_rel2 = keys2relation(keys[rel_predicate[best_pos].right.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
                     result *Result = NoneNull(new_rel,new_rel2);
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
                     cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                     result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                     result_free(Result);
                     free(new_rel->tuples);
                     free(new_rel);
                     free(new_rel2->tuples);
                     free(new_rel2);
                   }
                }
               }
               else if(rel_predicate[best_pos].flag == 1) {
                 if(keys[rel_predicate[best_pos].right.row] == NULL) {
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
                   cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                   result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                   result_free(Result);
                 }
                 else {
                   relation *new_rel = keys2relation(keys[rel_predicate[best_pos].right.row],cur_size,&cpy_tuple_array[rel_predicate[best_pos].right.row].my_relations[rel_predicate[best_pos].right.column]);
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
                   cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                   result2keys(Result,keys,rel_predicate[best_pos].right.row,0,rel_num);
                   result_free(Result);
                   free(new_rel->tuples);
                   free(new_rel);
                 }
               }
               else if(rel_predicate[best_pos].flag == 2) {
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
                   cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
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
                   cur_size = Result->total_records;//(Result->size-1)*bufferRows + Result->Tail->pos;
                   result2keys(Result,keys,rel_predicate[best_pos].left.row,0,rel_num);
                   result_free(Result);
                   free(new_rel->tuples);
                   free(new_rel);
                 }
               }
               if(rel_predicate[best_pos].flag == 0) {
                 if ( search_list(head,rel_predicate[best_pos].left.row)==0 ){  //an den einai hdh h sxesh sthn lista topothethse thn
                   push_list(&head,rel_predicate[best_pos].left.row);
                 }
                 if ( search_list(head,rel_predicate[best_pos].right.row)==0 ){
                   push_list(&head,rel_predicate[best_pos].right.row);
                 }
               }
               else if(rel_predicate[best_pos].flag == 1) {
                 if ( search_list(head,rel_predicate[best_pos].right.row)==0 ){
                   push_list(&head,rel_predicate[best_pos].right.row);
                 }
               }
               else {
                 if ( search_list(head,rel_predicate[best_pos].left.row)==0 ){
                   push_list(&head,rel_predicate[best_pos].left.row);
                 }
               }
               rel_predicate[best_pos].metric = -1; //gia na mhn ksanaepilegei
             }

            //free_metadata_array(metadata_array,rel_num);
            free(rel_pointers);
            free(rel_predicate);
            freeList(head);
            //-----------------------------select------------------------------
            int selection_num;
            point *rel_selection=string2rel_selection(tok3,&selection_num); //pinakas row,column gia to select
            uint64_t add;
            for(i=0;i<selection_num;i++) {
              add = calculate_sum(keys,cur_size,cpy_tuple_array,rel_selection[i].row,rel_selection[i].column);  //upologismos auroismatos column
              push_list2(&adds,add,1);
             }
             push_list2(&adds,1,-1);           //  ---->  -1 gia allgh grammhs

            free(rel_selection);
            free_structs(cpy_tuple_array,rel_num);

            for(i=0;i<rel_num;i++) {
              if(keys[i] != NULL) {
                free(keys[i]);
              }
            }
            free(keys);
        }
    }
    free(line);
    fclose(fp);
}

void result2keys(result *Result,int **keys,int row,int index,int rel_num) { //allazei twn pinaka keys
  int size = (Result->size-1)*bufferRows + Result->Tail->pos;

  if(keys[row] == NULL) { //topothethsh kainoyrgiou pinaka

    keys[row] = malloc(size*sizeof(int));

    int pos = 0;
    result_node *tmp=Result->Head;
    while ( tmp!=NULL ){
      //printf("---------------node %d--------------------\n",i);
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

    int **new_keys = malloc(rel_num*sizeof(int*));  //new_keys o neos pinakas pou meta ginetai anathesh stn keys endiamesa
    int i;
    for(i=0;i<rel_num;i++) {
      if(keys[i] != NULL) {
        new_keys[i] = malloc(size*sizeof(int));
      }
    }
    int pos = 0;
    result_node *tmp=Result->Head;
    while ( tmp!=NULL ){
      int j;
      for ( j=0; j<tmp->pos; j++ ){
        for(i=0;i<rel_num;i++) {
          if(keys[i] != NULL) {
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
  }
}

relation *keys2relation(int *keys,int size,relation *OldRelation) { //ftiaxnei ena relation me ta payloads twn keys twn endiameswn apotelesmatwn
  relation *new_rel = malloc(sizeof(relation)); // to key pou prokuptei -1 einai index ston pinaka keys[i]
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
    // if ( condition==NULL ){
    //   printf("condition not NULL\n");
    // }
    rel_condition[i] = strdup(condition);

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
      // printf("Wrong operation \n");
    }

    char *rest=NULL;
    char *tok = strtok_r(rel_condition[i],&rel_predicate[i].operation,&rest);
    if(strchr(tok,'.')!=NULL) {
      rel_predicate[i].left.row = atoi(strtok(tok,"."));
      rel_predicate[i].left.column = atoi(strtok(NULL,"."));
      rel_predicate[i].flag = 0;  //estw join h isothta idias sxeshs
      rel_predicate[i].metric = 0;
    }
    else {
      rel_predicate[i].number = atoi(tok);
      rel_predicate[i].flag = 1;  //allagh
      rel_predicate[i].metric = 1000;
    }

    if(strchr(rest,'.')!=NULL) {
      tok = strtok(rest,".");
      rel_predicate[i].right.row = atoi(tok);
      //tok = strtok(NULL,".");
      rel_predicate[i].right.column = atoi(strtok(NULL,"."));

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
      relations_cpy[i].my_metadata.statistics_array[j].count = rel_pointers[i]->my_metadata.statistics_array[j].count;
    }

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

metadata *metadata_array_creation(full_relation **rel_pointers,int rel_num){
  metadata *metadata_array = malloc(rel_num*sizeof(metadata));

  int i;
  for(i=0;i<rel_num;i++) {
    metadata_array[i].num_tuples = rel_pointers[i]->my_metadata.num_tuples;
    metadata_array[i].num_columns = rel_pointers[i]->my_metadata.num_columns;

    metadata_array[i].statistics_array = malloc(metadata_array[i].num_columns*sizeof(statistics));
    int j;
    for(j=0;j<metadata_array[i].num_columns;j++) {
      metadata_array[i].statistics_array[j].min = rel_pointers[i]->my_metadata.statistics_array[j].min;
      metadata_array[i].statistics_array[j].max = rel_pointers[i]->my_metadata.statistics_array[j].max;
      metadata_array[i].statistics_array[j].count = rel_pointers[i]->my_metadata.statistics_array[j].count;
    }
  }

  return metadata_array;
}

void free_metadata_array(metadata *metadata_array,int rel_num){
  int i,j;
  for ( i=0; i<rel_num; i++ ){
    free(metadata_array[i].statistics_array);
  }
  free(metadata_array);
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
        the_predicate->metric += 500;
        return;
      }
      else {
        int min1 = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].min;
        int max1 = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].max;
        int min2 = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].min;
        int max2 = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].max;
        tmp = (double)(MIN(max1,max2)-MAX(min1,min2)) / ((double)(MAX(max1,max2)-MIN(min1,min2)));
      }
    }
    else{
      if ( the_predicate->flag==1 ){
        min = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].min;
        max = subcpy_full_relation[the_predicate->right.row].my_metadata.statistics_array[the_predicate->right.column].max;
        if ( the_predicate->operation=='>' ){
          tmp = (double)(the_predicate->number-min)/(double)(max-min);
          if ( tmp<=0 ){  //kanena stoixeio den pernaei elegexos eta
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
        min = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].min;
        max = subcpy_full_relation[the_predicate->left.row].my_metadata.statistics_array[the_predicate->left.column].max;

        if ( the_predicate->operation=='>' ){
          tmp = (double)(the_predicate->number-min)/(double)(max-min);
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

    int x = (1.0-tmp)*500;  //upologismos pososotou koinwn stoixeiwn apo statistika

    the_predicate->metric += x;

}

uint64_t calculate_sum(int **keys,int size,full_relation *cpy_tuple_array,int row,int column){
  if(keys[row] == NULL){
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

int *enumeration(predicate *rel_predicate,int condition_num,full_relation **rel_pointers,int rel_num) {

  intermidiate_results *inter_resuts = malloc(condition_num*sizeof(intermidiate_results));
  int **order;
  order = malloc(condition_num * sizeof(int *));
  int i;
  for(i=0;i<condition_num;i++) {
    order[i] = malloc(condition_num*sizeof(int));
    order[i][0] = i;
    inter_resuts[i].visited = NULL;
    push_list(&(inter_resuts[i].visited),rel_predicate[i].right.row);
    push_list(&(inter_resuts[i].visited),rel_predicate[i].left.row);
    inter_resuts[i].I_metadata = metadata_array_creation(rel_pointers,rel_num);
    inter_resuts[i].cur_sum = update_metadata_array(inter_resuts[i].I_metadata,&rel_predicate[i]);
    printf("cur sum is %d\n",inter_resuts[i].cur_sum );
    //upbdate metadata
  }


  for(i=1;i<condition_num;i++) { //gia styles

    int k;
    int best_line;
    for(k=0;k<condition_num;k++) { //gia grammes
      int cur = i;
      int best_f,new_f;

      int j;
      for(j=0;j<condition_num;j++) { //gia ola ta pithana
        if(allready_inside(order[k],cur,j) || !(search_list(inter_resuts[k].visited,rel_predicate[j].right.row) || search_list(inter_resuts[k].visited,rel_predicate[j].left.row))) {
          continue;
        }
        intermidiate_results *tmp_line = cpy_intermmediate(&inter_resuts[k],rel_num);
        //printf("perasa %d %d\n",order[k][0],j);
        //list *tmp_list = copy_list(inter_resuts[k].visited);
        new_f = update_metadata_array(tmp_line->I_metadata,&rel_predicate[j]);
        if(cur==i || best_f > new_f ) {// f > update_metadata_array(new)
          cur = i+1;
          order[k][cur-1] = j;
          //push_list(&(inter_resuts[k].visited),rel_predicate[j].right.row);
          //push_list(&(inter_resuts[k].visited),rel_predicate[j].left.row);
          best_f = new_f;
        }
      }
      inter_resuts[i].cur_sum += update_metadata_array(inter_resuts[k].I_metadata,&rel_predicate[order[k][cur-1]]);
      push_list(&(inter_resuts[k].visited),rel_predicate[order[k][cur-1]].right.row);
      push_list(&(inter_resuts[k].visited),rel_predicate[order[k][cur-1]].left.row);

    }


  }
  int best=0;
  for(i=1;i<condition_num;i++) {
    if(inter_resuts[i].cur_sum < inter_resuts[best].cur_sum) {
      best = i;
    }
  }

  return order[best];

}

intermidiate_results *cpy_intermmediate(intermidiate_results *tmp,int rel_num) {
  intermidiate_results *res = malloc(sizeof(intermidiate_results));
  res->visited = NULL;
  res->cur_sum = tmp->cur_sum;
  res->visited = copy_list(tmp->visited);
  res->I_metadata = cpy_metadata(tmp->I_metadata,rel_num);


  return res;
}

metadata *cpy_metadata(metadata *from_metadata,int rel_num){
    metadata *metadata_new = malloc(rel_num*sizeof(metadata));

    int i;
    for(i=0;i<rel_num;i++) {
      metadata_new[i].num_tuples = from_metadata[i].num_tuples;
      metadata_new[i].num_columns = from_metadata[i].num_columns;

      metadata_new[i].statistics_array = malloc(from_metadata[i].num_columns*sizeof(statistics));
      int j;
      for(j=0;j<metadata_new[i].num_columns;j++) {
        metadata_new[i].statistics_array[j].min = from_metadata[i].statistics_array[j].min;
        metadata_new[i].statistics_array[j].max = from_metadata[i].statistics_array[j].max;
        metadata_new[i].statistics_array[j].count = from_metadata[i].statistics_array[j].count;
      }
    }
    return metadata_new;
}

int allready_inside(int *order,int cur,int j){
  int k;
  for(k=0;k<cur;k++) {
    if(order[k] == j) {
      //printf("order is 1\n" );
      return 1;
    }
  }
  //printf("order is 0\n" );
  return 0;
}

uint64_t update_metadata_array(metadata *metadata_array,predicate *the_predicate){
  if ( the_predicate->flag==0 ){
    if ( the_predicate->left.row==the_predicate->right.row){
      uint64_t min=metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].min;
      uint64_t max=metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].max;
      uint64_t n=max-min + 1;
      metadata_array[the_predicate->left.row].num_tuples = metadata_array[the_predicate->left.row].num_tuples*metadata_array[the_predicate->left.row].num_tuples/(double)n;
      return metadata_array[the_predicate->left.row].num_tuples;
    }
    else{
      uint64_t min,max;
      uint64_t min_left=metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].min;
      uint64_t min_right=metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].min;
      uint64_t max_left=metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].max;
      uint64_t max_right=metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].max;
      min = (min_left>min_right) ? min_left : min_right;
      max = (max_left<max_right) ? max_left : max_right;
      metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].min = min;
      metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].min = min;
      metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].max = max;
      metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].max = max;
      uint64_t n=max-min + 1;
      uint64_t old_f_left=metadata_array[the_predicate->left.row].num_tuples;
      uint64_t old_f_right=metadata_array[the_predicate->right.row].num_tuples;
      uint64_t f_tonos=(uint64_t)(metadata_array[the_predicate->left.row].num_tuples*metadata_array[the_predicate->right.row].num_tuples)/(double)n;
      metadata_array[the_predicate->left.row].num_tuples = f_tonos;
      metadata_array[the_predicate->right.row].num_tuples = f_tonos;
      int old_count_left=metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].count;
      int old_count_right=metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].count;
      int d_tonos=(int)(old_count_left*old_count_right)/(double)n;
      metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].count = d_tonos;
      metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].count = d_tonos;
      int i;
      for ( i=0; i<metadata_array[the_predicate->left.row].num_columns; i++ ){
        if ( i!=the_predicate->left.column ){
          metadata_array[the_predicate->left.row].statistics_array[i].min = min_left;
          metadata_array[the_predicate->left.row].statistics_array[i].max = max_left;
          metadata_array[the_predicate->left.row].statistics_array[i].count = (int)metadata_array[the_predicate->left.row].statistics_array[i].count*
              (1-pow((double)(1-(metadata_array[the_predicate->left.row].statistics_array[the_predicate->left.column].count/(double)old_count_left)),
                                old_f_left/(double)metadata_array[the_predicate->left.row].statistics_array[i].count));
        }
      }
      for ( i=0; i<metadata_array[the_predicate->right.row].num_columns; i++ ){
        if ( i!=the_predicate->right.column ){
          metadata_array[the_predicate->right.row].statistics_array[i].min = min_left;
          metadata_array[the_predicate->right.row].statistics_array[i].max = max_left;
          metadata_array[the_predicate->right.row].statistics_array[i].count = (int)metadata_array[the_predicate->right.row].statistics_array[i].count*
              (1-pow((double)(1-(metadata_array[the_predicate->right.row].statistics_array[the_predicate->right.column].count/(double)old_count_left)),
                                old_f_left/(double)metadata_array[the_predicate->right.row].statistics_array[i].count));
        }
      }
      return f_tonos;
    }
  }
  else{
    int row,column;
    if ( the_predicate->flag==1 ){
      row = the_predicate->right.row;
      column = the_predicate->right.column;
    }
    else{ //flag 2
      row = the_predicate->left.row;
      column = the_predicate->left.column;
    }
    if ( the_predicate->operation=='<' || the_predicate->operation=='>' ){
      int i;
      uint64_t old_min=metadata_array[row].statistics_array[column].min;
      uint64_t old_max=metadata_array[row].statistics_array[column].max;
      uint64_t k1;
      uint64_t k2;
      if ( the_predicate->operation=='<' ){
        k1=old_min;
        k2=the_predicate->number;
        if ( k2>old_max ){
          k2 = old_max;
        }
      }
      else{
        k1 = the_predicate->number;
        if ( k1<old_min ){
          k1 = old_min;
        }
        k2 = old_max;
      }
      metadata_array[row].statistics_array[column].min = k1;
      metadata_array[row].statistics_array[column].max = k2;
      metadata_array[row].statistics_array[column].count = ((k2-k1)/(double)(old_max-old_min))*metadata_array[row].statistics_array[column].count;
      uint64_t old_f=metadata_array[row].num_tuples;
      metadata_array[row].num_tuples = ((k2-k1)/(double)(old_max-old_min))*old_f;
      for ( i=0; i<metadata_array[row].num_columns; i++ ){
        if ( i!=column ){
          metadata_array[row].statistics_array[i].count = (int)metadata_array[row].statistics_array[i].count*
              (1-pow((double)(1-(metadata_array[row].num_tuples/(double)old_f)),
                                old_f/(double)metadata_array[row].statistics_array[i].count));
        }
      }
      return metadata_array[row].num_tuples;
    }
    else{ //=
      int i;
      uint64_t old_min=metadata_array[row].statistics_array[column].min;
      uint64_t old_max=metadata_array[row].statistics_array[column].max;
      metadata_array[row].statistics_array[column].min = the_predicate->number;
      metadata_array[row].statistics_array[column].max = the_predicate->number;
      int old_count=metadata_array[row].statistics_array[column].count;
      metadata_array[row].statistics_array[column].count = 0;
      uint64_t old_f=metadata_array[row].num_tuples;
      metadata_array[row].num_tuples = 0;

      if ( the_predicate->number>old_min && the_predicate->number<old_max ){
        metadata_array[row].statistics_array[column].count = 1;
        metadata_array[row].num_tuples = old_f/(double)old_count;
      }
      for ( i=0; i<metadata_array[row].num_columns; i++ ){
        if ( i!=column ){
          metadata_array[row].statistics_array[i].count = (int)metadata_array[row].statistics_array[i].count*
              (1-pow((double)(1-(metadata_array[row].num_tuples/(double)old_f)),
                                old_f/(double)metadata_array[row].statistics_array[i].count));
        }
      }
      return metadata_array[row].num_tuples;  //f'
    }
  }
}
