#include "statistics.h"

int *enumeration(predicate *rel_predicate,int condition_num,full_relation **rel_pointers,int rel_num) {
  // epistrefei enan pinaka me thn epi8umhth seira pou prepei na ektelestoun ta predicates

  intermidiate_results *inter_resuts = malloc(condition_num*sizeof(intermidiate_results));
  int **order; // pinakas me tis epiloges
  order = malloc(condition_num * sizeof(int *));
  int i;
  for(i=0;i<condition_num;i++) { // bale prwta to filtro
    order[i] = malloc(condition_num*sizeof(int));
    order[i][0] = condition_num-1;
    inter_resuts[i].visited = NULL;
    int row_for_insert;
    if ( rel_predicate[condition_num-1].flag==0 ){
      if ( rel_predicate[condition_num-1].right.row==rel_predicate[condition_num-1].left.row ){
        row_for_insert = rel_predicate[condition_num-1].right.row;
      }
    }
    else if ( rel_predicate[condition_num-1].flag==1 ){
      row_for_insert = rel_predicate[condition_num-1].right.row;
    }
    else{
      row_for_insert = rel_predicate[condition_num-1].left.row;
    }
    push_list(&(inter_resuts[i].visited),row_for_insert);
    inter_resuts[i].I_metadata = metadata_array_creation(rel_pointers,rel_num);
    inter_resuts[i].cur_sum = update_metadata_array(inter_resuts[i].I_metadata,&rel_predicate[condition_num-1]);
  }

  for(i=0;i<condition_num;i++) { // bale oles ti dunates epiloges
    order[i][1] = i;
    if( i == order[i][0] || !(search_list(inter_resuts[i].visited,rel_predicate[i].right.row) || search_list(inter_resuts[i].visited,rel_predicate[i].left.row))) {
      inter_resuts[i].cur_sum = -1; // den uparxei lush gia ayth thn grammh
      continue;
    }
    push_list(&(inter_resuts[i].visited),rel_predicate[i].right.row);
    push_list(&(inter_resuts[i].visited),rel_predicate[i].left.row);
    inter_resuts[i].cur_sum += update_metadata_array(inter_resuts[i].I_metadata,&rel_predicate[i]);
  }

  for(i=2;i<condition_num;i++) { //gia tis styles

    int k;
    int best_line;
    for(k=0;k<condition_num;k++) { //gia tis grammes
      int cur = i;
      int best_f,new_f;
      if(inter_resuts[k].cur_sum == -1) {
        continue;
      }

      int j;
      for(j=0;j<condition_num-1;j++) { //gia ola ta pithana
        if(allready_inside(order[k],i,j) || !(search_list(inter_resuts[k].visited,rel_predicate[j].right.row) || search_list(inter_resuts[k].visited,rel_predicate[j].left.row))) {
          continue;
        }
        intermidiate_results *tmp_line = cpy_intermmediate(&inter_resuts[k],rel_num);
        new_f = update_metadata_array(tmp_line->I_metadata,&rel_predicate[j]);
        if(cur==i || best_f > new_f ) {// f > update_metadata_array(new)
          cur = i+1;
          order[k][cur-1] = j;
          best_f = new_f;
        }
        freeList(tmp_line->visited);
        free_metadata_array(tmp_line->I_metadata,rel_num);
        free(tmp_line);
      }
      if ( cur==i ){
        inter_resuts[k].cur_sum = -1;
      }
      else {
        inter_resuts[k].cur_sum += update_metadata_array(inter_resuts[k].I_metadata,&rel_predicate[order[k][cur-1]]);
        push_list(&(inter_resuts[k].visited),rel_predicate[order[k][cur-1]].right.row);
        push_list(&(inter_resuts[k].visited),rel_predicate[order[k][cur-1]].left.row);
      }
    }


  }
  int best=0;
  for(i=1;i<condition_num;i++) {
    if(inter_resuts[best].cur_sum == -1 || (inter_resuts[i].cur_sum != -1 && inter_resuts[i].cur_sum < inter_resuts[best].cur_sum)) {
      best = i;
    }
  }
  //printf("best cur sum is %d\n",inter_resuts[best] );

  int *final_best=malloc(condition_num*sizeof(int));
  for ( i=0; i<condition_num; i++){
    final_best[i] = order[best][i];
  }
  for ( i=0; i<condition_num; i++ ){
    free(order[i]);
  }
  free(order);

  for ( i=0; i<condition_num; i++ ){
    freeList(inter_resuts[i].visited);
    free_metadata_array(inter_resuts[i].I_metadata,rel_num);
  }
  free(inter_resuts);
  return final_best;

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
  // to stoixeio j brhskete hdh mesa ston pinaka order (epestrepse 1)
  int k;
  for(k=0;k<cur;k++) {
    if(order[k] == j) {
      return 1;
    }
  }
  return 0;
}

uint64_t update_metadata_array(metadata *metadata_array,predicate *the_predicate){
  // allazei ta statistika sumfwna me thn praksh
  // epistrefei to megethos tou apotelesmatos
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

// ------- palia ylopoihsh ---------- //
int findNextPredicate(predicate* rel_predicate,int size,list *head) { //upologismos next predicate gia ektelesh

  int i;
  int best_pos = 0; // position of best choice so far

  for(i=1;i<size;i++) {

    if(empty_list(head)) { //first time, the list with rows is empty
      if(rel_predicate[i].metric > rel_predicate[best_pos].metric) {
        best_pos = i;
      }
    }
    else { // not the first time
      if(rel_predicate[i].metric == -1) {
        continue;
      }
      if(rel_predicate[best_pos].metric == -1) {
          best_pos = i;
          continue;
      }
      int rows = 0;
      if(rel_predicate[i].flag == 0 && rel_predicate[best_pos].flag == 0) {
        int best_rows = search_list(head,rel_predicate[best_pos].left.row) + search_list(head,rel_predicate[best_pos].right.row);
        int rows = search_list(head,rel_predicate[i].left.row) + search_list(head,rel_predicate[i].right.row);
        if((rows > best_rows) || (best_rows == rows && rel_predicate[i].metric > rel_predicate[best_pos].metric)) {
          best_pos = i;
        }
      }
      else if(rel_predicate[i].flag == 1 || rel_predicate[i].flag == 2) {
        if(rel_predicate[best_pos].flag == 0 || (rel_predicate[best_pos].flag != 0 && rel_predicate[i].metric > rel_predicate[best_pos].metric)) {
          best_pos = i;
        }

      }
      else {
        printf("Something wrong\n");
      }

    }

  }

  return best_pos;
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
