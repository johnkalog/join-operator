#include "ThreadFunctions.h"

void* thread_1(void* argp){
  int Hash_number = pow(2,FirstHash_number);

  Sheduler_values *my_args=argp;
  int err,i;
  while ( my_args->shutdown==0 || my_args->my_Job_list->size > 0 ){ //  || my_args->my_Job_list->size > 0
    if ( err=pthread_mutex_lock(&mtx_forlist) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    //printf("size list %d\n",my_args->my_Job_list->size );
    if ( my_args->my_Job_list->size<=0 ){
      //printf("I wait\n");
      if(my_args->shutdown!=0){
          break;
          pthread_exit(NULL);
      }
      pthread_cond_wait(&cv_nonempty,&mtx_forlist1);
    }
    //printf("I start with size %d\n",my_args->my_Job_list->size);
    if ( my_args->my_Job_list->size>0 ){
      typeHist *myHist;
      Job *my_Job=pop_Job(my_args->my_Job_list);
      //printf("Job id is %d\n",my_Job->id);
      if ( err=pthread_mutex_unlock(&mtx_forlist) ){
        perror("pthread_mutex_lock");
        exit(1) ;
      }
      if ( my_Job!=NULL ){

        myHist = Rel_to_Hist(my_Job->relR,my_Job->my_limits->start,my_Job->my_limits->end);
        if ( err=pthread_mutex_lock(&mtx_write) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
        if(my_Job->id == 0) {
            for ( i=0; i<Hash_number; i++ ){
              my_args->Hist[i].num += myHist[i].num;
            }
        } else {
            for ( i=0; i<Hash_number; i++ ){
              my_args->Hist2[i].num += myHist[i].num;
            }
        }
        free(myHist);
        if ( err=pthread_mutex_unlock(&mtx_write) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
        free(my_Job);
      }
    }


  }
  //while den exei erthei shma eksodou
  //if list kenh wait must send signal from main to end all locked
  //if there is item
  //mutex lock
  //pop
  //save to my space
  //mutex unlock
  ///acomplish my job
  pthread_mutex_unlock(&mtx_forlist);
  pthread_exit(NULL);

}

void* thread_2(void* argp){
  Sheduler_values *my_args=argp;
  int err,i;
  while ( my_args->shutdown==0 || my_args->my_Job_list->size > 0 ){ //  || my_args->my_Job_list->size > 0
    if ( err=pthread_mutex_lock(&mtx_forlist) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    //printf("size list %d\n",my_args->my_Job_list->size );
    if ( my_args->my_Job_list->size<=0 ){
      //printf("I wait\n");
      if(my_args->shutdown!=0){
          break;
      }
      pthread_cond_wait(&cv_nonempty,&mtx_forlist1);
    }
    //printf("I start with size %d\n",my_args->my_Job_list->size);
    if ( my_args->my_Job_list->size>0 ){
      typeHist *myHist;
      Job *my_Job=pop_Job(my_args->my_Job_list);

      if ( err=pthread_mutex_unlock(&mtx_forlist) ){
        perror("pthread_mutex_lock");
        exit(1) ;
      }
      //printf("Job id is %d\n",my_Job->id);
      if(my_Job!=NULL) {
        if ( err=pthread_mutex_lock(&mtx_write) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
        if(my_Job->id == 0) {
          change_part_relation2(my_Job->relR,my_args->NewRelR,my_Job->my_limits->start,my_Job->my_limits->end,my_Job->PsumR);
        }
        else {
          change_part_relation2(my_Job->relS,my_args->NewRelS,my_Job->my_limits->start,my_Job->my_limits->end,my_Job->PsumS);
        }
        free(my_Job);
        if ( err=pthread_mutex_unlock(&mtx_write) ){
          perror("pthread_mutex_unlock");
          exit(1) ;
        }
      }
    }
  }
  pthread_mutex_unlock(&mtx_forlist);
  pthread_exit(NULL);
}

void* thread_3(void* argp){
  Sheduler_values *my_args=argp;
  int err,i;
  result *tmp_Result=result_init();

  int sizeBucket=pow(2,SecondHash_number);
  HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
  TheHashBucket->chain = NULL;
  TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));

  while ( my_args->shutdown==0 || my_args->my_Job_list->size > 0 ){ //  || my_args->my_Job_list->size > 0
    if ( err=pthread_mutex_lock(&mtx_forlist) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    //printf("size list %d\n",my_args->my_Job_list->size );
    if ( my_args->my_Job_list->size<=0 ){
      //printf("I wait\n");
      if(my_args->shutdown!=0){
          break;
      }
      pthread_cond_wait(&cv_nonempty,&mtx_forlist1);
    }
    //printf("I start with size %d\n",my_args->my_Job_list->size);
    if ( my_args->my_Job_list->size>0 ){
      typeHist *myHist;
      Job *my_Job=pop_Job(my_args->my_Job_list);

      if ( err=pthread_mutex_unlock(&mtx_forlist) ){
        perror("pthread_mutex_unlock");
        exit(1) ;
      }
      //printf("Job id is %d\n",my_Job->id);
      if(my_Job!=NULL) {
        for(i=my_Job->my_limits->start; i<my_Job->my_limits->end; i++){
            one_bucket_join(i,tmp_Result,TheHashBucket,my_args->Hist,my_args->Hist2,my_args->PsumR,my_args->PsumS,my_args->NewRelR,my_args->NewRelS);
        }
      }
    }
  }
  if ( err=pthread_mutex_lock(&mtx_write) ){
    perror("pthread_mutex_lock");
    exit(1) ;
  }

  if(my_args->Result->Head == NULL) {
    my_args->Result = tmp_Result;
  }
  else {
    my_args->Result->Tail->next = tmp_Result->Head;
    my_args->Result->Tail = tmp_Result->Tail;
    my_args->Result->size += tmp_Result->size;
    my_args->Result->total_records += tmp_Result->total_records;

  }
  // size_kombwn
  if ( err=pthread_mutex_unlock(&mtx_write) ){
    perror("pthread_mutex_unlock");
    exit(1) ;
  }


  free(TheHashBucket->bucket);
  free(TheHashBucket);

  pthread_mutex_unlock(&mtx_forlist);
  pthread_exit(NULL);

}

Job_list *Job_list_init(){
  Job_list *my_Job_list=malloc(sizeof(Job_list));
  my_Job_list->size = 0;
  my_Job_list->Head = NULL;
  my_Job_list->Tail = NULL;
  return my_Job_list;
}

void push_Job(Job_list *my_Job_list,Job *newJob){
  Job *cpy=malloc(sizeof(Job));
  cpy->id = newJob->id;
  cpy->bucket_index = newJob->bucket_index;
  cpy->relR = newJob->relR;
  cpy->relS = newJob->relS;
  cpy->my_limits = newJob->my_limits;
  cpy->HistR = newJob->HistR;
  cpy->HistS = newJob->HistS;
  cpy->PsumR = newJob->PsumR;
  cpy->PsumS = newJob->PsumS;
  cpy->next = newJob->next;
  if ( my_Job_list->Head==NULL ){
    my_Job_list->Head = cpy;
    my_Job_list->Tail = my_Job_list->Head;
  }
  else{
    my_Job_list->Tail->next = cpy;
    my_Job_list->Tail = cpy;
  }
  my_Job_list->size ++;
}

Job *pop_Job(Job_list *my_Job_list){
  if ( my_Job_list->Head==NULL ){
    return NULL;
  }
  else{
    Job *tmp=my_Job_list->Head;
    my_Job_list->Head = tmp->next;
    if ( tmp->next==NULL ){
      my_Job_list->Tail = NULL;
    }
    my_Job_list->size --;
    return tmp;
  }
}
