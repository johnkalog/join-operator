#include "ThreadFunctions.h"

void* Histogram_thread(void* argp){

  int Hash_number = pow(2,FirstHash_number);

  Sheduler_values *my_args=argp;
  int err,i;
  while ( my_args->shutdown==0 || my_args->my_Job_list->size > 0 ){ //  || my_args->my_Job_list->size > 0


    //printf("I wait %ld\n", pthread_self());
      sem_wait(&sem);

      //printf("I start with size %d\n",my_args->my_Job_list->size);
      if ( my_args->my_Job_list->size<=0 ){
      	break;
      }
      typeHist *myHist;
      if ( err=pthread_mutex_lock(&mtx_xd) ){
        perror("pthread_mutex_lock");
        exit(1) ;
      }
      if ( my_args->my_Job_list->size<=0 ){

        pthread_mutex_unlock(&mtx_xd);
        break;
      }
      //printf("size list %d  %ld\n",my_args->my_Job_list->size,  pthread_self() );
      Job *my_Job=pop_Job(my_args->my_Job_list);

      if ( err=pthread_mutex_unlock(&mtx_xd) ){
        perror("pthread_mutex_lock");
        exit(1) ;
      }
      //printf("t1\n");
      //printf("Job id is %d\n",my_Job->id);

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
      else{
          printf("xDDDD\n");
          my_args->my_Job_list->size=0;
      }


  }
  pthread_exit(NULL);

}

void* NewRel_thread(void* argp){
  //printf("thread_2\n");

  Sheduler_values *my_args=argp;
  int err,i;
  while ( my_args->shutdown==0 || my_args->my_Job_list->size > 0 ){ //  || my_args->my_Job_list->size > 0

    sem_wait(&sem);

    //printf("I start with size %d\n",my_args->my_Job_list->size);
    if ( my_args->my_Job_list->size<=0 ){
      break;
    }
    typeHist *myHist;
    if ( err=pthread_mutex_lock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    if ( my_args->my_Job_list->size<=0 ){

      pthread_mutex_unlock(&mtx_xd);
      break;
    }
    //printf("size list %d  %ld\n",my_args->my_Job_list->size,  pthread_self() );
    Job *my_Job=pop_Job(my_args->my_Job_list);

    if ( err=pthread_mutex_unlock(&mtx_xd) ){
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
    else{
        printf("xDDDD\n");
        my_args->my_Job_list->size = 0;
    }

  }
  pthread_exit(NULL);
}

void* Bucket_thread(void* argp){
  //printf("thread_3\n");
  Sheduler_values *my_args=argp;
  int err,i;
  result *tmp_Result=result_init();

  int sizeBucket=pow(2,SecondHash_number);
  HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
  TheHashBucket->chain = NULL;
  TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));

  while ( my_args->shutdown==0 || my_args->my_Job_list->size > 0 ){ //  || my_args->my_Job_list->size > 0
    sem_wait(&sem);

    //printf("I start with size %d\n",my_args->my_Job_list->size);
    if ( my_args->my_Job_list->size<=0 ){
      break;
    }
    typeHist *myHist;
    if ( err=pthread_mutex_lock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    if ( my_args->my_Job_list->size<=0 ){
      //printf("why?\n");
      pthread_mutex_unlock(&mtx_xd);
      break;
    }
    //printf("size list %d  %ld\n",my_args->my_Job_list->size,  pthread_self() );
    Job *my_Job=pop_Job(my_args->my_Job_list);

    if ( err=pthread_mutex_unlock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    //printf("Job id is %d\n",my_Job->id);
    if(my_Job!=NULL) {
          one_bucket_join(my_Job->bucket_index,tmp_Result,TheHashBucket,my_args->Hist,my_args->Hist2,my_args->PsumR,my_args->PsumS,my_args->NewRelR,my_args->NewRelS);
    }
    else{
        printf("xDDDD\n");
        my_args->my_Job_list->size = 0;
    }
    free(my_Job);
}
  if ( err=pthread_mutex_lock(&mtx_write) ){
    perror("pthread_mutex_lock");
    exit(1) ;
  }
  int flag=0;
  //printf("%d ----- %d\n",tmp_Result->size,tmp_Result->total_records  );
  if(tmp_Result->Head != NULL) {
    if(my_args->Result->Head == NULL) {
      free(my_args->Result);
      my_args->Result = tmp_Result;
      flag=1;
    }
    else {
      my_args->Result->Tail->next = tmp_Result->Head;
      my_args->Result->Tail = tmp_Result->Tail;
      my_args->Result->size += tmp_Result->size;
      my_args->Result->total_records += tmp_Result->total_records;

    }
  }
  if ( err=pthread_mutex_unlock(&mtx_write) ){
    //perror("pthread_mutex_unlock");
    exit(1) ;
  }
  if(flag==0){
      free(tmp_Result);
  }

  free(TheHashBucket->bucket);
  free(TheHashBucket);
  
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
