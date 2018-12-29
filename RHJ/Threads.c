#include "ThreadFunctions.h"

void* thread_1(void* argp){
  int Hash_number = pow(2,FirstHash_number);

  Sheduler_values *my_args=argp;
  int err,i;
  while ( my_args->shutdown==0){ //  || my_args->my_Job_list->size > 0
    if ( err=pthread_mutex_lock(&mtx_forlist) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    printf("size list %d\n",my_args->my_Job_list->size );
    if ( my_args->my_Job_list->size<=0 ){
      printf("I wait\n");
      pthread_cond_wait(&cv_nonempty,&mtx_forlist1);
    }
    printf("I start with size %d\n",my_args->my_Job_list->size);
    if ( my_args->my_Job_list->size>0 ){
      typeHist *myHist;
      Job *my_Job=pop_Job(my_args->my_Job_list);
      printf("Job id is %d\n",my_Job->id);
      if ( my_Job!=NULL ){
        myHist = Rel_to_Hist(my_Job->relR,my_Job->my_limits->start,my_Job->my_limits->end);
        if ( err=pthread_mutex_unlock(&mtx_forlist) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
        if ( err=pthread_mutex_lock(&mtx_write) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
        for ( i=0; i<Hash_number; i++ ){
          my_args->Hist[i].num += myHist[i].num;
        }
        if ( err=pthread_mutex_unlock(&mtx_write) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
        free(my_Job);
      }
      else {
        if ( err=pthread_mutex_unlock(&mtx_forlist) ){
          perror("pthread_mutex_lock");
          exit(1) ;
        }
      }

    }
    else {
      if ( err=pthread_mutex_unlock(&mtx_forlist) ){
        perror("pthread_mutex_lock");
        exit(1) ;
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
  pthread_exit(NULL);

}

void* thread_2(void* argp){

}

void* thread_3(void* argp){

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
