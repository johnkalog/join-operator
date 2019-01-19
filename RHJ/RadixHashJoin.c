#include <math.h>
#include "ThreadFunctions.h"

result* Join(relation *relR, relation *relS) {

  result *Result=result_init();

  int i,j;
  for ( i=0; i<relR->num_tuples; i++ ){
    for ( j=0; j<relS->num_tuples; j++ ){
      if ( relR->tuples[i].payload==relS->tuples[j].payload ){
        insert(Result,relR->tuples[i].key,relS->tuples[j].key);
      }
    }
  }
  return Result;


}

result* RadixHashJoin(relation *relR, relation *relS) {
  // returns result
  // result: InfoNode se tupo listas pou kathe kombos exei ena 2d array
  int i;
  pthread_mutex_init(&mtx_xd,NULL);
  pthread_mutex_init(&mtx_write,NULL);

  sem_init(&sem, 0, 0);

  pthread_t err;
  pthread_t *thread_pool=malloc(num_threads*sizeof(pthread_t));
  //----------------------------------------------------------------------------------------------------

  Job_list *my_Job_list=Job_list_init();
  Sheduler_values *args=malloc(sizeof(Sheduler_values));
  args->shutdown = 0;
  args->my_Job_list = my_Job_list;

  for ( i=0; i<num_threads; i++ ){
    if ( err=pthread_create(&thread_pool[i],NULL,Histogram_thread,args) ){
      perror ("pthread_create");
      exit(1);
    }
  }

  int Hash_number = pow(2,FirstHash_number);  //arithmos twn bucket
  typeHist *HistR,*HistS;

  int j;
  args->Hist = malloc(Hash_number*sizeof(typeHist));

  for ( i=0; i<Hash_number; i++ ){
    args->Hist[i].box = i;
    args->Hist[i].num = 0;
  }

  int current_num_tuples;
  current_num_tuples=relR->num_tuples;
  limits *limits_arrayR=calculate_limits(current_num_tuples);

  Job *newJob;
  for ( i=0; i<num_threads; i++ ){
    newJob = malloc(sizeof(Job));
    newJob->id = 0;
    newJob->my_limits = &limits_arrayR[i];
    newJob->relR = relR;
    newJob->next = NULL;
    if ( err=pthread_mutex_lock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    push_Job(my_Job_list,newJob);

    if ( err=pthread_mutex_unlock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    sem_post(&sem);
    free(newJob);
  }



  args->Hist2 = malloc(Hash_number*sizeof(typeHist));

  for ( i=0; i<Hash_number; i++ ){
    args->Hist2[i].box = i;
    args->Hist2[i].num = 0;
  }

  current_num_tuples=relS->num_tuples;
  limits *limits_arrayS=calculate_limits(current_num_tuples);

  for ( i=0; i<num_threads; i++ ){
    newJob = malloc(sizeof(Job));
    newJob->id = 1;
    newJob->my_limits = &limits_arrayS[i];
    newJob->relR = relS;
    newJob->next = NULL;
    if ( err=pthread_mutex_lock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    push_Job(my_Job_list,newJob);
    if ( err=pthread_mutex_unlock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    sem_post(&sem);
    free(newJob);
  }

  args->shutdown = 1;
  for ( i=0; i<num_threads; i++ ){
    sem_post(&sem);
  }
  for ( i=0; i<num_threads; i++ ){
    if ( err=pthread_join(thread_pool[i],NULL)) {
      perror ("pthread_join");
    }
  }
  HistR = args->Hist;
  HistS = args->Hist2;

  free(limits_arrayR);
  free(limits_arrayS);
  free(args);
  free(my_Job_list);

//----------------------------------------------------------------------------------------------------
  typeHist *PsumR=Hist_to_Psum(HistR);
  typeHist *PsumS=Hist_to_Psum(HistS);

//----------------------------------------------------------------------------------------------------

Hash_number = pow(2,FirstHash_number);

my_Job_list=Job_list_init();
args=malloc(sizeof(Sheduler_values));
args->shutdown = 0;
args->my_Job_list = my_Job_list;
sem_init(&sem, 0, 0);

for ( i=0; i<num_threads; i++ ){
  if ( err=pthread_create(&thread_pool[i],NULL,NewRel_thread,args) ){
    perror ("pthread_create");
    exit(1);
  }
}



current_num_tuples=relR->num_tuples;
args->PsumR = PsumR;
args->NewRelR = malloc(sizeof(relation));
args->NewRelR->num_tuples = current_num_tuples;
args->NewRelR->tuples = malloc(current_num_tuples*(sizeof(tuple)));
limits_arrayR=calculate_limits(current_num_tuples);

for ( i=0; i<num_threads; i++ ){
  newJob = malloc(sizeof(Job));
  newJob->id = 0;
  newJob->my_limits = &limits_arrayR[i];
  newJob->PsumR = PsumR;
  newJob->relR = relR;
  newJob->next = NULL;
  if ( err=pthread_mutex_lock(&mtx_xd) ){
    perror("pthread_mutex_lock");
    exit(1) ;
  }
  push_Job(my_Job_list,newJob);
  if ( err=pthread_mutex_unlock(&mtx_xd) ){
    perror("pthread_mutex_lock");
    exit(1) ;
  }
  free(newJob);
  sem_post(&sem);
}


current_num_tuples=relS->num_tuples;
args->PsumS = PsumS;
args->NewRelS = malloc(sizeof(relation));
args->NewRelS->num_tuples = current_num_tuples;
args->NewRelS->tuples = malloc(current_num_tuples*(sizeof(tuple)));
limits_arrayS=calculate_limits(current_num_tuples);

for ( i=0; i<num_threads; i++ ){
  newJob = malloc(sizeof(Job));
  newJob->id = 1;
  newJob->my_limits = &limits_arrayS[i];
  newJob->PsumS = PsumS;
  newJob->relS = relS;
  newJob->next = NULL;
  if ( err=pthread_mutex_lock(&mtx_xd) ){
    perror("pthread_mutex_lock");
    exit(1) ;
  }
  push_Job(my_Job_list,newJob);
  if ( err=pthread_mutex_unlock(&mtx_xd) ){
    perror("pthread_mutex_lock");
    exit(1) ;
  }
  free(newJob);
  sem_post(&sem);
}

args->shutdown = 1;

for ( i=0; i<num_threads; i++ ){
  sem_post(&sem);
}
for ( i=0; i<num_threads; i++ ){
  if ( err=pthread_join(thread_pool[i],NULL)) {
    perror ("pthread_join");
  }
}
free(my_Job_list);
free(limits_arrayR);
free(limits_arrayS);

//----------------------------------

//----------------------------------------------------------------------------------------------------

  int sizeR,sizeS; // sizeR - sizeS megethos kathe bucket


  ///------------- print buckets for debug  ------------///
  //printf("---------------------buckets relR------------------------------\n");
  //print_buckets(Hash_number,HistR,relNewR);
  //printf("-----------------------end buckets relR-------------------------\n");
  //printf("---------------------buckets rels------------------------------\n");
  //print_buckets(Hash_number,HistS,relNewS);
  //printf("-----------------------end buckets relS-------------------------\n");
  /// -------------------------------------------------///

  free(PsumR);
  free(PsumS);

  PsumR=Hist_to_Psum(HistR);
  PsumS=Hist_to_Psum(HistS);


  my_Job_list=Job_list_init();

  args->shutdown = 0;

  current_num_tuples=relR->num_tuples;
  args->PsumR = PsumR;
  args->PsumS = PsumS;
  args->Hist = HistR;
  args->Hist2 = HistS;

  args->Result = result_init();

  args->my_Job_list = my_Job_list;

  sem_init(&sem, 0, 0);

  for ( i=0; i<num_threads; i++ ){
    if ( err=pthread_create(&thread_pool[i],NULL,Bucket_thread,args) ){
      perror ("pthread_create");
      exit(1);
    }
  }

  for ( i=0; i<Hash_number; i++ ){
    newJob = malloc(sizeof(Job));
    newJob->bucket_index = i;
    newJob->next = NULL;
    if ( err=pthread_mutex_lock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    push_Job(my_Job_list,newJob);
    if ( err=pthread_mutex_unlock(&mtx_xd) ){
      perror("pthread_mutex_lock");
      exit(1) ;
    }
    sem_post(&sem);

    free(newJob);
  }



  args->shutdown = 1;
  for ( i=0; i<num_threads; i++ ){
    sem_post(&sem);
  }
  for ( i=0; i<num_threads; i++ ){
    if ( err=pthread_join(thread_pool[i],NULL)) {
      perror ("pthread_join");
    }
  }

  if ( err=pthread_mutex_destroy(&mtx_xd) ) {
    perror("pthread_mutex_destroy xd");
    exit(1) ;
  }

  if ( err=pthread_mutex_destroy(&mtx_write) ) {
    perror("pthread_mutex_destroy write");
    exit(1) ;
  }

  free(my_Job_list);
  free(args->NewRelR->tuples);
  free(args->NewRelS->tuples);
  free(args->NewRelR);
  free(args->NewRelS);
  sem_destroy(&sem);
  free(HistR);
  free(HistS);
  free(PsumR);
  free(PsumS);
  free(thread_pool);
  result *Result = args->Result;
  free(args);
  return Result;
}
