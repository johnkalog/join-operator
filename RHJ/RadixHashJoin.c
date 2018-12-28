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
  result *Result=result_init(); // arxikopoihsh result

  // pthread_mutex_init(&mtx_forlist,NULL);
  // pthread_mutex_init(&mtx_write,NULL);
  pthread_mutex_t mtx_forlist= PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_t mtx_forlist1= PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_t mtx_write= PTHREAD_MUTEX_INITIALIZER;

  // pthread_mutex_t mtx_forlist1 = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_init(&cv_nonempty,NULL);
  pthread_t err;
  pthread_t *thread_pool=malloc(num_threads*sizeof(pthread_t));
  //----------------------------------------------------------------------------------------------------
  // typeHist **HistR_Array,**HistS_Array;
  //
  // HistR_Array = malloc(num_threads*sizeof(typeHist *));
  // HistS_Array = malloc(num_threads*sizeof(typeHist *));

  Job_list *my_Job_list=Job_list_init();
  Sheduler_values *args=malloc(sizeof(Sheduler_values));
  args->shutdown = 0;
  args->my_Job_list = my_Job_list;

  for ( i=0; i<num_threads; i++ ){
    if ( err=pthread_create(&thread_pool[i],NULL,thread_1,args) ){
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
    newJob->id = i;
    newJob->my_limits = &limits_arrayR[i];
    newJob->relR = relR;
    newJob->next = NULL;
    push_Job(my_Job_list,newJob);
    free(newJob);
    printf("I pushed a Jod\n");
    pthread_cond_signal(&cv_nonempty);
  }
  HistR = args->Hist;

  args->Hist = malloc(Hash_number*sizeof(typeHist));

  for ( i=0; i<Hash_number; i++ ){
    args->Hist[i].box = i;
    args->Hist[i].num = 0;
  }

  current_num_tuples=relS->num_tuples;
  limits *limits_arrayS=calculate_limits(current_num_tuples);

  for ( i=0; i<num_threads; i++ ){
    newJob = malloc(sizeof(Job));
    newJob->id = 1;
    newJob->my_limits = &limits_arrayS[i];
    newJob->relR = relS;
    newJob->next = NULL;
    push_Job(my_Job_list,newJob);
    free(newJob);
    pthread_cond_signal(&cv_nonempty);
  }
  HistS = args->Hist;

  args->shutdown = 1;
  pthread_cond_broadcast(&cv_nonempty);

  // for ( i=0; i<num_threads; i++ ){
  //   free(HistR_Array[i]);
  //   free(HistS_Array[i]);
  // }
  // free(HistR_Array);
  // free(HistS_Array);

  for ( i=0; i<num_threads; i++ ){
    if ( err=pthread_join(thread_pool[i],NULL)) {
      perror ("pthread_join");
    }
  }

  if ( err=pthread_mutex_destroy(&mtx_forlist) ) {
    perror("pthread_mutex_destroy");
    exit(1) ;
  }
  if ( err=pthread_mutex_destroy(&mtx_write) ) {
    perror("pthread_mutex_destroy");
    exit(1) ;
  }
  if ( err=pthread_cond_destroy(&cv_nonempty) ) {
    perror("pthread_cond_destroy");
    exit(1) ;
  }

  free(thread_pool);
  free(args);
  free(my_Job_list);

//----------------------------------------------------------------------------------------------------

  typeHist *PsumR=Hist_to_Psum(HistR);
  typeHist *PsumS=Hist_to_Psum(HistS);

//----------------------------------------------------------------------------------------------------

relation *relNewR = malloc(sizeof(relation));
relNewR->num_tuples = relR->num_tuples;
relNewR->tuples = malloc(relNewR->num_tuples*sizeof(tuple));
for ( i=0; i<num_threads; i++ ){
  change_part_relation(relR,relNewR,&limits_arrayR[i],PsumR);
}
free(limits_arrayR);
// free(PsumR);

relation *relNewS = malloc(sizeof(relation));
relNewS->num_tuples = relS->num_tuples;
relNewS->tuples = malloc(relNewS->num_tuples*sizeof(tuple));
for ( i=0; i<num_threads; i++ ){
  change_part_relation(relS,relNewS,&limits_arrayS[i],PsumS);
}
free(limits_arrayS);
// free(PsumS);

//----------------------------------------------------------------------------------------------------

  // typeHist *H;
  // relation *relNewR1 = FirstHash(relR,&H);
  // relation *relNewS1 = FirstHash(relS,&HistS);

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

  int sizeBucket=pow(2,SecondHash_number);
  HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
  TheHashBucket->chain = NULL;
  TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));

  for ( i=0; i<Hash_number; i++ ) {
    one_bucket_join(i,Result,TheHashBucket,HistR,HistS,PsumR,PsumS,relNewR,relNewS);
  }

  free_memory(relNewR);
  free_memory(relNewS);
  free(TheHashBucket->bucket);
  free(TheHashBucket);
  free(HistR);
  free(HistS);
  free(PsumR);
  free(PsumS);
  return Result;
}

// result* RadixHashJoin(relation *relR, relation *relS) {
//   // returns result
//   // result: InfoNode se tupo listas pou kathe kombos exei ena 2d array
//
//   result *Result=result_init(); // arxikopoihsh result
//
//   typeHist *HistR,*HistS;
//   // dhmiourgia newn pinakwn relation
//   // FirstHash dhmiourgei kai to Hist
//   relation *relNewR = FirstHash(relR,&HistR);
//   relation *relNewS = FirstHash(relS,&HistS);
//
//   int i,sizeR,sizeS; // sizeR - sizeS megethos kathe bucket
//   int current_indexR=0; // current_index deixnei thn arxh tou kathe bucket sto relation
//   int current_indexS=0;
//
//   int Hash_number = pow(2,FirstHash_number);  //arithmos twn bucket
//
//   ///------------- print buckets for debug  ------------///
//   //printf("---------------------buckets relR------------------------------\n");
//   //print_buckets(Hash_number,HistR,relNewR);
//   //printf("-----------------------end buckets relR-------------------------\n");
//   //printf("---------------------buckets rels------------------------------\n");
//   //print_buckets(Hash_number,HistS,relNewS);
//   //printf("-----------------------end buckets relS-------------------------\n");
//   /// -------------------------------------------------///
//
//   int sizeBucket=pow(2,SecondHash_number);
//   HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
//   TheHashBucket->chain = NULL;
//   TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));
//
//   for ( i=0; i<Hash_number; i++ ) {
//     sizeR = HistR[i].num; // current size tou bucket
//     sizeS = HistS[i].num;
//     if ( sizeR<=sizeS ){
//       // ean bucket R < bucket S (=)
//       // printf("\n\nscan S\n");
//       // Second Hash dhmiourgei to Chain kai to bucket sto struct TheHashBucket
//       SecondHash(sizeR,relNewR,current_indexR,TheHashBucket);
//       Scan_Buckets(Result,TheHashBucket,relNewR,relNewS,current_indexR,current_indexS,sizeR,sizeS,0);
//     }
//     else{
//       // ean bucket S < bucket R
//       // printf("\n\nscan R\n");
//       SecondHash(sizeS,relNewS,current_indexS,TheHashBucket);
//       Scan_Buckets(Result,TheHashBucket,relNewS,relNewR,current_indexS,current_indexR,sizeS,sizeR,1);
//     }
//     /////////////
//     current_indexR += HistR[i].num;  //arxh tou bucket
//     current_indexS += HistS[i].num;
//     free(TheHashBucket->chain);
//   }
//
//   free_memory(relNewR);
//   free_memory(relNewS);
//   free(TheHashBucket->bucket);
//   free(TheHashBucket);
//   free(HistR);
//   free(HistS);
//   return Result;
// }
