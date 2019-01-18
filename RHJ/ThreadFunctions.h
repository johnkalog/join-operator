#include "hash.h"
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>

//pthread_mutex_t mtx_forlist;
//pthread_mutex_t mtx_forlist1;
pthread_mutex_t mtx_write;
pthread_mutex_t mtx_xd;
//pthread_cond_t cv_nonempty;
sem_t sem;


typedef struct limits{
  int start,end;  //means end-1
}limits;

typedef struct Sheduler_values{
  int shutdown;
  typeHist *Hist,*Hist2;
  typeHist *PsumR,*PsumS;
  relation *NewRelR,*NewRelS;
  result *Result;
  struct Job_list *my_Job_list;
}Sheduler_values;

typedef struct Job{
  int id,bucket_index;
  relation *relR,*relS;
  limits *my_limits;
  typeHist *HistR,*HistS,*PsumR,*PsumS;
  struct Job *next;
}Job;

typedef struct Job_list{
  int size;
  Job *Head,*Tail;
}Job_list;

limits *calculate_limits(int );
typeHist *Rel_to_Hist(relation *,int,int);
typeHist *Hist_to_Psum(typeHist *);
void change_part_relation(relation *,relation *,limits *,typeHist *);
void change_part_relation2(relation *,relation *,int ,int ,typeHist *);
void one_bucket_join(int ,result *,HashBucket *,typeHist *,typeHist *,typeHist *,typeHist *,relation *,relation *);

void JobSheduler(int);

void* Histogram_thread(void*);
void* NewRel_thread(void*);
void* Bucket_thread(void*);

Job_list *Job_list_init();
void push_Job(Job_list *,Job *);
Job *pop_Job(Job_list *);
