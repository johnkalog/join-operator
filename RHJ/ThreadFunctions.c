#include "ThreadFunctions.h"

limits *calculate_limits(int num_tuples){
  limits *limits_arrayR=malloc(num_threads*sizeof(limits));
  int i,new_end,current_threads,current_num_tuples=num_tuples;
  new_end=0;
  current_threads=num_threads;
  for ( i=0; i<num_threads; i++ ){
    int population=(int)ceil((double)current_num_tuples/(double)current_threads);
    limits_arrayR[i].start = new_end;
    limits_arrayR[i].end = new_end+population;
    current_num_tuples -= population;
    new_end += population;
    current_threads --;
  }
  return limits_arrayR;
}

typeHist *Rel_to_Hist(relation *R,int start,int end){
  int sizeHist = pow(2,FirstHash_number);

  typeHist *Hist = malloc(sizeHist*sizeof(typeHist));

  int i;
  for(i=0;i<sizeHist;i++) {

    Hist[i].box = i;
    Hist[i].num = 0;
  }

  for(i=start;i<end;i++) {

    uint32_t box = FirstHashFunction(R->tuples[i].payload,FirstHash_number);
    Hist[box].num++;
  }
  return Hist;
}

typeHist *Hist_to_Psum(typeHist *Hist){
  int i;
  int sizeHist = pow(2,FirstHash_number);

  typeHist *Psum = malloc(sizeHist*sizeof(typeHist));
  Psum[0].box = Hist[0].box;
  Psum[0].num = 0;
  for(i=1;i<sizeHist;i++) {
    Psum[i].box = Hist[i].box;
    Psum[i].num = Psum[i-1].num + Hist[i-1].num;
  }
  return Psum;
}

void change_part_relation(relation *relR,relation *relNewR,limits *my_limits,typeHist *Psum){
  int i;

  for(i=my_limits->start;i<my_limits->end;i++) {
    uint32_t box = FirstHashFunction(relR->tuples[i].payload,FirstHash_number);

    relNewR->tuples[Psum[box].num].payload = relR->tuples[i].payload;
    relNewR->tuples[Psum[box].num].key = relR->tuples[i].key;
    Psum[box].num++;
  }
}

void one_bucket_join(int index,result *Result,HashBucket *TheHashBucket,typeHist *HistR,typeHist *HistS,typeHist *PsumR,typeHist *PsumS,relation *relNewR,relation *relNewS){

  int sizeR = HistR[index].num; // current size tou bucket
  int sizeS = HistS[index].num;
  if ( sizeR<=sizeS ){
    SecondHash(sizeR,relNewR,PsumR[index].num,TheHashBucket);
    Scan_Buckets(Result,TheHashBucket,relNewR,relNewS,PsumR[index].num,PsumS[index].num,sizeR,sizeS,0);
  }
  else{
    SecondHash(sizeS,relNewS,PsumS[index].num,TheHashBucket);
    Scan_Buckets(Result,TheHashBucket,relNewS,relNewR,PsumS[index].num,PsumR[index].num,sizeS,sizeR,1);
  }
  free(TheHashBucket->chain);
}
