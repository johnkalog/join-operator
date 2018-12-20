#include "ThreadFunctions.h"

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
