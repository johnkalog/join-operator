#include "functions.h"
#include <math.h>

typedef struct typeHist {
  uint32_t box;
  uint32_t num;
}typeHist;

uint32_t HashFunction(int32_t ,int );
relation *FirstHash(relation*); //dhmioyrgei R'

result* RadixHashJoin(relation *relR, relation *relS) {


  relation *relNewR = FirstHash(relR);
  relation *relNewS = FirstHash(relS);

  int i;
  for(i=0;i<relNewR->num_tuples;i++) {
    printf("key: %u   payload: %u\n",relNewR->tuples[i].key,relNewR->tuples[i].payload );
  }

  free_memory(relNewS);
  free_memory(relNewR);
  return NULL; //prosorino
}

relation *FirstHash(relation* relR) {

  int n = 1;
  int sizeHist = pow(2,n);
  relation *NewRel = malloc(sizeof(relation));
  NewRel->num_tuples = relR->num_tuples;
  NewRel->tuples = malloc(NewRel->num_tuples*sizeof(tuple));

  typeHist *Hist = malloc(sizeHist*sizeof(typeHist));
  //bzero(Hist,sizeHist);
  int i;
  for(i=0;i<sizeHist;i++) {
    Hist[i].box = i; //arxikopoihsh
    Hist[i].num = 0;
  }

  for(i=0;i<relR->num_tuples;i++) {
    uint32_t box = HashFunction(relR->tuples[i].payload,n);
    Hist[box].num++;
    //printf("%u\n",box );
  }

/*
  for(i=0;i<sizeHist;i++) {
    printf("%u   is   %u\n",Hist[i].box, Hist[i].num );
  }
*/
  typeHist *Psum = malloc(sizeHist*sizeof(typeHist));
  Psum[0].box = Hist[0].box;
  Psum[0].num = 0;
  for(i=1;i<sizeHist;i++) {
    Psum[i].box = Hist[i].box;
    Psum[i].num = Psum[i-1].num + Hist[i-1].num;

  }

  //dhmiourgia relNewR
  for(i=0;i<relR->num_tuples;i++) {
    uint32_t box = HashFunction(relR->tuples[i].payload,n);
    NewRel->tuples[Psum[box].num].payload = relR->tuples[i].payload;
    NewRel->tuples[Psum[box].num].key = relR->tuples[i].key;
    Psum[box].num++;
  }


  free(Hist); //isws prosorino
  free(Psum); //isws prosorino
  return NewRel;
}


uint32_t HashFunction(int32_t value,int n) {
  uint32_t temp = value << (sizeof(int32_t)*8)-n;
  temp = temp >> (sizeof(int32_t)*8)-n;
  //printf("%u\n",temp );
  return temp;
}
