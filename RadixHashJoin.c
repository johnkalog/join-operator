#include "functions.h"
#include "hash.h"
#include <math.h>

#define FirstHash_number 2
#define SecondHash_number 3

result* RadixHashJoin(relation *relR, relation *relS) {
  // returns result
  // result: tupos listas pou kathe kombos exei ena 2d array

  result *Result=result_init(); // arxikopoihsh result

  typeHist *HistR,*HistS;
  // dhmiourgia newn pinakwn relation
  // FirstHash epistrfei kai to Hist
  relation *relNewR = FirstHash(relR,&HistR);
  relation *relNewS = FirstHash(relS,&HistS);

  int i,sizeR,sizeS; // sizeR - sizeS megethos kathe bucket
  int current_indexR=0; // current_index deixnei thn arxh tou kathe
  int current_indexS=0;

  int Hash_number = pow(2,FirstHash_number);

  ///------------- print buckets for debug  ------------///
  //printf("---------------------buckets relR------------------------------\n");
  //print_buckets(Hash_number,HistR,relNewR);
  //printf("-----------------------end buckets relR-------------------------\n");
  //printf("---------------------buckets rels------------------------------\n");
  //print_buckets(Hash_number,HistS,relNewS);
  //printf("-----------------------end buckets relS-------------------------\n");
  /// -------------------------------------------------///


  HashBucket *fullBucket;
  for ( i=0; i<Hash_number; i++ ){  //size tou bucket
    sizeR = HistR[i].num; // current size
    sizeS = HistS[i].num;
    if ( sizeR<=sizeS ){
      // ean bucket R < bucket S (=)
      // printf("\n\nscan S\n");
      // Second Hash dhmiourgei to Chain kai to bucket sto struct fullBucket
      fullBucket=SecondHash(sizeR,relNewR,current_indexR);
      Scan_Buckets(Result,fullBucket,relNewR,relNewS,current_indexR,current_indexS,sizeR,sizeS);
    }
    else{
      // ean bucket S < bucket R
      // printf("\n\nscan R\n");
      fullBucket=SecondHash(sizeS,relNewS,current_indexS);
      Scan_Buckets(Result,fullBucket,relNewS,relNewR,current_indexS,current_indexR,sizeS,sizeR);
    }
    /////////////
    current_indexR += HistR[i].num;  //arxh tou bucket
    current_indexS += HistS[i].num;
    free_hash_bucket(fullBucket);
  }

  //printf("bufferRows: %d\n",bufferRows);
  free_memory(relNewR);
  free_memory(relNewS);
  free(HistR);
  free(HistS);
  return Result;
}

void print_buckets(int Hash_number,typeHist *Hist,relation *relNew){
  int i,j,size,current_index=0;
  for ( i=0; i<Hash_number; i++ ){
    size = Hist[i].num;
    printf("  Bucket %d box %d:\n",i,Hist[i].box);
    for ( int j=0; j<size; j++ ){
      printf("    value %d\n",relNew->tuples[current_index+j].payload);
    }
    current_index += Hist[i].num;
  }
}


void Scan_Buckets(result *Result,HashBucket *fullBucket,relation *RelHash,relation *RelScan,int startHash,int startScan,int sizeHash,int sizeScan){
  ///// scan bucket RelScan kai briskei ta koina me to fullBucket /////
  ///// apothikeuei ta rowids sto Result //////

  int  i,bucket_index,chain_index;
  for(i=0;i<sizeScan;i++){
    // payload timh stou RelScan
    int32_t payload = RelScan->tuples[startScan+i].payload,key=RelScan->tuples[startScan+i].key;
    bucket_index = HashFunction(payload,SecondHash_number);

    //printf("payload %d\n",payload );
    if(fullBucket->bucket[bucket_index]!=-1){ // uparxei tetoio stoixeio sto fullBucket
      chain_index = fullBucket->bucket[bucket_index];

      while(chain_index != -1) { // bres ola ta koina
        //elegxos
        if(payload == RelHash->tuples[startHash + chain_index].payload) {
          printf("I found something %d \n",payload );
          insert(Result,key,RelHash->tuples[startHash + chain_index].key);
        }
        chain_index = fullBucket->chain[chain_index];

      }
    }
  }

}

void free_hash_bucket(HashBucket *fullBucket){
  free(fullBucket->chain);
  free(fullBucket->bucket);
  free(fullBucket);
}

HashBucket *SecondHash(uint32_t size,relation *relNew,int start_index){
  // deutero Hash
  // dhmiourgia chain kai bucket (HashBucket)

  int i,bucket_index,previous_last,tmp; //for mod
  int sizeBucket=pow(2,SecondHash_number);
  HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
  TheHashBucket->chain = malloc(size*sizeof(int));
  TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));

  // arxikopoihsh me -1
  for ( i=0; i<sizeBucket; i++ ){
    TheHashBucket->bucket[i] = -1; //arxika -1
  }
  for(i=0; i<size;i++){
    TheHashBucket->chain[i] = -1; //arxika -1
  }
  for ( i=0; i<size; i++ ){
    bucket_index = HashFunction(relNew->tuples[start_index+i].payload,SecondHash_number);   //relNew->tuples[start_index+i].key%n;
     if ( TheHashBucket->bucket[bucket_index]==-1 ){
       TheHashBucket->bucket[bucket_index] = i;
       //TheHashBucket->chain[i-1] = 0; //san arithmhsh apo 1 kai to 0 na einai gia thn fhlwsh tou tipota
     }
      else{
        tmp = TheHashBucket->bucket[bucket_index];
        TheHashBucket->bucket[bucket_index] = i;
        TheHashBucket->chain[i] = tmp;
      }
     //   TheHashBucket->bucket[bucket_index] == i;
     //   TheHashBucket->chain[i-1] = previous_last-1;
     // }
  }

  return TheHashBucket;
}

relation *FirstHash(relation* relR,typeHist **Hist) {
  // prwto Hash
  // dhmiourgia Hist kai Psum


  int sizeHist = pow(2,FirstHash_number);
  // dhmiourgia neou relation
  //pou apothikeuei ta stoixeia me thn epithimhth seira
  relation *NewRel = malloc(sizeof(relation));
  NewRel->num_tuples = relR->num_tuples;
  NewRel->tuples = malloc(NewRel->num_tuples*sizeof(tuple));

  *Hist = malloc(sizeHist*sizeof(typeHist));

  int i;
  for(i=0;i<sizeHist;i++) {
    // arxikopoihsh
    (*Hist)[i].box = i;
    (*Hist)[i].num = 0;
  }

  for(i=0;i<relR->num_tuples;i++) {
    // gia kathe stoixeio des se poia kathgoria anoikei kai auksise
    // to Hist num ths analoghs kathgorias
    uint32_t box = HashFunction(relR->tuples[i].payload,FirstHash_number);
    (*Hist)[box].num++;
    //printf("%u\n",box );
  }

  // print Hist
  for(i=0;i<sizeHist;i++) {
    printf("%u   is   %u\n",(*Hist)[i].box, (*Hist)[i].num );
  }

  // dhmiourgia Psum
  typeHist *Psum = malloc(sizeHist*sizeof(typeHist));
  Psum[0].box = (*Hist)[0].box;
  Psum[0].num = 0;
  for(i=1;i<sizeHist;i++) {
    Psum[i].box = (*Hist)[i].box;
    Psum[i].num = Psum[i-1].num + (*Hist)[i-1].num;

  }

  //dhmiourgia relNewR
  for(i=0;i<relR->num_tuples;i++) {
    uint32_t box = HashFunction(relR->tuples[i].payload,FirstHash_number); // pou anoikei to kathe stoixeio
    // eisagwgh analoga me to Psum
    NewRel->tuples[Psum[box].num].payload = relR->tuples[i].payload;
    NewRel->tuples[Psum[box].num].key = relR->tuples[i].key;
    Psum[box].num++; // auskise th epomenh thesh tou Psum
  }

  free(Psum);
  return NewRel;
}


uint32_t HashFunction(int32_t value,int n) {
  // gyrnaei ta teleutaia n bits
  uint32_t temp = value << (sizeof(int32_t)*8)-n;
  temp = temp >> (sizeof(int32_t)*8)-n;
  //printf("%u\n",temp );
  return temp;
}
