#include "functions.h"
#include <math.h>

#define FirstHash_number 2
#define SecondHash_number 3

typedef struct typeHist {
  uint32_t box;
  uint32_t num;
}typeHist;

typedef struct HashBucket{
  int *chain,*bucket;
}HashBucket;

void print_buckets(int,typeHist *,relation *);
uint32_t HashFunction(int32_t ,int );
relation *FirstHash(relation*,typeHist **); //dhmioyrgei R'
HashBucket *SecondHash(uint32_t,relation *relNewR,int);
void free_hash_bucket(HashBucket *);
void Scan_Buckets(HashBucket*,relation*,relation*,int,int,int,int);

result* RadixHashJoin(relation *relR, relation *relS) {

  typeHist *HistR,*HistS;
  relation *relNewR = FirstHash(relR,&HistR);
  relation *relNewS = FirstHash(relS,&HistS);

  int i,sizeR,sizeS;
  int current_indexR=0;
  int current_indexS=0;
  // for(i=0;i<relNewR->num_tuples;i++) {
  //   printf("key: %u   payload: %u\n",relNewR->tuples[i].key,relNewR->tuples[i].payload );
  // }
  // for ( i=0; i<4; i++ ){
  //   printf("cwcwecwecw %d\n",HistR[i].num);
  // }
  int Hash_number = pow(2,FirstHash_number);
  printf("---------------------buckets relR------------------------------\n");
  print_buckets(Hash_number,HistR,relNewR);
  printf("-----------------------end buckets relR-------------------------\n");
  printf("---------------------buckets rels------------------------------\n");
  print_buckets(Hash_number,HistS,relNewS);
  printf("-----------------------end buckets relS-------------------------\n");
  HashBucket *fullBucket;
  for ( i=0; i<Hash_number; i++ ){  //size tou bucket
    sizeR = HistR[i].num;
    sizeS = HistS[i].num;
    if ( sizeR<=sizeS ){
      printf("\n\nscan S\n");
      fullBucket=SecondHash(sizeR,relNewR,current_indexR);
      Scan_Buckets(fullBucket,relNewR,relNewS,current_indexR,current_indexS,sizeR,sizeS);
    }
    else{
      printf("\n\nscan R\n");
      fullBucket=SecondHash(sizeS,relNewS,current_indexS);
      Scan_Buckets(fullBucket,relNewS,relNewR,current_indexS,current_indexR,sizeS,sizeR);
    }
    /////////////
    current_indexR += HistR[i].num;  //arxh tou bucket
    current_indexS += HistS[i].num;
    free_hash_bucket(fullBucket);
  }

  printf("sssssssssssssssssssssfwe %d\n",bufferRows);
  free_memory(relNewR);
  free_memory(relNewS);
  free(HistR);
  free(HistS);
  return NULL; //prosorino
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


void Scan_Buckets(HashBucket *fullBucket,relation *RelHash,relation *RelScan,int startHash,int startScan,int sizeHash,int sizeScan){
///////////
  int  i,bucket_index,chain_index;
  for(i=0;i<sizeScan;i++){
    int32_t payload = RelScan->tuples[startScan+i].payload;
    bucket_index = HashFunction(payload,SecondHash_number);
  //  printf("scan buckets -> %d\n",bucket_index );
    printf("payload %d\n",payload );
    if(fullBucket->bucket[bucket_index]!=-1){
      //// ------  elegxos ---------------
      chain_index = fullBucket->bucket[bucket_index];

      while(chain_index != -1) {
        //elegxos
        if(payload == RelHash->tuples[startHash + chain_index].payload) {
          printf("I found something %d \n",payload );
        }
        chain_index = fullBucket->chain[chain_index];

      }
      //telos?
    }
  }

}

void free_hash_bucket(HashBucket *fullBucket){
  free(fullBucket->chain);
  free(fullBucket->bucket);
  free(fullBucket);
}

HashBucket *SecondHash(uint32_t size,relation *relNew,int start_index){
  int i,bucket_index,previous_last,tmp; //for mod
  int sizeBucket=pow(2,SecondHash_number);
  HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
  TheHashBucket->chain = malloc(size*sizeof(int));
  TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));
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

  //int n = 2;
  int sizeHist = pow(2,FirstHash_number);
  relation *NewRel = malloc(sizeof(relation));
  NewRel->num_tuples = relR->num_tuples;
  NewRel->tuples = malloc(NewRel->num_tuples*sizeof(tuple));

  *Hist = malloc(sizeHist*sizeof(typeHist));
  //bzero(Hist,sizeHist);
  int i;
  for(i=0;i<sizeHist;i++) {
    (*Hist)[i].box = i; //arxikopoihsh
    (*Hist)[i].num = 0;
  }

  for(i=0;i<relR->num_tuples;i++) {
    uint32_t box = HashFunction(relR->tuples[i].payload,FirstHash_number);
    (*Hist)[box].num++;
    //printf("%u\n",box );
  }


  for(i=0;i<sizeHist;i++) {
    printf("%u   is   %u\n",(*Hist)[i].box, (*Hist)[i].num );
  }

  typeHist *Psum = malloc(sizeHist*sizeof(typeHist));
  Psum[0].box = (*Hist)[0].box;
  Psum[0].num = 0;
  for(i=1;i<sizeHist;i++) {
    Psum[i].box = (*Hist)[i].box;
    Psum[i].num = Psum[i-1].num + (*Hist)[i-1].num;

  }

  //dhmiourgia relNewR
  for(i=0;i<relR->num_tuples;i++) {
    uint32_t box = HashFunction(relR->tuples[i].payload,FirstHash_number);
    NewRel->tuples[Psum[box].num].payload = relR->tuples[i].payload;
    NewRel->tuples[Psum[box].num].key = relR->tuples[i].key;
    Psum[box].num++;
  }

  // free(Hist); //isws prosorino
  free(Psum); //isws prosorino
  return NewRel;
}


uint32_t HashFunction(int32_t value,int n) {
  uint32_t temp = value << (sizeof(int32_t)*8)-n;
  temp = temp >> (sizeof(int32_t)*8)-n;
  //printf("%u\n",temp );
  return temp;
}
