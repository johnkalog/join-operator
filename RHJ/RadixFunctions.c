#include "hash.h"


result *NoneNull(relation *rel1,relation *rel2) { //sthn periptwsh = duo sxesewn pou exoun ksanaginei join
  int i;
  result *Result = result_init();
  for(i=0;i<rel1->num_tuples;i++) {
    if(rel1->tuples[i].payload == rel2->tuples[i].payload) {
      insert(Result,rel1->tuples[i].key,-1);
    }
  }
  return Result;
}

result *Simple_Scan(relation *Rel,uint64_t value,char op){  //scan tou relation pou anaferetai se sugkekrimeno column kai save
    int i;
    result *Result=result_init();

    if(op == '='){

        for(i=0; i<Rel->num_tuples;i++){
            if( Rel->tuples[i].payload == value){
                insert(Result,Rel->tuples[i].key,-1); //-1 sthn deuterh thesi tou pinaka sto Result giati mono  mia sxesh xrhsimopoieitai
            }
        }

    }
    else if( op == '>'){

        for(i=0; i<Rel->num_tuples;i++){
            if( Rel->tuples[i].payload > value){
                insert(Result,Rel->tuples[i].key,-1);
            }
        }

    }
    else{

        for(i=0; i<Rel->num_tuples;i++){
            if( Rel->tuples[i].payload < value){
                insert(Result,Rel->tuples[i].key,-1);
            }
        }

    }

    return Result;
}

result *Simple_Scan_Tables(relation *Rel, relation *Rel2){  //sugkrish duo column idias sxeshs
    int i;
    result *Result=result_init();

    for(i=0; i<Rel->num_tuples; i++){

        if( Rel->tuples[i].payload == Rel2->tuples[i].payload ){
            insert(Result,Rel->tuples[i].key,-1);
        }

    }
    return Result;
}


void Scan_Buckets(result *Result,HashBucket *fullBucket,relation *RelHash,relation *RelScan,int startHash,int startScan,int sizeHash,int sizeScan,int first){
  ///// scan bucket RelScan kai briskei ta koina me to fullBucket /////
  ///// apothikeuei ta rowids sto Result //////

  int  i,bucket_index,chain_index;
  for(i=0;i<sizeScan;i++){
    // payload timh ston RelScan
    int32_t payload = RelScan->tuples[startScan+i].payload;
    int32_t key=RelScan->tuples[startScan+i].key;
    bucket_index = SecondHashFunction(payload,FirstHash_number,SecondHash_number);

    //printf("payload %d\n",payload );
    if(fullBucket->bucket[bucket_index]!=-1){ // uparxei tetoio stoixeio sto fullBucket diatrexontas to chain?
      chain_index = fullBucket->bucket[bucket_index];

      while(chain_index != -1) { // bres ola ta koina
        //elegxos
        //first==0 R is smaller
        if(payload == RelHash->tuples[startHash + chain_index].payload){
          if ( first==0 ){
            insert(Result,RelHash->tuples[startHash + chain_index].key,key);  //eisagwgh sto result
          }
          else if ( first==1 ){
            insert(Result,key,RelHash->tuples[startHash + chain_index].key);  //eisagwgh sto result
          }
        }

        chain_index = fullBucket->chain[chain_index];
      }
    }
  }
}

void free_hash_bucket(HashBucket *fullBucket){  //eleutherwsh xwrou
  free(fullBucket->chain);
  free(fullBucket->bucket);
  free(fullBucket);
}

void SecondHash(uint32_t size,relation *relNew,int start_index,HashBucket *TheHashBucket){
  // deutero Hash
  // dhmiourgia chain kai bucket (HashBucket)

  int i,bucket_index,previous_last,tmp;
  int sizeBucket=pow(2,SecondHash_number);
  TheHashBucket->chain = malloc(size*sizeof(int));

  for ( i=0; i<sizeBucket; i++ ){
    TheHashBucket->bucket[i] = -1; //arxika -1
  }
  for(i=0; i<size;i++){
    TheHashBucket->chain[i] = -1; //arxika -1
  }
  for ( i=0; i<size; i++ ){
    bucket_index = SecondHashFunction(relNew->tuples[start_index+i].payload,FirstHash_number,SecondHash_number);   //relNew->tuples[start_index+i].key%n;
     if ( TheHashBucket->bucket[bucket_index]==-1 ){
       TheHashBucket->bucket[bucket_index] = i;
     }
      else{
        tmp = TheHashBucket->bucket[bucket_index];
        TheHashBucket->bucket[bucket_index] = i;
        TheHashBucket->chain[i] = tmp;
      }
  }
}

relation *FirstHash(relation* relR,typeHist **Hist) {
  // prwto Hash
  // dhmiourgia Hist kai Psum


  int sizeHist = pow(2,FirstHash_number);
  // dhmiourgia neou relation
  //pou apothikeuei ta stoixeia me thn epithimhth seira organmena se buckets
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
    uint32_t box = FirstHashFunction(relR->tuples[i].payload,FirstHash_number);
    (*Hist)[box].num++;
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
    uint32_t box = FirstHashFunction(relR->tuples[i].payload,FirstHash_number); // pou anoikei to kathe stoixeio
    // eisagwgh analoga me to Psum
    NewRel->tuples[Psum[box].num].payload = relR->tuples[i].payload;
    NewRel->tuples[Psum[box].num].key = relR->tuples[i].key;
    Psum[box].num++; // auskise th epomenh thesh tou Psum
  }

  free(Psum);
  return NewRel;
}

uint32_t SecondHashFunction(int32_t value,int n1,int n2) {
  // gyrnaei ta  n2 bits amesws meta ta n1 apo aristera//
  uint32_t temp = value << (sizeof(int32_t)*8)-(n1+n2);
  temp = temp >> (sizeof(int32_t)*8)-n2;
  //printf("%u\n",temp );
  return temp;
}

uint32_t FirstHashFunction(int32_t value,int n) {
  // gyrnaei ta teleutaia n bits
  uint32_t temp = value << (sizeof(int32_t)*8)-n;
  temp = temp >> (sizeof(int32_t)*8)-n;
  //printf("%u\n",temp );
  return temp;
}

void print_buckets(int Hash_number,typeHist *Hist,relation *relNew){
  int i,j,size,current_index=0;
  for ( i=0; i<Hash_number; i++ ){
    size = Hist[i].num;
    printf("  Bucket %d box %d:\n",i,Hist[i].box);
    for ( int j=0; j<size; j++ ){
      printf("    value %ld\n",relNew->tuples[current_index+j].payload);
    }
    current_index += Hist[i].num;
  }
}
