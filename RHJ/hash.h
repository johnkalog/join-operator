#include <math.h>
#include "list_result.h"
#define num_threads 4

#define FirstHash_number 9
#define SecondHash_number 11

typedef struct tuple{
  int32_t key;
  uint64_t payload;
} tuple;

typedef struct relation{
  tuple *tuples;
  uint64_t num_tuples;
} relation;


typedef struct typeHist {
  uint32_t box; // aritmos koutiou, meta apo hash function
  uint32_t num; // analoga Psum - Hist
}typeHist; // idio gia Hist kai Psum

typedef struct HashBucket{
  int *chain,*bucket;
}HashBucket;


void relation_creation(relation *A,relation *B,char *argv[]);
void relation_print(relation *);
void print_buckets(int,typeHist *,relation *);
uint32_t FirstHashFunction(int32_t ,int );
uint32_t SecondHashFunction(int32_t ,int ,int);
relation *FirstHash(relation*,typeHist **); //dhmioyrgei R'
void SecondHash(uint32_t,relation *relNewR,int,HashBucket *);
void Scan_Buckets(result *,HashBucket*,relation*,relation*,int,int,int,int,int);
result* Join(relation *, relation *);
result* RadixHashJoin(relation *relR, relation *relS);
void free_hash_bucket(HashBucket *);
void free_memory(relation *A);

result *NoneNull(relation *,relation *);
result *Simple_Scan(relation *,uint64_t,char);
result *Simple_Scan_Tables(relation *,relation *);
typeHist *Rel_to_Hist(relation *,int,int);
typeHist *Hist_to_Psum(typeHist *);
