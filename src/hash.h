#include "list_result.h"

#define FirstHash_number 9
#define SecondHash_number 11

typedef struct tuple{
  int32_t key;
  int32_t payload;
} tuple;

typedef struct relation{
  tuple *tuples;
  uint32_t num_tuples;
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
void Scan_Buckets(result *,HashBucket*,relation*,relation*,int,int,int,int);
result* RadixHashJoin(relation *relR, relation *relS);
void free_hash_bucket(HashBucket *);
void free_memory(relation *A);
