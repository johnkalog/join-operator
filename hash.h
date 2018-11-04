typedef struct typeHist {
  uint32_t box; // aritmos koutiou, meta apo hash function
  uint32_t num; // analoga Psum - Hist
}typeHist; // idio gia Hist kai Psum

typedef struct HashBucket{
  int *chain,*bucket;
}HashBucket;

void print_buckets(int,typeHist *,relation *);
uint32_t HashFunction(int32_t ,int );
relation *FirstHash(relation*,typeHist **); //dhmioyrgei R'
HashBucket *SecondHash(uint32_t,relation *relNewR,int);
void free_hash_bucket(HashBucket *);
void Scan_Buckets(result *,HashBucket*,relation*,relation*,int,int,int,int);
result* RadixHashJoin(relation *relR, relation *relS);
