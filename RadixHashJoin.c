#include "functions.h"
#include "hash.h"
#include <math.h>


result* RadixHashJoin(relation *relR, relation *relS) {

  result *Result=result_init();

  typeHist *HistR,*HistS;
  relation *relNewR = FirstHash(relR,&HistR);
  relation *relNewS = FirstHash(relS,&HistS);

  int i,sizeR,sizeS;
  int current_indexR=0;
  int current_indexS=0;

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
      Scan_Buckets(Result,fullBucket,relNewR,relNewS,current_indexR,current_indexS,sizeR,sizeS);
    }
    else{
      printf("\n\nscan R\n");
      fullBucket=SecondHash(sizeS,relNewS,current_indexS);
      Scan_Buckets(Result,fullBucket,relNewS,relNewR,current_indexS,current_indexR,sizeS,sizeR);
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
  return Result; //prosorino
}
