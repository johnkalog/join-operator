#include "hash.h"
#include <math.h>


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

  free_memory(relNewR);
  free_memory(relNewS);
  free(HistR);
  free(HistS);
  return Result;
}
