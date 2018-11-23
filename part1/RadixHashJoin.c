#include "hash.h"
#include <math.h>


result* RadixHashJoin(relation *relR, relation *relS) {
  // returns result
  // result: InfoNode se tupo listas pou kathe kombos exei ena 2d array

  result *Result=result_init(); // arxikopoihsh result

  typeHist *HistR,*HistS;
  // dhmiourgia newn pinakwn relation
  // FirstHash dhmiourgei kai to Hist
  relation *relNewR = FirstHash(relR,&HistR);
  relation *relNewS = FirstHash(relS,&HistS);

  int i,sizeR,sizeS; // sizeR - sizeS megethos kathe bucket
  int current_indexR=0; // current_index deixnei thn arxh tou kathe bucket sto relation
  int current_indexS=0;

  int Hash_number = pow(2,FirstHash_number);  //arithmos twn bucket

  ///------------- print buckets for debug  ------------///
  //printf("---------------------buckets relR------------------------------\n");
  //print_buckets(Hash_number,HistR,relNewR);
  //printf("-----------------------end buckets relR-------------------------\n");
  //printf("---------------------buckets rels------------------------------\n");
  //print_buckets(Hash_number,HistS,relNewS);
  //printf("-----------------------end buckets relS-------------------------\n");
  /// -------------------------------------------------///

  int sizeBucket=pow(2,SecondHash_number);
  HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
  TheHashBucket->chain = NULL;
  TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));

  for ( i=0; i<Hash_number; i++ ) {
    sizeR = HistR[i].num; // current size tou bucket
    sizeS = HistS[i].num;
    if ( sizeR<=sizeS ){
      // ean bucket R < bucket S (=)
      // printf("\n\nscan S\n");
      // Second Hash dhmiourgei to Chain kai to bucket sto struct TheHashBucket
      SecondHash(sizeR,relNewR,current_indexR,TheHashBucket);
      Scan_Buckets(Result,TheHashBucket,relNewR,relNewS,current_indexR,current_indexS,sizeR,sizeS,0);
    }
    else{
      // ean bucket S < bucket R
      // printf("\n\nscan R\n");
      SecondHash(sizeS,relNewS,current_indexS,TheHashBucket);
      Scan_Buckets(Result,TheHashBucket,relNewS,relNewR,current_indexS,current_indexR,sizeS,sizeR,1);
    }
    /////////////
    current_indexR += HistR[i].num;  //arxh tou bucket
    current_indexS += HistS[i].num;
    free(TheHashBucket->chain);
  }

  free_memory(relNewR);
  free_memory(relNewS);
  free(TheHashBucket->bucket);
  free(TheHashBucket);
  free(HistR);
  free(HistS);
  return Result;
}
