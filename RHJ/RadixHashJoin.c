#include <math.h>
#include "ThreadFunctions.h"

result* Join(relation *relR, relation *relS) {

  result *Result=result_init();

  int i,j;
  for ( i=0; i<relR->num_tuples; i++ ){
    for ( j=0; j<relS->num_tuples; j++ ){
      if ( relR->tuples[i].payload==relS->tuples[j].payload ){
        insert(Result,relR->tuples[i].key,relS->tuples[j].key);
      }
    }
  }
  return Result;


}

result* RadixHashJoin(relation *relR, relation *relS) {
  // returns result
  // result: InfoNode se tupo listas pou kathe kombos exei ena 2d array
  int i;
  result *Result=result_init(); // arxikopoihsh result

//----------------------------------------------------------------------------------------------------
  typeHist **HistR_Array,**HistS_Array;
  // dhmiourgia newn pinakwn relation
  // FirstHash dhmiourgei kai to Hist
  HistR_Array = malloc(num_threads*sizeof(typeHist *));
  HistS_Array = malloc(num_threads*sizeof(typeHist *));

  limits *limits_arrayR;
  limits_arrayR = malloc(num_threads*sizeof(limits));
  int current_num_tuples,new_end,current_threads;
  current_num_tuples=relR->num_tuples;
  new_end=0;
  current_threads=num_threads;
  for ( i=0; i<num_threads; i++ ){
    int population=(int)ceil((double)current_num_tuples/(double)current_threads);
    limits_arrayR[i].start = new_end;
    limits_arrayR[i].end = new_end+population;
    current_num_tuples -= population;
    new_end += population;
    current_threads --;
  }
  for ( i=0; i<num_threads; i++ ){
    HistR_Array[i] = Rel_to_Hist(relR,limits_arrayR[i].start,limits_arrayR[i].end);
  }

  limits *limits_arrayS;
  limits_arrayS = malloc(num_threads*sizeof(limits));
  current_num_tuples=relS->num_tuples;
  new_end=0;
  current_threads=num_threads;
  for ( i=0; i<num_threads; i++ ){
    int population=(int)ceil((double)current_num_tuples/(double)current_threads);
    limits_arrayS[i].start = new_end;
    limits_arrayS[i].end = new_end+population;
    current_num_tuples -= population;
    new_end += population;
    current_threads --;
  }
  for ( i=0; i<num_threads; i++ ){
    HistS_Array[i] = Rel_to_Hist(relS,limits_arrayS[i].start,limits_arrayS[i].end);
  }

  int Hash_number = pow(2,FirstHash_number);  //arithmos twn bucket
  typeHist *HistR,*HistS;

  int j;
  HistR = malloc(Hash_number*sizeof(typeHist));

  for ( i=0; i<Hash_number; i++ ){
    HistR[i].box = i;
    HistR[i].num = 0;
    for ( j=0; j<num_threads; j++ ){
      HistR[i].num += HistR_Array[j][i].num;
    }
  }

  HistS = malloc(Hash_number*sizeof(typeHist));
  for ( i=0; i<Hash_number; i++ ){
    HistS[i].box = i;
    HistS[i].num = 0;
    for ( j=0; j<num_threads; j++ ){
      HistS[i].num += HistS_Array[j][i].num;
    }
  }

  for ( i=0; i<num_threads; i++ ){
    free(HistR_Array[i]);
    free(HistS_Array[i]);
  }
  free(HistR_Array);
  free(HistS_Array);


//----------------------------------------------------------------------------------------------------

  typeHist *PsumR=Hist_to_Psum(HistR);
  typeHist *PsumS=Hist_to_Psum(HistS);

//----------------------------------------------------------------------------------------------------

relation *relNewR = malloc(sizeof(relation));
relNewR->num_tuples = relR->num_tuples;
relNewR->tuples = malloc(relNewR->num_tuples*sizeof(tuple));
for ( i=0; i<num_threads; i++ ){
  change_part_relation(relR,relNewR,&limits_arrayR[i],PsumR);
}
free(limits_arrayR);
free(PsumR);

relation *relNewS = malloc(sizeof(relation));
relNewS->num_tuples = relS->num_tuples;
relNewS->tuples = malloc(relNewS->num_tuples*sizeof(tuple));
for ( i=0; i<num_threads; i++ ){
  change_part_relation(relS,relNewS,&limits_arrayS[i],PsumS);
}
free(limits_arrayS);
free(PsumS);

//----------------------------------------------------------------------------------------------------

  // typeHist *H;
  // relation *relNewR1 = FirstHash(relR,&H);
  // relation *relNewS1 = FirstHash(relS,&HistS);

  int sizeR,sizeS; // sizeR - sizeS megethos kathe bucket
  int current_indexR=0; // current_index deixnei thn arxh tou kathe bucket sto relation
  int current_indexS=0;


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

// result* RadixHashJoin(relation *relR, relation *relS) {
//   // returns result
//   // result: InfoNode se tupo listas pou kathe kombos exei ena 2d array
//
//   result *Result=result_init(); // arxikopoihsh result
//
//   typeHist *HistR,*HistS;
//   // dhmiourgia newn pinakwn relation
//   // FirstHash dhmiourgei kai to Hist
//   relation *relNewR = FirstHash(relR,&HistR);
//   relation *relNewS = FirstHash(relS,&HistS);
//
//   int i,sizeR,sizeS; // sizeR - sizeS megethos kathe bucket
//   int current_indexR=0; // current_index deixnei thn arxh tou kathe bucket sto relation
//   int current_indexS=0;
//
//   int Hash_number = pow(2,FirstHash_number);  //arithmos twn bucket
//
//   ///------------- print buckets for debug  ------------///
//   //printf("---------------------buckets relR------------------------------\n");
//   //print_buckets(Hash_number,HistR,relNewR);
//   //printf("-----------------------end buckets relR-------------------------\n");
//   //printf("---------------------buckets rels------------------------------\n");
//   //print_buckets(Hash_number,HistS,relNewS);
//   //printf("-----------------------end buckets relS-------------------------\n");
//   /// -------------------------------------------------///
//
//   int sizeBucket=pow(2,SecondHash_number);
//   HashBucket *TheHashBucket=malloc(sizeof(HashBucket));
//   TheHashBucket->chain = NULL;
//   TheHashBucket->bucket = malloc(sizeBucket*sizeof(int));
//
//   for ( i=0; i<Hash_number; i++ ) {
//     sizeR = HistR[i].num; // current size tou bucket
//     sizeS = HistS[i].num;
//     if ( sizeR<=sizeS ){
//       // ean bucket R < bucket S (=)
//       // printf("\n\nscan S\n");
//       // Second Hash dhmiourgei to Chain kai to bucket sto struct TheHashBucket
//       SecondHash(sizeR,relNewR,current_indexR,TheHashBucket);
//       Scan_Buckets(Result,TheHashBucket,relNewR,relNewS,current_indexR,current_indexS,sizeR,sizeS,0);
//     }
//     else{
//       // ean bucket S < bucket R
//       // printf("\n\nscan R\n");
//       SecondHash(sizeS,relNewS,current_indexS,TheHashBucket);
//       Scan_Buckets(Result,TheHashBucket,relNewS,relNewR,current_indexS,current_indexR,sizeS,sizeR,1);
//     }
//     /////////////
//     current_indexR += HistR[i].num;  //arxh tou bucket
//     current_indexS += HistS[i].num;
//     free(TheHashBucket->chain);
//   }
//
//   free_memory(relNewR);
//   free_memory(relNewS);
//   free(TheHashBucket->bucket);
//   free(TheHashBucket);
//   free(HistR);
//   free(HistS);
//   return Result;
// }
