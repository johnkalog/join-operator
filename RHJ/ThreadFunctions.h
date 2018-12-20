#include "hash.h"

typedef struct limits{
  int start,end;  //means end-1
}limits;

typeHist *Rel_to_Hist(relation *,int,int);
typeHist *Hist_to_Psum(typeHist *);
void change_part_relation(relation *,relation *,limits *,typeHist *);
