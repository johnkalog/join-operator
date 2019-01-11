// C program to print all permutations with duplicates allowed
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

typedef struct list{
    int *perm;
    struct list *next;
} list;

void push_list_perm(list *first,list *new){
  list *tmp=first;
  while ( tmp->next!=NULL ){
    tmp = tmp->next;
  }
  tmp->next = new;
}

list *pop_list_perm(list *my_list){
  if (my_list!=NULL ){
    list *tmp=my_list;
    my_list = my_list->next;
    return tmp;
  }
  return NULL;
}
/* Function to swap values at two pointers */
void swap(int *x, int *y)
{
    int temp;
    temp = *x;
    *x = *y;
    *y = temp;
}

/* Function to print permutations of string
   This function takes three parameters:
   1. String
   2. Starting index of the string
   3. Ending index of the string. */
// int factorial(int n){
//   int i,factorial=1;
//   for(i=1; i<=n; ++i){
//     factorial *= i;              // factorial = factorial*i;
//   }
//   return factorial;
// }
//
// void permute(int **full_array,int current,int *a, int l, int r)
// {
//    int i;
//    if (l == r){
//      int j;
//      for ( j=0; j<3; j++ ){
//        full_array[current][j] = a[j];
//        printf("%d",full_array[current][j]);
//      }
//      printf("\n");
//    }
//    else
//    {
//        for (i = l; i <= r; i++)
//        {
//           swap((a+l), (a+i));
//           int c=current+1;
//           permute(full_array,c,a, l+1, r);
//           swap((a+l), (a+i)); //backtrack
//        }
//    }
// }
//
// /* Driver program to test above functions */
// int main()
// {
//     int array[3] = {0,1,2},i,j;
//     int n = 3,size=factorial(n);
//     int **full_array=malloc(size*(sizeof(int *)));
//     for ( i=0; i<size; i++ ){
//       full_array[i] = malloc(n*sizeof(int));
//     }
//     permute(full_array,0,array, 0, n-1);
//     for ( i=0; i<size; i++ ){
//       for ( j=0; j<n; j++){
//         printf("%d",full_array[i][j]);
//       }
//       printf("\n");
//     }
//     return 0;
// }

void permute(list *my_list,int length,int *a, int l, int r)
{
   int i;
   if (l == r){
     list *ins=malloc(sizeof(list));
     int j,*arr=malloc(length*sizeof(int));
     for ( j=0; j<3; j++ ){
       arr[j] = a[j];
     }
     ins->perm = arr;
     ins->next = NULL;
     push_list_perm(my_list,ins);
   }
   else
   {
       for (i = l; i <= r; i++)
       {
          swap((a+l), (a+i));
          permute(my_list,length,a, l+1, r);
          swap((a+l), (a+i)); //backtrack
       }
   }
}

/* Driver program to test above functions */
int main()
{
    int array[3] = {0,1,2},i;
    int n = 3;
    list *tmp;
    list *head=malloc(sizeof(list));
    head->perm = array;
    head->next = NULL;
    permute(head,n,array,0, n-1);
    tmp=pop_list_perm(head);
    while ( tmp!=NULL ){
      for ( i=0; i<n; i++ ){
        printf("%d",tmp->perm[i]);
      }
      printf("\n");
      // free(tmp->perm);
      // free(tmp);
      tmp=pop_list_perm(head);
    }
    return 0;
}
