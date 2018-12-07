#include "Sql_queries.h"


int findNextPredicate(predicate* rel_predicate,int size,list *head) {

  int i;
  int best_pos = 0; // position of best choice so far

  for(i=1;i<size;i++) {

    if(empty_list(head)) { //first time, the list with rows is empty
      if(rel_predicate[i].metric > rel_predicate[best_pos].metric) {
        best_pos = i;
      }
    }
    else { // not the first time
      if(rel_predicate[i].metric == -1) {
        continue;
      }
      if(rel_predicate[best_pos].metric == -1) {
          best_pos = i;
          continue;
      }
      int rows = 0;
      if(rel_predicate[i].flag == 0 && rel_predicate[best_pos].flag == 0) {
        int best_rows = search_list(head,rel_predicate[best_pos].left.row) + search_list(head,rel_predicate[best_pos].right.row);
        int rows = search_list(head,rel_predicate[i].left.row) + search_list(head,rel_predicate[i].right.row);
        if((rows > best_rows) || (best_rows == rows && rel_predicate[i].metric > rel_predicate[best_pos].metric)) {
          best_pos = i;
        }
      }
      else if(rel_predicate[i].flag == 1 || rel_predicate[i].flag == 2) {
        //rows = search_list(rel_predicate.left.row);
        if(rel_predicate[best_pos].flag == 0 || (rel_predicate[best_pos].flag != 0 && rel_predicate[i].metric > rel_predicate[best_pos].metric)) {
          best_pos = i;
        }

      }
      else {
        printf("Something wrong\n");
      }

    }



  }

  return best_pos;
}



void push_list(list **head, uint64_t val) {
    if(*head==NULL){
        *head = malloc(sizeof(list));
        (*head)->val = val;
        (*head)->next = NULL;
        return;
    }
    list *new_node;
    new_node = malloc(sizeof(list));
    new_node->val = val;
    new_node->next = (*head);
    *head = new_node;
}

void push_list2(list **head,uint64_t val,int flag){
    if(*head==NULL){
        *head = malloc(sizeof(list));
        (*head)->val = val;
        (*head)->flag = flag;       // -1 gia allgh grammhs
        (*head)->next = NULL;
        return;
    }
    list *new_node,*temp=*head;
    new_node = malloc(sizeof(list));
    new_node->val = val;
    new_node->flag = flag;
    new_node->next = NULL;
    while(temp->next!=NULL){
        temp=temp->next;
    }
    temp->next=new_node;
}

int search_list(list *head,int x){

    while(head!=NULL){
        if(head->val==x) {
          return 1;
        }
        head=head->next;
    }
    return 0;
}

int empty_list(list *head){
  if(head==NULL) {
    return 1;
  }
  return 0;
}

void freeList(list* head) {
   list *tmp;
   while (head != NULL) {
       tmp = head;
       head = head->next;
       free(tmp);
    }

}

void print_list(list *add){
  list *tmp=add;
  while ( tmp!=NULL ){
    if ( tmp->val==0 ){
      printf("NULL ");
    }
    else{
        if(tmp->flag==-1){
            printf("\n");
        }
        else{
            printf("%lu ",tmp->val);
        }
    }
    tmp = tmp->next;
  }
}
