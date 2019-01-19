#include "Sql_queries.h"

void push_list(list **head, uint64_t val) { //eisagwgh
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

void push_list2(list **head,uint64_t val,int flag){ //allazei kai to flag
    if(*head==NULL){
        *head = malloc(sizeof(list));
        (*head)->val = val;
        (*head)->flag = flag;
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

list *copy_list(list *start1) {
    if(start1==NULL) return NULL;
    list *temp=(list *) malloc(sizeof(list));
    temp->val=start1->val;
    temp->next=copy_list(start1->next);
    return temp;
}

void print_list(list *add){
  list *tmp=add;
  char space=' ';
  while ( tmp!=NULL ){
    char buffer[64]={0};
    if ( tmp->val==0 ){
      //printf("NULL");
      write(1,"NULL",4);
      if(tmp->next->flag!=-1){
        //printf(" ");
        write(1,&space,1);
      }
    }
    else{
        if(tmp->flag==-1){
            //printf("\n");
            write(1,"\n",1);
        }
        else{
            //printf("%lu",tmp->val);
            sprintf(buffer,"%lu",tmp->val);
            write(1,buffer,strlen(buffer));
            if(tmp->next->flag!=-1){
              //printf(" ");
              write(1,&space,1);
            }
        }
    }
    tmp = tmp->next;
  }
}
