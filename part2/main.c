//#define __STDC_FORMAT_MACROS
#include "Sql_queries.h"

//0 ta indexes?

int main(int argc,char *argv[]) {

    if(argc!=3){
        printf("Need 2 arguments\n");
        exit(1);
    }
    unsigned int num_lines;
    full_relation *relations_array = NULL;
    //tuple **tuple_arrays;

    relations_array=full_relation_creation(argv[1],&num_lines);

    //print_relation(relations_array[0]);

    // printf("Press Enter\n");
    // getchar();
    // sleep(1);
    // printf("Done\n");

    sql_queries(argv[2],relations_array);

    list *head=malloc(sizeof(list));
    head=NULL;
    push_list(&head,2);
    push_list(&head,3);
    search_list(head,2);
    printf("xd %d\n",head->val );

    free_structs(relations_array,num_lines);

    return 0;
}
