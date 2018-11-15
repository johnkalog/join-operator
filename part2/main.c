//#define __STDC_FORMAT_MACROS
#include "Full_Relation.h"

int main(int argc,char *argv[]) {

    if(argc!=2){
        printf("Need 1 argument only\n");
        return -1;
    }
    unsigned int num_lines;
    full_relation *relations_array = NULL;
    //tuple **tuple_arrays;


    relations_array=full_relation_creation(argv[1],&num_lines);

    print_relation(relations_array[0]);
    free_structs(relations_array,num_lines);
    return 0;
}
