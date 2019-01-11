#include "Sql_queries.h"

int main(int argc,char *argv[]) {

    if(argc!=3){
        printf("Need 2 arguments\n");
        exit(1);
    }

    unsigned int num_lines;

    full_relation *relations_array = NULL;

    relations_array=full_relation_creation(argv[1],&num_lines); //apothhkeush sxesewn sthn mnhnmh

    // printf("Done\n");

    sql_queries(argv[2],relations_array); //ektelei ola ta queries kai emfanizei ta apotelesmata
    // printf("fewwefew %d\n",check_if_in(&relations_array[0].my_relations[0],5745));

    free_structs(relations_array,num_lines);

    return 0;
}
