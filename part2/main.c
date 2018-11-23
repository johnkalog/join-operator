//#define __STDC_FORMAT_MACROS
#include "Full_Relation.h"

int main(int argc,char *argv[]) {

    if(argc!=6){
        printf("Need 6 arguments\n");
        exit(1);
    }
    unsigned int num_lines;
    full_relation *relations_array = NULL;
    //tuple **tuple_arrays;

    relations_array=full_relation_creation(argv[1],&num_lines);

    //print_relation(relations_array[0]);

    char str[50] = "0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1";
    char *tok1 = strtok(str,"|");
    char *tok2 = strtok(NULL,"|");
    char *tok3 = strtok(NULL,"|");



    char *cp_tok1 = strdup(tok1);
    full_relation **rel_pointers=NULL;
    int rel_num=0;
    char *rel_id=strtok(cp_tok1," ");
    while ( rel_id!=NULL ) {
      rel_num ++;
      rel_id = strtok(NULL," ");
    }
    free(cp_tok1);
    rel_pointers = malloc(rel_num*sizeof(full_relation *));
    rel_id=strtok(tok1," ");
    int i;
    for ( i=0; i<rel_num; i++ ) {
    rel_pointers[i] = &relations_array[atoi(rel_id)];
    rel_id = strtok(NULL," ");

    }
    for ( i=0; i<rel_num; i++ ) {
      printf("fefwef %ld\n",rel_pointers[i]->my_metadata.num_columns);

    }
    printf("%d\n",rel_num);
    //printf("%s\n",tok1 );
    printf("%s\n",tok2 );
    printf("%s\n",tok3 );
    int relAindex=atoi(argv[2]),relBindex=atoi(argv[3]),relAcol=atoi(argv[4]),relBcol=atoi(argv[5]);
    // 0 for > 1 for < 2 for =

    result *Result=RHJcaller(relations_array,relAindex,relBindex,relAcol,relBcol);
    free_structs(relations_array,num_lines);

    printf("Press Enter\n");
    getchar();
    sleep(1);
    printf("Done\n");
    result_print(Result);

    result_free(Result);
    return 0;
}
