#include "../part1/hash.h"
#include "Full_Relation.h"

void sql_reader(char *filepath){
    char *line=NULL,*query=NULL;
    size_t len = 0;
    ssize_t nread;
    FILE *fp;
    fp=fopen(filepath,"r");
    if (fp == NULL) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    while((nread = getline(&line, &len, fp)) != -1){
        if(line[0]=='F'){
            printf("It's F, new batch!\n");
        }
        else{
            //printf("%s",line );
            char *tok1 = strtok(line,"|");
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
            free(rel_pointers);
            printf("%d\n",rel_num);
            //printf("%s\n",tok1 );
            printf("%s\n",tok2 );
            printf("%s\n",tok3 );


        }
    }

}
