#include "Sql_queries.h"

void sql_queries(char *filepath,full_relation *relations_array){
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
            printf("///////////////////////%s",line );
            char *tok1 = strtok(line,"|");
            char *tok2 = strtok(NULL,"|");
            char *tok3 = strtok(NULL,"|");

            //----------------------------from--------------------------
            int i;
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
            for ( i=0; i<rel_num; i++ ) {
              rel_pointers[i] = &relations_array[atoi(rel_id)];
              rel_id = strtok(NULL," ");
            }
            for ( i=0; i<rel_num; i++ ) {
              printf("fefwef %ld\n",rel_pointers[i]->my_metadata.num_columns);

            }
            free(rel_pointers);
            //--------------------------where--------------------------------
            char *cp_tok2 = strdup(tok2);
            char **rel_condition=NULL;
            int condition_num=0;
            char *condition=strtok(cp_tok2,"&");
            while ( condition!=NULL ) {
              condition_num ++;
              condition = strtok(NULL,"&");
            }
            free(cp_tok2);
            rel_condition = malloc(condition_num*sizeof(char *));
            condition = strtok(tok2,"&");
            for ( i=0; i<condition_num; i++ ) {
              if ( condition==NULL ){
                printf("ssssssssssssssssfwefrtyu\n");
              }
              rel_condition[i] = malloc((strlen(condition)+1)*sizeof(char));
              strcpy(rel_condition[i],condition);
              condition = strtok(NULL,"&");
            }

            for ( i=0; i<condition_num; i++ ) {
              printf("fefwef %s\n",rel_condition[i]);
            }
            for(i=0;i<condition_num;i++){
                free(rel_condition[i]);
            }
            free(rel_condition);
            //-----------------------------select------------------------------
            char *cp_tok3 = strdup(tok3);
            char **rel_selection=NULL;
            int selection_num=0;
            char *selection=strtok(cp_tok3," ");
            while ( selection!=NULL ) {
              selection_num ++;
              selection = strtok(NULL," ");
            }
            free(cp_tok3);
            printf("dewfewfw %d\n",selection_num);
            rel_selection = malloc(selection_num*sizeof(char *));
            selection = strtok(tok3," ");
            for ( i=0; i<selection_num; i++ ) {
              if ( selection==NULL ){
                printf("fewwefwefwwef\n");
              }
              rel_selection[i] = selection;
              selection = strtok(NULL," ");
            }

            for ( i=0; i<selection_num; i++ ) {
              printf("fefwef %s\n",rel_selection[i]);
            }
            free(rel_selection);
            //int relAindex=atoi(argv[3]),relBindex=atoi(argv[4]),relAcol=atoi(argv[5]),relBcol=atoi(argv[6]);
            //result *Result=RHJcaller(relations_array,relAindex,relBindex,relAcol,relBcol);
            // result_print(Result);
            //
            // result_free(Result);
        }
    }
    fclose(fp);
}
