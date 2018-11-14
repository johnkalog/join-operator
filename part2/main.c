#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
//#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>


unsigned int line_count(FILE *);

int main(int argc,char *argv[]) {
    if(argc<3){
        printf("Need 2 arguments, '../small/small.init'   and   '../small/'\n");
        return -1;
    }

    FILE *fp=NULL,*fp_binary=NULL;
    fp = fopen(argv[1],"r");

    //fseek(fp, 0L, SEEK_END);
    //long int num_lines = ftell(fp);
    unsigned int num_lines = line_count(fp);
    printf("num lines of filess %u\n",num_lines );

    if(fp == NULL) {
      return -1;
    }

    int i,j;
    char * line = NULL,*path=NULL;
    size_t len = 0;
    ssize_t read;
    uint64_t num64,num_of_tuples,num_of_cols;
    while((read = getline(&line, &len, fp)) != -1) {
        i=0;
        while(line[i]!='\n')  i++;
        line[i]='\0';

        path=malloc((strlen(line)+strlen(argv[2])+1)*sizeof(char));
        strcpy(path,argv[2]);
        strcat(path,line);
        fp_binary = fopen(path,"rb");
        fread(&num_of_tuples,sizeof(uint64_t),1,fp_binary);
        fread(&num_of_cols,sizeof(uint64_t),1,fp_binary);
        //  printf("%"PRIu64" - %"PRIu64"\n",num_of_tuples,num_of_cols );

        for(j=0;j<num_of_cols;j++){
            for(i=0;i<num_of_tuples;i++){
                fread(&num64,sizeof(uint64_t),1,fp_binary);
                ///// FILL STRUCTS FILL STRUCTS FILL STRUCTS
            }
        }

      /////////////
      free(path);
      //////////
      fclose(fp_binary);
    }


    //fread(&num64,sizeof(uint64_t),1,fp);
    //printf("%"PRIu64"\n",num64 );



    fclose(fp);
    return 0;
}

unsigned int line_count(FILE *n) {
  char c;
  unsigned int lines = 0;

  while ((c = fgetc(n)) != EOF) {
    if (c == '\n') ++lines;
  }
  fseek(n,0,SEEK_SET);
  return lines;
}
