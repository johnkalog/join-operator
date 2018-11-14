#include "stdio.h"
//#include <stdint.h>
//#define __STDC_FORMAT_MACROS
//#include <inttypes.h>
#include <string.h>


unsigned int line_count(FILE *);

int main(int argc,char *argv[]) {
    FILE *fp;
    fp = fopen(argv[1],"r");

    //fseek(fp, 0L, SEEK_END);
    //long int num_lines = ftell(fp);
    unsigned int num_lines = line_count(fp);
    printf("num lines of filess %u\n",num_lines );

    if(fp == NULL) {
      return -1;
    }

    char * line = NULL;
    size_t len = 0;
    ssize_t read;
    while((read = getline(&line, &len, fp)) != -1) {
        printf("Retrieved line of length %zu :\n", read);
        printf("%s", line);
    }


    //uint64_t num64;
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
