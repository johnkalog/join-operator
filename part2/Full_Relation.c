#include "Full_Relation.h"

unsigned int line_count(FILE *n) {
  char c;
  unsigned int lines = 0;

  while ((c = fgetc(n)) != EOF) {
    if (c == '\n') ++lines;
  }
  fseek(n,0,SEEK_SET);
  return lines;
}


full_relation *full_relation_creation(char *path_file,unsigned int *num_lines){

  full_relation *relations_array;


  FILE *fp=NULL,*fp_binary=NULL;
  fp = fopen(path_file,"r");
  if ( fp==NULL) {
    printf("Error in fopen\n");
    exit(1);
  }

  //fseek(fp, 0L, SEEK_END);
  //long int num_lines = ftell(fp);
  *num_lines = line_count(fp);
  //printf("num lines of filess %u\n",*num_lines );

  relations_array=malloc((*num_lines)*sizeof(full_relation));
  //tuple_arrays=malloc((*num_lines)*sizeof(tuple *));

  int i,j,k,l;
  char* line = NULL,*pre_path=NULL,*path=NULL;
  size_t len = 0;
  ssize_t read;
  uint64_t num64,num_of_tuples,num_of_cols;

  int input_filename_length=0; //../small/small.init krataei to small_init length
  for ( i=strlen(path_file); path_file[i-1]!='/'; i--){
    input_filename_length ++;
  }
  pre_path = malloc((strlen(path_file)-input_filename_length+1)*sizeof(char));
  bzero(pre_path,strlen(path_file)-input_filename_length+1);
  strncpy(pre_path,path_file,strlen(path_file)-input_filename_length);
  //printf("ferfwef %d %d %s\n",input_filename_length,(int)strlen(path_file),pre_path);

  k = 0;
  while((read = getline(&line, &len, fp)) != -1) {
      i=0;
      while(line[i]!='\n')  i++;
      line[i]='\0';

      path=malloc((strlen(line)+strlen(pre_path)+1)*sizeof(char));
      strcpy(path,pre_path);
      strcat(path,line);
      //printf("path: %s\n",path);
      fp_binary = fopen(path,"rb");
      fread(&num_of_tuples,sizeof(uint64_t),1,fp_binary);
      fread(&num_of_cols,sizeof(uint64_t),1,fp_binary);
      relations_array[k].my_metadata.num_tuples = num_of_tuples;
      relations_array[k].my_metadata.num_columns = num_of_cols;
      //printf("%ld %ld\n",relations_array[k].my_metadata.num_tuples,relations_array[k].my_metadata.num_columns);
      relations_array[k].my_relations = malloc(relations_array[k].my_metadata.num_columns*sizeof(relation));
      tuple *tuple_array = malloc(num_of_tuples*num_of_cols*sizeof(tuple));

      //printf("%"PRIu64" - %"PRIu64"\n",num_of_tuples,num_of_cols );

      for(i=0;i<num_of_cols;i++){
          for(j=0;j<num_of_tuples;j++){
              fread(&num64,sizeof(uint64_t),1,fp_binary);
              tuple_array[i*num_of_tuples+j].key = j+1;
              tuple_array[i*num_of_tuples+j].payload = num64;
              // if ( k==0 ){
              //   printf("dwqdqw %ld\n",i*num_of_tuples+j);
              // }
              ///// FILL STRUCTS FILL STRUCTS FILL STRUCTS
          }
      }
      for ( l=0; l<relations_array[k].my_metadata.num_columns; l++ ){
        relations_array[k].my_relations[l].num_tuples = relations_array[k].my_metadata.num_tuples;
        relations_array[k].my_relations[l].tuples = &tuple_array[l*num_of_tuples];
      }

    /////////////
    free(path);
    //////////
    fclose(fp_binary);
    k ++;
  }


  //fread(&num64,sizeof(uint64_t),1,fp);
  //printf("%"PRIu64"\n",num64 );

  free(line);
  free(pre_path);
  fclose(fp);
  return relations_array;
}

void print_relation(full_relation tmp){

  int i,j;
  for ( i=0; i<tmp.my_metadata.num_tuples; i++ ){
    for ( j=0; j<tmp.my_metadata.num_columns; j++ ){
      printf("%ld|",tmp.my_relations[j].tuples[i].payload);
    }
    printf("\n");
  }
}

result* RHJcaller(full_relation *relations_array,int relAindex,int relBindex,int relAcol,int relBcol,int operator){
  return RadixHashJoin(&relations_array[relAindex].my_relations[relAcol],&relations_array[relBindex].my_relations[relBcol],operator);
}

void free_structs(full_relation *relations_array,unsigned int num_lines){

  //int i,j;
  int i;
 for ( i=0; i<num_lines; i++ ){

   free(relations_array[i].my_relations[0].tuples);
   free(relations_array[i].my_relations);
      //for ( j=0; j<relations_array[i].my_metadata.num_columns; j++ ){
  //   //   relations_array[i].my_relations[j].tuples = NULL;
  //   // }
  //   free(relations_array[i].my_relations);
  // }
  // for ( i=0; i<num_lines; i++ ){
  //   free(tuple_arrays[i]);
  // }
  //free(tuple_arrays);
  }
  free(relations_array);
}
