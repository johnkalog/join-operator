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

  *num_lines = line_count(fp);

  relations_array=malloc((*num_lines)*sizeof(full_relation));

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

  k = 0;
  while((read = getline(&line, &len, fp)) != -1) {
      i=0;
      while(line[i]!='\n')  i++;
      line[i]='\0';

      path=malloc((strlen(line)+strlen(pre_path)+1)*sizeof(char));
      strcpy(path,pre_path);
      strcat(path,line);

      fp_binary = fopen(path,"rb");
      fread(&num_of_tuples,sizeof(uint64_t),1,fp_binary);
      fread(&num_of_cols,sizeof(uint64_t),1,fp_binary);
      relations_array[k].my_metadata.num_tuples = num_of_tuples;  //apothhkeush metadata
      relations_array[k].my_metadata.num_columns = num_of_cols;
      relations_array[k].my_relations = malloc(relations_array[k].my_metadata.num_columns*sizeof(relation));
      tuple *tuple_array = malloc(num_of_tuples*num_of_cols*sizeof(tuple)); //pinakas me ta ola ta stoixeia mias sxeshs apo kathe kolwna

      for(i=0;i<num_of_cols;i++){
          for(j=0;j<num_of_tuples;j++){
              fread(&num64,sizeof(uint64_t),1,fp_binary);
              tuple_array[i*num_of_tuples+j].key = j+1;
              tuple_array[i*num_of_tuples+j].payload = num64;
          }
      }
      for ( l=0; l<relations_array[k].my_metadata.num_columns; l++ ){
        relations_array[k].my_relations[l].num_tuples = relations_array[k].my_metadata.num_tuples;
        relations_array[k].my_relations[l].tuples = &tuple_array[l*num_of_tuples];  //o katallhlos deikths gia thn antistoixh column
      }

      relations_array[k].my_metadata.statistics_array = malloc(relations_array[k].my_metadata.num_columns*sizeof(statistics));
      int p;
      for ( p=0; p<relations_array[k].my_metadata.num_columns; p++ ){
        relations_array[k].my_metadata.statistics_array[p].min = calculate_min(relations_array[k].my_relations[p].tuples,relations_array[k].my_metadata.num_tuples);
        relations_array[k].my_metadata.statistics_array[p].max = calculate_max(relations_array[k].my_relations[p].tuples,relations_array[k].my_metadata.num_tuples);
      }
    /////////////
    free(path);
    //////////
    fclose(fp_binary);
    k ++;
  }

  free(line);
  free(pre_path);
  fclose(fp);
  return relations_array;
}

uint64_t calculate_min(tuple *my_small_array,int num_tuples){ //upologismos mikroterou stoixeiou
  uint64_t min=my_small_array[0].payload,tmp;
  int i;
  for ( i=1; i<num_tuples; i++ ){
    tmp = my_small_array[i].payload;
    if ( tmp<min ){
      min = tmp;
    }
  }

  return min;
}

uint64_t calculate_max(tuple *my_small_array,int num_tuples){ //upologismos megaluterou stoixeiou
  uint64_t max=my_small_array[0].payload,tmp;
  int i;
  for ( i=1; i<num_tuples; i++ ){
    tmp = my_small_array[i].payload;
    if ( tmp>max ){
      max = tmp;
    }
  }

  return max;
}

void print_relation(full_relation tmp){ //gia epalhtheush apotelesmatwn

  int i,j;
  for ( i=0; i<tmp.my_metadata.num_tuples; i++ ){
    for ( j=0; j<tmp.my_metadata.num_columns; j++ ){
      printf("%ld|",tmp.my_relations[j].tuples[i].payload);
    }
    printf("\n");
  }
}

void free_structs(full_relation *relations_array,unsigned int num_lines){

  int i,j;
 for ( i=0; i<num_lines; i++ ){
   if(relations_array[i].my_metadata.num_tuples > 0) {
     free(relations_array[i].my_metadata.statistics_array);
     free(relations_array[i].my_relations[0].tuples);
     free(relations_array[i].my_relations);
   }
  }
  free(relations_array);
}
