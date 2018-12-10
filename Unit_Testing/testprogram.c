#include "./../part2/Sql_queries.h"
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>

int init_suite(void) { return 0; }
int clean_suite(void) { return 0; }


int sum(int x, int y)
{
  int z;
  z=x+y;
  return z;
}

int diff(int a, int b)
{
  int  c;
  c=a-b;
  return c;
}

void test_sum(void)//our test function 1
{
  CU_ASSERT(5==sum(3,2));
  CU_ASSERT(9==sum(3,6));
  CU_ASSERT(3==sum(-3,6));
  CU_ASSERT(6==sum(0,6));
}

int check1D(int *array1,int *array2,int n) {

  int i;
  for(i=0;i<n;i++) {
    if(array1[i]!=array2[i]){
      return -1;
    }
  }
  return 0;
}

void test_arrays(void)//our test function 2
{
  int A1[5] = {1,2,3,4,5};
  int A2[5] = {1,2,3,4,5};
  int K1[3] = {0,1,2};
  int K2[3] = {5,4,3};
  int K3[3] = {0,0,0};
  int K4[3] = {5,5,5};
  int K5[3] = {2,2,2};
  int K6[3] = {3,3,3};
  int K7[4] = {2,1,2,1};
  int K8[4] = {3,4,3,4};


  int **A = malloc(2*sizeof(int*));
  A[0] = NULL;
  A[1] = NULL;
  result *Result=result_init();
  insert(Result,0,5);
  insert(Result,1,4);
  insert(Result,2,3);

  result2keys(Result,A,0,0,2);
  CU_ASSERT(0==check1D(A1,A2,5));
  CU_ASSERT(0==check1D(K1,A[0],3));

  result2keys(Result,A,1,1,2);
  CU_ASSERT(0==check1D(K2,A[1],3));

  free(Result);

  Result=result_init();
  insert(Result,1,3);
  insert(Result,1,3);
  insert(Result,1,3);
  result2keys(Result,A,0,0,2);
  CU_ASSERT(0==check1D(K3,A[0],3));
  CU_ASSERT(0==check1D(K4,A[1],3));


  A[0][0] = 0;A[0][1] = 1;A[0][2] = 2;
  A[1][0] = 5;A[1][1] = 4;A[1][2] = 3;
  result2keys(Result,A,0,1,2);
  CU_ASSERT(0==check1D(K5,A[0],3));
  CU_ASSERT(0==check1D(K6,A[1],3));
  free(Result);


  A[0][0] = 0;A[0][1] = 1;A[0][2] = 2;
  A[1][0] = 5;A[1][1] = 4;A[1][2] = 3;

  Result=result_init();
  insert(Result,3,3);
  insert(Result,2,3);
  insert(Result,3,3);
  insert(Result,2,3);
  result2keys(Result,A,1,0,2);

  CU_ASSERT(0==check1D(K7,A[0],3));
  CU_ASSERT(0==check1D(K8,A[1],3));

}

void test_diff(void)//our test function 2
{
  CU_ASSERT(1==diff(3,2));
  CU_ASSERT(-3==diff(3,6));
  CU_ASSERT(-9==diff(-3,6));
  CU_ASSERT(-6==diff(0,6));
}
int main (void)// Main function
{

  CU_pSuite pSuite1,pSuite2 = NULL;

  // Initialize CUnit test registry
  if (CUE_SUCCESS != CU_initialize_registry())
    return CU_get_error();

  // Add suite1 to registry
  pSuite1 = CU_add_suite("Basic_Test_Suite1", init_suite, clean_suite);
  if (NULL == pSuite1) {
    CU_cleanup_registry();
    return CU_get_error();
  }

  // add test1 to suite1
  if ((NULL == CU_add_test(pSuite1, "\n\n……… Testing Sum function……..\n\n", test_sum)))
  {
    CU_cleanup_registry();
    return CU_get_error();
  }

  if ((NULL == CU_add_test(pSuite1, "\n\n……… Testing diff function……..\n\n", test_diff)))
  {
    CU_cleanup_registry();
    return CU_get_error();
  }

  //add test 2 to suite2
  pSuite2 = CU_add_suite("Arrays", init_suite, clean_suite);
  if (NULL == pSuite2) {
    CU_cleanup_registry();
    return CU_get_error();
  }
  if ((NULL == CU_add_test(pSuite2, "\n\n……… Testing test_arrays function……..\n\n", test_arrays)))
  {
    CU_cleanup_registry();
    return CU_get_error();
  }

  CU_basic_run_tests();// OUTPUT to the screen

  CU_cleanup_registry();//Cleaning the Registry
  return CU_get_error();

}