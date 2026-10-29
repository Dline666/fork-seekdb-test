#include "observer/seekdb_cuvs_startup_omp.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif
extern char **environ;
extern "C" int omp_get_max_threads(void);
extern "C" int omp_get_num_threads(void);
static int team()
{
  int observed=0;
#pragma omp parallel
  {
#pragma omp single
    observed=omp_get_num_threads();
  }
  return observed;
}
static std::string executable()
{
#ifdef __APPLE__
  uint32_t bytes=0;_NSGetExecutablePath(nullptr,&bytes);std::vector<char> path(bytes);
  assert(_NSGetExecutablePath(path.data(),&bytes)==0);return path.data();
#else
  return "/proc/self/exe";
#endif
}
int main(int argc,char **argv)
{
  assert(argc==3 && strcmp(argv[1],"argument with spaces")==0 && strcmp(argv[2],"literal-$HOME;quotes-'\"")==0);
  const char *marker=getenv(SEEKDB_CUVS_OMP_MARKER);
  int own=team(),other=0;std::thread thread([&]{other=team();});thread.join();
  printf("%s pid=%ld max=%d current_team=%d new_stdthread_team=%d\n",marker?"after_exec":"before_exec",(long)getpid(),omp_get_max_threads(),own,other);fflush(stdout);
  if(marker) {
    assert(strcmp(marker,"1")==0 && own==1 && other==1);
    assert(strtol(getenv("CUVS_TEST_PARENT_PID"),nullptr,10)==getpid());
    assert(strcmp(getenv("CUVS_TEST_PRESERVED"),"spaces $ and ; literal")==0);
    assert(strcmp(getenv("OMP_WAIT_POLICY"),"PASSIVE")==0);
    SeekdbCuvsStartupCpus cpus={128,128,1};
    assert(seekdb_cuvs_startup_attempt(argc,argv,environ,cpus,"/proc/self/exe",malloc,free,execve)==0);
    puts("PASS actual self-exec: same PID, exact argv/env, new std::thread default, marker prevents second exec");return 0;
  }
  assert(getenv("OMP_NUM_THREADS")==nullptr && getenv("OMP_WAIT_POLICY")==nullptr);
  std::vector<char *> inherited;
  for(char **item=environ;*item;++item) inherited.push_back(*item);
  std::string pid="CUVS_TEST_PARENT_PID="+std::to_string(getpid());
  char preserved[]="CUVS_TEST_PRESERVED=spaces $ and ; literal";
  inherited.push_back(pid.data());inherited.push_back(preserved);inherited.push_back(nullptr);
  SeekdbCuvsStartupCpus cpus={128,128,1}; // Synthetic quota exercises policy on any host.
  int old_max=omp_get_max_threads();
  assert(seekdb_cuvs_startup_attempt(argc,argv,inherited.data(),cpus,"/does-not-exist-cuvs-probe",malloc,free,execve)==-1);
  assert(getenv("OMP_NUM_THREADS")==nullptr && getenv(SEEKDB_CUVS_OMP_MARKER)==nullptr && omp_get_max_threads()==old_max);
  puts("PASS actual failed exec: no original environment/ICV mutation");fflush(stdout);
  (void)seekdb_cuvs_startup_attempt(argc,argv,inherited.data(),cpus,executable().c_str(),malloc,free,execve);
  perror("actual self-exec failed");return 1;
}
