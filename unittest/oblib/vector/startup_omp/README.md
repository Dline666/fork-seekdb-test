# Linux GPU startup OpenMP experiment

This experiment is isolated on branch `competition/amd-cagra-startup-omp`, based
on the formally passing `e55976cd78dd45b44856682614912d96d611558f`. It does not
contain the later graph-only snapshot or DDL batching changes. Its production
header is `src/observer/seekdb_cuvs_startup_omp.h`; every test includes that same
header directly. There is no duplicate implementation under the test directory.

The small hook at the start of `src/observer/main.cpp::main` is compiled only for
Linux with `OB_BUILD_CUVS`. It runs before seekdb changes signal masks, switches
stacks, parses arguments, changes working directory, opens PID/log files or starts
server threads. Shared-library constructors have already run, so merely setting
environment variables in main would not reset their OpenMP defaults. The helper
instead execs the same executable once with a separately constructed environment.

It preserves the original argv, all original environment string entries and
the PID. It adds `OMP_NUM_THREADS=<effective CPU limit>`,
`OMP_WAIT_POLICY=PASSIVE`, and `SEEKDB_CUVS_OMP_REEXEC=1`. It does not modify the
current environ, affinity, cgroup, TLS or any security setting. Failed exec,
allocation failure, missing resource information or invalid input leave the
original startup running. A marker of any value prevents another exec.

## Conservative policy

Restart is skipped if any explicit setting below exists, even with an empty
value:

- `OMP_NUM_THREADS`, `OMP_WAIT_POLICY`, their `_ALL` variants.
- `OMP_PROC_BIND`, `OMP_PROC_BIND_ALL`, `OMP_PLACES`, `GOMP_CPU_AFFINITY`,
  `KMP_AFFINITY`, `KMP_HW_SUBSET`, `KMP_PLACE_THREADS`,
  `KMP_HIDDEN_HELPER_AFFINITY`.
- `SEEKDB_CUVS_OMP_REEXEC`.

Explicit binding settings are respected because an OpenMP constructor may
already have narrowed the initial thread to a single place. That narrow mask
must not be mistaken for the original process allocation and propagated as a
new automatic policy.

Exact help/version argv tokens (`--help`, `-h`, `--version`, `-V`) skip the
optimization without invoking getopt or mutating arguments. Matching an option
value also conservatively skips. CPU-only and non-Linux builds do nothing.

`effective_limit` is the minimum positive online CPU count, initial thread's
sched affinity count and visible cgroup-v2 `/sys/fs/cgroup/cpu.max` quota rounded
up. A restart is considered only when that limit is at least 25% below online
CPUs. Missing/unlimited/malformed cgroup quota does not become a one-CPU limit.
Only the visible cgroup root is supported; hidden ancestors, arbitrary mounts
and cgroup v1 are not traversed.

## Run tests

Linux with GCC/libgomp, Python3 and util-linux `taskset`:

```sh
bash unittest/oblib/vector/startup_omp/run_host_tests.sh
```

macOS with an existing libomp:

```sh
OPENMP_LIBRARY=/absolute/path/to/libomp.dylib \
  bash unittest/oblib/vector/startup_omp/run_host_tests.sh
```

No installation is performed. Compilers can be supplied via `CC`/`CXX`. The
runner builds tests in a fresh private temporary directory and removes only
that directory when finished. macOS adjusts only the temporary test binary's
load path to the provided libomp, leaving installed libraries unchanged.

`test_startup_policy.c` runs as C11 and C++17 with ASan/UBSan and warning errors.
It checks quota parsing (including invalid/overflow/unlimited/fractional values),
affinity/quota policy, exact argv/environment pointer preservation, explicit
settings/binding/marker/help/version skip, allocation overflow and OOM,
failed-exec cleanup and no environment mutation.

`test_startup_self_exec.cpp` runs a genuine OpenMP parallel region and a new
std::thread before and after calling the production policy helper with synthetic
restricted CPU metadata. It checks same PID, literal arguments containing
spaces/quotes/dollar/semicolon, an unrelated env value, failed-exec fallback,
new thread initial ICVs and marker prevention. Synthetic metadata is restricted
to this test call, not the production startup policy.

Linux additionally compiles `test_linux_wrapper.cpp` in GPU-enabled, forced
exec-failure and CPU-only variants, then runs `run_linux_wrapper_tests.py`.
The driver uses `taskset` to restrict only each owned child to one already
permitted CPU. It never changes the parent affinity or environment. The child
calls the actual `seekdb_cuvs_maybe_restart_for_cpu_quota` wrapper, including real
Linux CPU detection and `/proc/self/exe`. Fourteen cases cover normal restricted
startup, long/short help/version, explicit thread count/wait policy, passive and
active binding, places, GNU affinity, externally supplied marker, injected exec
failure, and CPU-only compilation. JSON evidence must report the original Popen
PID, and the normal case must report one thread in both main and a new std::thread.
Only the failure binary replaces execve with a test function returning EACCES;
the production header itself remains unchanged.

## Validation status and limits

The integrated production-header C11/C++17 ASan/UBSan policy tests and the real
macOS/libomp self-exec test passed locally. Example: PID76982 stayed unchanged,
main/new std::thread teams went from8 to1 after exec, and failed exec preserved
original ICVs. The new actual Linux wrapper cases are prepared but **have not
run in this macOS workspace**; Linux CI/GPU execution is still required.

This changes process-wide OpenMP defaults, so CPU VSAG thread scheduling may
change even though its algorithm and SQL path are unchanged. Profiler variables,
LD_PRELOAD and other environment values are preserved, but profiler constructors
and close-on-exec control FDs can still behave differently across exec. Same PID
is helpful but does not prove profiler compatibility.

Run actual Linux seekdb startup/help/version/explicit policy scenarios, CPU VSAG
and GPU CAGRA SQL correctness, snapshot persistence, Recall, resource usage and
external profiler tests before any formal competition submission. No GPU speedup
or score is claimed from these CPU-only mechanism tests.
