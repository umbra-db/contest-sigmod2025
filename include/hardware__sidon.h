// Hardware information for Intel Xeon E7-4880 v2 node sidon.

// Architecture from `uname -srm`.
#define SPC__X86_64

// CPU from `/proc/cpuinfo`.
#define SPC__CPU_NAME "Intel(R) Xeon(R) CPU E7-4880 v2 @ 2.50GHz"

// The servers might have multiple CPUs. We limit all benchmarks to a single node using numactl. The listed CPU numbers
// below are for a single CPU. The listed NUMA numbers are just meant to give you a rough idea of the system.
#define SPC__CORE_COUNT 15
#define SPC__THREAD_COUNT 30
#define SPC__NUMA_NODE_COUNT 4
#define SPC__NUMA_NODES_ACTIVE_IN_BENCHMARK 1

// Main memory per NUMA node (MB).
#define SPC__NUMA_NODE_DRAM_MB 515809

// Obtained from `lsb_release -a`.
#define SPC__OS "Ubuntu 22.04.4 LTS"

// Obtained from: `uname -srm`.
#define SPC__KERNEL "Linux 5.15.0-116-generic x86_64"

// Intel: possible options are AVX, AVX2, and AVX512. No Intel CPU older than Intel Xeon E7-4880 v2 will be used.
#define SPC__SUPPORTS_AVX

// Cache information from `getconf -a | grep CACHE`.
#define SPC__LEVEL1_ICACHE_SIZE                 32768
#define SPC__LEVEL1_ICACHE_ASSOC
#define SPC__LEVEL1_ICACHE_LINESIZE             64
#define SPC__LEVEL1_DCACHE_SIZE                 32768
#define SPC__LEVEL1_DCACHE_ASSOC                8
#define SPC__LEVEL1_DCACHE_LINESIZE             64
#define SPC__LEVEL2_CACHE_SIZE                  262144
#define SPC__LEVEL2_CACHE_ASSOC                 8
#define SPC__LEVEL2_CACHE_LINESIZE              64
#define SPC__LEVEL3_CACHE_SIZE                  39321600
#define SPC__LEVEL3_CACHE_ASSOC                 20
#define SPC__LEVEL3_CACHE_LINESIZE              64
#define SPC__LEVEL4_CACHE_SIZE                  0
#define SPC__LEVEL4_CACHE_ASSOC
#define SPC__LEVEL4_CACHE_LINESIZE
