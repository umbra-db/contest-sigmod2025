// Hardware information for Ampere Altra Max node ca09.

// Architecture from `uname -srm`.
#define SPC__AARCH64

// CPU from `/proc/cpuinfo`.
#define SPC__CPU_NAME ""

// The servers might have multiple CPUs. We limit all benchmarks to a single node using numactl. The listed CPU numbers
// below are for a single CPU. The listed NUMA numbers are just meant to give you a rough idea of the system.
#define SPC__CORE_COUNT 128
#define SPC__THREAD_COUNT 128
#define SPC__NUMA_NODE_COUNT 1
#define SPC__NUMA_NODES_ACTIVE_IN_BENCHMARK 1

// Main memory per NUMA node (MB).
#define SPC__NUMA_NODE_DRAM_MB 515809

// Obtained from `lsb_release -a`.
#define SPC__OS "Ubuntu 24.04.1 LTS"

// Obtained from: `uname -srm`.
#define SPC__KERNEL "Linux 6.8.0-50-generic aarch64"

// ARM: possible options are SVE, SVE2, and NEON. No ARM CPU older than Ampere Altra Max will be used.
#define SPC__SUPPORTS_NEON

// Cache information from `getconf -a | grep CACHE`.
// As Ubuntu did not list all numbers, we also took cache sizes from `cat /sys/devices/system/cpu/cpu0/cache/index*/size`
#define SPC__LEVEL1_ICACHE_SIZE                 65536
#define SPC__LEVEL1_ICACHE_ASSOC
#define SPC__LEVEL1_ICACHE_LINESIZE             64
#define SPC__LEVEL1_DCACHE_SIZE                 65536
#define SPC__LEVEL1_DCACHE_ASSOC
#define SPC__LEVEL1_DCACHE_LINESIZE             64
#define SPC__LEVEL2_CACHE_SIZE                  1048576
#define SPC__LEVEL2_CACHE_ASSOC
#define SPC__LEVEL2_CACHE_LINESIZE
#define SPC__LEVEL3_CACHE_SIZE
#define SPC__LEVEL3_CACHE_ASSOC
#define SPC__LEVEL3_CACHE_LINESIZE
#define SPC__LEVEL4_CACHE_SIZE 
#define SPC__LEVEL4_CACHE_ASSOC
#define SPC__LEVEL4_CACHE_LINESIZE
