#pragma once

#include "platform.h"

#define EXPERIMENTAL_MODE_TICTOC_DISK       0
#define EXPERIMENTAL_MODE_TICTOC_MEMORY 1
#define EXPERIMENTAL_MODE_TICTOC_COUNTER    0
#define EXPERIMENTAL_MODE_TICTOC_SKETCH     0
#define EXPERIMENTAL_MODE_STO_DISK          0
#define EXPERIMENTAL_MODE_STO_MEMORY        0
#define EXPERIMENTAL_MODE_STO_COUNTER       0
#define EXPERIMENTAL_MODE_STO_SKETCH        0
#define EXPERIMENTAL_MODE_2PL_NO_WAIT       0
#define EXPERIMENTAL_MODE_2PL_WAIT_DIE      0
#define EXPERIMENTAL_MODE_2PL_WOUND_WAIT    0
#define EXPERIMENTAL_MODE_SILO_MEMORY       0
#define EXPERIMENTAL_MODE_BYPASS_SPLINTERDB 0

#if EXPERIMENTAL_MODE_TICTOC_DISK || EXPERIMENTAL_MODE_STO_DISK
typedef uint32 txn_timestamp;
#else
typedef uint128 txn_timestamp;
#endif

static inline void
check_experimental_mode_is_valid()
{
   platform_assert(
      EXPERIMENTAL_MODE_TICTOC_DISK + EXPERIMENTAL_MODE_TICTOC_MEMORY
         + EXPERIMENTAL_MODE_TICTOC_COUNTER + EXPERIMENTAL_MODE_TICTOC_SKETCH
         + EXPERIMENTAL_MODE_STO_DISK + EXPERIMENTAL_MODE_STO_MEMORY
         + EXPERIMENTAL_MODE_STO_COUNTER + EXPERIMENTAL_MODE_STO_SKETCH
         + EXPERIMENTAL_MODE_2PL_NO_WAIT + EXPERIMENTAL_MODE_2PL_WAIT_DIE
         + EXPERIMENTAL_MODE_2PL_WOUND_WAIT + EXPERIMENTAL_MODE_SILO_MEMORY
      == 1);
}

static inline void
print_current_experimental_modes()
{
   // This function is used for only research experiments
   platform_set_log_streams(stdout, stderr);
   if (EXPERIMENTAL_MODE_TICTOC_DISK) {
      platform_default_log("EXPERIMENTAL_MODE_TICTOC_DISK\n");
   }
   if (EXPERIMENTAL_MODE_TICTOC_MEMORY) {
      platform_default_log("EXPERIMENTAL_MODE_TICTOC_MEMORY\n");
   }
   if (EXPERIMENTAL_MODE_TICTOC_COUNTER) {
      platform_default_log("EXPERIMENTAL_MODE_TICTOC_COUNTER\n");
   }
   if (EXPERIMENTAL_MODE_TICTOC_SKETCH) {
      platform_default_log("EXPERIMENTAL_MODE_TICTOC_SKETCH\n");
   }
   if (EXPERIMENTAL_MODE_STO_DISK) {
      platform_default_log("EXPERIMENTAL_MODE_STO_DISK\n");
   }
   if (EXPERIMENTAL_MODE_STO_MEMORY) {
      platform_default_log("EXPERIMENTAL_MODE_STO_MEMORY\n");
   }
   if (EXPERIMENTAL_MODE_STO_COUNTER) {
      platform_default_log("EXPERIMENTAL_MODE_STO_COUNTER\n");
   }
   if (EXPERIMENTAL_MODE_STO_SKETCH) {
      platform_default_log("EXPERIMENTAL_MODE_STO_SKETCH\n");
   }
   if (EXPERIMENTAL_MODE_2PL_NO_WAIT) {
      platform_default_log("EXPERIMENTAL_MODE_2PL_NO_WAIT\n");
   }
   if (EXPERIMENTAL_MODE_2PL_WAIT_DIE) {
      platform_default_log("EXPERIMENTAL_MODE_2PL_WAIT_DIE\n");
   }
   if (EXPERIMENTAL_MODE_2PL_WOUND_WAIT) {
      platform_default_log("EXPERIMENTAL_MODE_2PL_WOUND_WAIT\n");
   }
   if (EXPERIMENTAL_MODE_SILO_MEMORY) {
      platform_default_log("EXPERIMENTAL_MODE_SILO_MEMORY\n");
   }
}
