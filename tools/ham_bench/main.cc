/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <cstdio>
#include <ctime>

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>

#include <ham/hamsterdb_int.h>

#include "../getopts.h"
#include "../common.h"
#include "configuration.h"
#include "datasource.h"
#include "datasource_numeric.h"
#include "datasource_binary.h"
#include "generator_runtime.h"
#include "generator_parser.h"
#include "hamsterdb.h"
#ifdef HAM_WITH_BERKELEYDB
#  include "berkeleydb.h"
#endif
#include "metrics.h"
#include "misc.h"

#include "1globals/globals.h"

#define ARG_HELP                                1
#define ARG_VERBOSE                             2
#define ARG_QUIET                               3
#define ARG_NO_PROGRESS                         4
#define ARG_REOPEN                              5
#define ARG_METRICS                             6
#define ARG_OPEN                                8
#define ARG_INMEMORY                            10
#define ARG_OVERWRITE                           11
#define ARG_DISABLE_MMAP                        12
#define ARG_PAGESIZE                            13
#define ARG_KEYSIZE                             14
#define ARG_KEYSIZE_FIXED                       15
#define ARG_RECSIZE                             16
#define ARG_RECSIZE_FIXED                       17
#define ARG_REC_INLINE                          18
#define ARG_CACHE                               19
#define ARG_USE_CURSORS                         23
#define ARG_KEY                                 24
#define ARG_DUPLICATE                           26
#define ARG_FULLCHECK                           27
#define ARG_FULLCHECK_FREQUENCY                 28
#define ARG_RECOVERY                            34
#define ARG_HINTING                             37
#define ARG_DIRECT_ACCESS                       39
#define ARG_USE_TRANSACTIONS                    41
#define ARG_USE_FSYNC                           42
#define ARG_USE_BERKELEYDB                      43
#define ARG_USE_HAMSTERDB                       47
#define ARG_NUM_THREADS                         44
#define ARG_ENABLE_ENCRYPTION                   45
#define ARG_USE_REMOTE                          46
#define ARG_ERASE_PCT                           48
#define ARG_FIND_PCT                            49
#define ARG_TABLE_SCAN_PCT                      50
#define ARG_STOP_TIME                           51
#define ARG_STOP_OPS                            52
#define ARG_STOP_BYTES                          53
#define ARG_TEE                                 54
#define ARG_SEED                                55
#define ARG_DISTRIBUTION                        56
#define ARG_EXTKEY_THRESHOLD                    57
#define ARG_DUPTABLE_THRESHOLD                  58
#define ARG_BULK_ERASE                          59
#define ARG_FLUSH_TXN_IMMEDIATELY               60
#define ARG_DISABLE_RECOVERY                    61
#define ARG_JOURNAL_COMPRESSION                 62
#define ARG_RECORD_COMPRESSION                  63
#define ARG_KEY_COMPRESSION                     64
#define ARG_PAX_LINEAR_THRESHOLD                65
#define ARG_PAX_DISABLE_SIMD                    66
#define ARG_READ_ONLY                           67
#define ARG_ENABLE_CRC32                        68
#define ARG_RECORD_NUMBER32                     69
#define ARG_RECORD_NUMBER64                     70
#define ARG_POSIX_FADVICE                       71

/*
 * command line parameters
 */
static option_t opts[] = {
  { 
    ARG_HELP,         // symbolic name of this option
    "h",          // short option 
    "help",         // long option 
    "Prints this help screen",   // help string
    0 },          // no flags
  {
    ARG_VERBOSE,
    "v",
    "verbose",
    "Prints verbose information",
    0 },
  {
    ARG_QUIET,
    "q",
    "quiet",
    "Does not print profiling metrics",
    0 },
  {
    ARG_NO_PROGRESS,
    0,
    "no-progress",
    "Disables the progress bar",
    0 },
  {
    ARG_REOPEN,
    "r",
    "reopen",
    "Calls OPEN/FULLCHECK/CLOSE after each close",
    0 },
  {
    ARG_OPEN,
    "o",
    "open",
    "Opens an existing Environment",
    0 },
  {
    ARG_METRICS,
    0,
    "metrics",
    "Prints metrics and statistics ('none', 'default', 'png', 'all')",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_TEE,
    0,
    "tee",
    "Copies the generated test data into the specified file",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_SEED,
    0,
    "seed",
    "Sets the seed for the random number generator",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_DISTRIBUTION,
    0,
    "distribution",
    "Sets the distribution of the key values ('random', 'ascending',\n"
            "\t'descending')",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_INMEMORY,
    0,
    "inmemorydb",
    "Creates in-memory-databases",
    0 },
  {
    ARG_OVERWRITE,
    0,
    "overwrite",
    "Overwrite existing keys",
    0 },
  {
    ARG_DUPLICATE,
    0,
    "duplicate",
    "Enables duplicate keys ('first': inserts them at the beginning;\n"
            "\t'last': inserts at the end (default))",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_USE_CURSORS,
    0,
    "use-cursors",
    "use cursors for insert/erase",
    0 },
  {
    ARG_RECOVERY,
    0,
    "use-recovery",
    "Uses recovery",
    0 },
  {
    ARG_KEY,
    0,
    "key",
    "Describes the key type ('uint16', 'uint32', 'uint64', 'custom', 'string', 'binary' (default))",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_DISABLE_MMAP,
    0,
    "no-mmap",
    "Disables memory mapped I/O",
    0 },
  {
    ARG_FULLCHECK,
    0,
    "fullcheck",
    "Sets 'fullcheck' algorithm ('find' uses ham_db_find,\n"
            "\t'reverse' searches backwards, leave empty for default)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_FULLCHECK_FREQUENCY,
    0,
    "fullcheck-frequency",
    "Sets how often/after how many operations the 'fullcheck' is performed\n"
            "\t(default: 100)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_PAGESIZE,
    0,
    "pagesize",
    "Sets the pagesize (use 0 for default)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_KEYSIZE,
    0,
    "keysize",
    "Sets the key size (use 0 for default)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_KEYSIZE_FIXED,
    0,
    "keysize-fixed",
    "Forces a fixed key size; default behavior depends on --keytype",
    0 },
  {
    ARG_RECSIZE,
    0,
    "recsize",
    "Sets the logical record size of the generated test data (default is 1024)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_RECSIZE_FIXED,
    0,
    "recsize-fixed",
    "Sets the hamsterdb btree record size (default is UNLIMITED)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_REC_INLINE,
    0,
    "force-records-inline",
    "Forces hamsterdb to store records in the Btree leaf",
    0 },
  {
    ARG_CACHE,
    0,
    "cache",
    "Sets the cachesize (use 0 for default) or 'unlimited'",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_HINTING,
    0,
    "hints",
    "sets hinting flags - one of:\n"
    "\tHAM_HINT_APPEND, HAM_HINT_PREPEND",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_DIRECT_ACCESS,
    0,
    "direct-access",
    "sets HAM_DIRECT_ACCESS flag",
    0 },
  {
    ARG_USE_TRANSACTIONS,
    0,
    "use-transactions",
    "use Transactions; arguments are \n"
    "\t'tmp' - create temp. Transactions;\n"
    "\tN - (number) group N statements into a Transaction;\n"
    "\t'all' - group the whole test into a single Transaction",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_USE_FSYNC,
    0,
    "use-fsync",
    "Calls fsync() when flushing to disk",
    0 },
  {
    ARG_USE_BERKELEYDB,
    0,
    "use-berkeleydb",
    "Enables use of berkeleydb (default: disabled)",
    0 },
  {
    ARG_USE_HAMSTERDB,
    0,
    "use-hamsterdb",
    "Enables use of hamsterdb ('true' (default), 'false')",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_NUM_THREADS,
    0,
    "num-threads",
    "sets the number of threads (default: 1)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_ERASE_PCT,
    0,
    "erase-pct",
    "Percentage of erase calls (default: 0)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_FIND_PCT,
    0,
    "find-pct",
    "Percentage of lookup calls (default: 0)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_TABLE_SCAN_PCT,
    0,
    "table-scan-pct",
    "Percentage of table-scans (default: 0)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_STOP_TIME,
    0,
    "stop-seconds",
    "Stops test after specified duration, in seconds",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_STOP_OPS,
    0,
    "stop-ops",
    "Stops test after executing specified number of operations (default: 1 mio)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_STOP_BYTES,
    0,
    "stop-bytes",
    "Stops test after inserting specified number of bytes",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_ENABLE_ENCRYPTION,
    0,
    "use-encryption",
    "Enables use of AES encryption",
    0 },
  {
    ARG_USE_REMOTE,
    0,
    "use-remote",
    "Runs test in remote client/server scenario",
    0 },
  {
    ARG_EXTKEY_THRESHOLD,
    0,
    "extkey-threshold",
    "Keys > threshold are moved to a blob",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_DUPTABLE_THRESHOLD,
    0,
    "duptable-threshold",
    "Duplicates > threshold are moved to an external table",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_BULK_ERASE,
    0,
    "bulk-erase",
    "Performs bulk erase of all inserted keys, empties the database",
    0 },
  {
    ARG_FLUSH_TXN_IMMEDIATELY,
    0,
    "flush-txn-immediately",
    "Immediately flushes transactions instead of buffering them",
    0 },
  {
    ARG_DISABLE_RECOVERY,
    0,
    "disable-recovery",
    "Disables recovery (HAM_DISABLE_RECOVERY)",
    0 },
  {
    ARG_JOURNAL_COMPRESSION,
    0,
    "journal-compression",
    "Pro: Enables journal compression ('none', 'zlib', 'snappy', 'lzf', 'lzo')",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_RECORD_COMPRESSION,
    0,
    "record-compression",
    "Pro: Enables record compression ('none', 'zlib', 'snappy', 'lzf', 'lzo')",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_KEY_COMPRESSION,
    0,
    "key-compression",
    "Pro: Enables key compression ('none', 'zlib', 'snappy', 'lzf', 'lzo', "
            "'bitmap')",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_PAX_LINEAR_THRESHOLD,
    0,
    "pax-linear-threshold",
    "Sets the threshold when switching from binary search to linear search",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_PAX_DISABLE_SIMD,
    0,
    "pax-disable-simd",
    "Pro: Enables use of SIMD instructions",
    0 },
  {
    ARG_READ_ONLY,
    0,
    "read-only",
    "Uses the HAM_READ_ONLY flag",
    0 },
  {
    ARG_ENABLE_CRC32,
    0,
    "enable-crc32",
    "Pro: Enables use of CRC32 verification",
    0 },
  {
    ARG_RECORD_NUMBER32,
    0,
    "record-number32",
    "Enables use of 32bit record numbers",
    0 },
  {
    ARG_RECORD_NUMBER64,
    0,
    "record-number64",
    "Enables use of 64bit record numbers",
    0 },
  {
    ARG_POSIX_FADVICE,
    0,
    "posix-fadvice",
    "Sets the posix_fadvise() parameter: 'random', 'normal' (default)",
    GETOPTS_NEED_ARGUMENT },
  {0, 0}
};

static int
parse_compression_type(const char *param)
{
  if (!strcmp(param, "none"))
    return (HAM_COMPRESSOR_NONE);
  if (!strcmp(param, "zlib"))
    return (HAM_COMPRESSOR_ZLIB);
  if (!strcmp(param, "snappy"))
    return (HAM_COMPRESSOR_SNAPPY);
  if (!strcmp(param, "lzf"))
    return (HAM_COMPRESSOR_LZF);
  if (!strcmp(param, "lzo"))
    return (HAM_COMPRESSOR_LZO);
  printf("invalid compression specifier '%s': expecting 'none', 'zlib', "
                  "'snappy', 'lzf', 'lzo'\n", param);
  exit(-1);
  return (HAM_COMPRESSOR_NONE);
}

namespace hamsterdb {
extern int g_linear_threshold;
extern bool g_is_simd_enabled;
};

static void
parse_config(int argc, char **argv, Configuration *c)
{
  unsigned opt;
  char *param;
	
  getopts_init(argc, argv, "ham_bench");

  /*
   * parse command line parameters
   */
  while ((opt = getopts(&opts[0], &param))) {
    if (opt == ARG_HELP) {
      getopts_usage(&opts[0]);
      exit(0);
    }
    else if (opt == ARG_QUIET) {
      c->quiet = true;
    }
    else if (opt == ARG_VERBOSE) {
      c->verbose++;
    }
    else if (opt == ARG_INMEMORY) {
      c->inmemory = true;
    }
    else if (opt == ARG_DISTRIBUTION) {
      if (param && !strcmp(param, "random"))
        c->distribution = Configuration::kDistributionRandom;
      else if (param && !strcmp(param, "ascending"))
        c->distribution = Configuration::kDistributionAscending;
      else if (param && !strcmp(param, "descending"))
        c->distribution = Configuration::kDistributionDescending;
      else if (param && !strcmp(param, "zipfian"))
        c->distribution = Configuration::kDistributionZipfian;
      else {
        printf("[FAIL] invalid parameter for --distribution\n");
        exit(-1);
      }
    }
    else if (opt == ARG_OVERWRITE) {
      if (c->duplicate) {
        printf("[FAIL] invalid combination: overwrite && duplicate\n");
        exit(-1);
      }
      c->overwrite = true;
    }
    else if (opt == ARG_DUPLICATE) {
      if (c->overwrite) {
        printf("[FAIL] invalid combination: overwrite && duplicate\n");
        exit(-1);
      }
      if (param && !strcmp(param, "first"))
        c->duplicate = Configuration::kDuplicateFirst;
      else if ((param && !strcmp(param, "last")) || !param)
        c->duplicate = Configuration::kDuplicateLast;
      else {
        printf("[FAIL] invalid parameter for 'duplicate'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_USE_CURSORS) {
      c->use_cursors = true;
    }
    else if (opt == ARG_RECOVERY) {
      c->use_recovery = true;
    }
    else if (opt == ARG_KEY) {
      if (param && !strcmp(param, "custom"))
        c->key_type = Configuration::kKeyCustom;
      else if (param && !strcmp(param, "uint8"))
        c->key_type = Configuration::kKeyUint8;
      else if (param && !strcmp(param, "uint16"))
        c->key_type = Configuration::kKeyUint16;
      else if (param && !strcmp(param, "uint32"))
        c->key_type = Configuration::kKeyUint32;
      else if (param && !strcmp(param, "uint64"))
        c->key_type = Configuration::kKeyUint64;
      else if (param && !strcmp(param, "real32"))
        c->key_type = Configuration::kKeyReal32;
      else if (param && !strcmp(param, "real64"))
        c->key_type = Configuration::kKeyReal64;
      else if (param && !strcmp(param, "string"))
        c->key_type = Configuration::kKeyString;
      else if (param && strcmp(param, "binary")) {
        printf("invalid parameter for --key\n");
        exit(-1);
      }
    }
    else if (opt == ARG_RECSIZE_FIXED) {
      if (param) {
        c->rec_size_fixed = strtoul(param, 0, 0);
        c->rec_size = c->rec_size_fixed;
      }
      else {
        printf("invalid parameter for --recsize-fixed (value is missing)\n");
        exit(-1);
      }
    }
    else if (opt == ARG_REC_INLINE) {
      c->force_records_inline = true;
    }
    else if (opt == ARG_NO_PROGRESS) {
      c->no_progress = true;
    }
    else if (opt == ARG_DISABLE_MMAP) {
      c->no_mmap = true;
    }
    else if (opt == ARG_PAGESIZE) {
      c->pagesize = strtoul(param, 0, 0);
    }
    else if (opt == ARG_KEYSIZE) {
      c->key_size = strtoul(param, 0, 0);
    }
    else if (opt == ARG_KEYSIZE_FIXED) {
      c->key_is_fixed_size = true;
    }
    else if (opt == ARG_RECSIZE) {
      c->rec_size = strtoul(param, 0, 0);
    }
    else if (opt == ARG_CACHE) {
      if (strstr(param, "unlimited"))
        c->cacheunlimited = true;
      else
        c->cachesize = strtoul(param, 0, 0);
    }
    else if (opt == ARG_HINTING) {
      if (!param) {
        printf("[FAIL] missing parameter for '--hints'\n");
        exit(-1);
      }
      if (strstr(param, "HAM_HINT_APPEND"))
        c->hints |= HAM_HINT_APPEND;
      if (strstr(param, "HAM_HINT_PREPEND"))
        c->hints |= HAM_HINT_PREPEND;
      if (param && !c->hints) {
        printf("[FAIL] invalid or missing parameter for '--hints'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_DIRECT_ACCESS) {
      c->direct_access = true;
    }
    else if (opt == ARG_USE_FSYNC) {
      c->use_fsync = true;
    }
    else if (opt == ARG_USE_BERKELEYDB) {
      c->use_berkeleydb = true;
    }
    else if (opt == ARG_USE_HAMSTERDB) {
      if (!param || !strcmp(param, "true"))
        c->use_hamsterdb = true;
      else if (param && !strcmp(param, "false"))
        c->use_hamsterdb = false;
      else {
        printf("[FAIL] invalid or missing parameter for 'use-hamsterdb'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_USE_TRANSACTIONS) {
      c->use_transactions = true;
      if (strcmp("tmp", param) == 0)
        c->transactions_nth = 0;
      else if (strcmp("all", param) == 0)
        c->transactions_nth = 0xffffffff;
      else {
        c->transactions_nth = strtoul(param, 0, 0);
        if (!c->transactions_nth) {
          printf("[FAIL] invalid parameter for 'use-transactions'\n");
          exit(-1);
        }
      }
    }
    else if (opt == ARG_REOPEN) {
      c->reopen = true;
    }
    else if (opt == ARG_OPEN) {
      c->open = true;
    }
    else if (opt == ARG_METRICS) {
      if (param && !strcmp(param, "none"))
        c->metrics = Configuration::kMetricsNone;
      else if (param && !strcmp(param, "all"))
        c->metrics = Configuration::kMetricsAll;
      else if (param && !strcmp(param, "png"))
        c->metrics = Configuration::kMetricsPng;
      else if (param && strcmp(param, "default")) {
        printf("[FAIL] invalid parameter for '--metrics'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_TEE) {
      if (!param) {
        printf("[FAIL] missing filename - use --tee=<file>\n");
        exit(-1);
      }
      c->tee_file = param;
    }
    else if (opt == ARG_SEED) {
      if (!param) {
        printf("[FAIL] missing parameter - use --seed=<arg>\n");
        exit(-1);
      }
      c->seed = strtoul(param, 0, 0);
    }
    else if (opt == ARG_FULLCHECK) {
      if (param && !strcmp(param, "find"))
        c->fullcheck = Configuration::kFullcheckFind;
      else if (param && !strcmp(param, "reverse"))
        c->fullcheck = Configuration::kFullcheckReverse;
      else if (param && !strcmp(param, "none"))
        c->fullcheck = Configuration::kFullcheckNone;
      else if (param && strcmp(param, "default")) {
        printf("[FAIL] invalid parameter for --fullcheck\n");
        exit(-1);
      }
    }
    else if (opt == ARG_FULLCHECK_FREQUENCY) {
      c->fullcheck_frequency = strtoul(param, 0, 0);
    }
    else if (opt == ARG_ERASE_PCT) {
      c->erase_pct = strtoul(param, 0, 0);
      if (!c->erase_pct || c->erase_pct > 100) {
        printf("[FAIL] invalid parameter for 'erase-pct'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_FIND_PCT) {
      c->find_pct = strtoul(param, 0, 0);
      if (!c->find_pct || c->find_pct > 100) {
        printf("[FAIL] invalid parameter for 'find-pct'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_TABLE_SCAN_PCT) {
      c->table_scan_pct = strtoul(param, 0, 0);
      if (!c->table_scan_pct || c->table_scan_pct > 100) {
        printf("[FAIL] invalid parameter for 'table-scan-pct'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_STOP_TIME) {
      c->limit_seconds = strtoul(param, 0, 0);
      if (!c->limit_seconds) {
        printf("[FAIL] invalid parameter for 'stop-seconds'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_STOP_BYTES) {
      c->limit_bytes = strtoul(param, 0, 0);
      if (!c->limit_bytes) {
        printf("[FAIL] invalid parameter for 'stop-bytes'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_STOP_OPS) {
      c->limit_ops = strtoul(param, 0, 0);
      if (!c->limit_ops) {
        printf("[FAIL] invalid parameter for 'stop-ops'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_NUM_THREADS) {
      c->num_threads = strtoul(param, 0, 0);
      if (!c->num_threads) {
        printf("[FAIL] invalid parameter for 'num-threads'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_ENABLE_ENCRYPTION) {
      c->use_encryption = true;
    }
    else if (opt == ARG_USE_REMOTE) {
#ifndef HAM_ENABLE_REMOTE
      printf("[FAIL] I was built without support for remote!\n");
      exit(-1);
#else
      c->use_remote = true;
#endif
    }
    else if (opt == ARG_EXTKEY_THRESHOLD) {
      c->extkey_threshold = strtoul(param, 0, 0);
      if (!c->extkey_threshold) {
        printf("[FAIL] invalid parameter for 'extkey-threshold'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_DUPTABLE_THRESHOLD) {
      c->duptable_threshold = strtoul(param, 0, 0);
      if (!c->duptable_threshold) {
        printf("[FAIL] invalid parameter for 'duptable-threshold'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_BULK_ERASE) {
      c->bulk_erase = true;
    }
    else if (opt == ARG_FLUSH_TXN_IMMEDIATELY) {
      c->flush_txn_immediately = true;
    }
    else if (opt == ARG_DISABLE_RECOVERY) {
      c->disable_recovery = true;
    }
    else if (opt == ARG_JOURNAL_COMPRESSION) {
      c->journal_compression = parse_compression_type(param);
    }
    else if (opt == ARG_RECORD_COMPRESSION) {
      c->record_compression = parse_compression_type(param);
    }
    else if (opt == ARG_KEY_COMPRESSION) {
      c->key_compression = parse_compression_type(param);
    }
    else if (opt == ARG_PAX_LINEAR_THRESHOLD) {
      hamsterdb::Globals::ms_linear_threshold = strtoul(param, 0, 0);
    }
    else if (opt == ARG_POSIX_FADVICE) {
      if (!strcmp(param, "normal"))
        c->posix_fadvice = HAM_POSIX_FADVICE_NORMAL;
      else if (!strcmp(param, "random"))
        c->posix_fadvice = HAM_POSIX_FADVICE_RANDOM;
      else {
        printf("[FAIL] invalid parameter for 'posix-fadvice'\n");
        exit(-1);
      }
    }
    else if (opt == ARG_PAX_DISABLE_SIMD) {
      hamsterdb::Globals::ms_is_simd_enabled = false;
    }
    else if (opt == ARG_ENABLE_CRC32) {
      c->enable_crc32 = true;
    }
    else if (opt == ARG_RECORD_NUMBER32) {
      c->record_number32 = true;
      c->key_is_fixed_size = true;
      c->key_size = 4;
      c->key_type = Configuration::kKeyUint32;
      c->distribution = Configuration::kDistributionAscending;
    }
    else if (opt == ARG_RECORD_NUMBER64) {
      c->record_number64 = true;
      c->key_is_fixed_size = true;
      c->key_size = 8;
      c->key_type = Configuration::kKeyUint64;
      c->distribution = Configuration::kDistributionAscending;
    }
    else if (opt == ARG_READ_ONLY) {
      c->read_only = true;
    }
    else if (opt == GETOPTS_PARAMETER) {
      c->filename = param;
    }
    else {
      printf("[FAIL] unknown parameter '%s'\n", param);
      exit(-1);
    }
  }

  if (c->bulk_erase) {
    if (!c->filename.empty()) {
      printf("[FAIL] '--bulk-erase' not supported with test files\n");
      exit(-1);
    }
    if (c->limit_seconds || c->limit_bytes) {
      printf("[FAIL] '--bulk-erase' only supported with --stop-ops\n");
      exit(-1);
    }
  }

  if (c->duplicate == Configuration::kDuplicateFirst && !c->use_cursors) {
    printf("[FAIL] '--duplicate=first' needs 'use-cursors'\n");
    exit(-1);
  }
}

static void
print_metrics(Metrics *metrics, Configuration *conf)
{
  const char *name = metrics->name;
  double total = metrics->insert_latency_total + metrics->find_latency_total
                  + metrics->erase_latency_total
                  + metrics->txn_commit_latency_total;

  printf("\t%s elapsed time (sec)             %f\n", name, total);
  printf("\t%s total_#ops                     %lu\n",
                  name, (long unsigned int)(metrics->insert_ops
                  + metrics->erase_ops + metrics->find_ops
                  + metrics->txn_commit_ops
                  + metrics->other_ops));
  if (metrics->insert_ops) {
    printf("\t%s insert_#ops                    %lu (%f/sec)\n",
                  name, (long unsigned int)metrics->insert_ops,
                  (double)metrics->insert_ops / metrics->insert_latency_total);
    printf("\t%s insert_throughput              %f/sec\n",
                  name, (double)metrics->insert_bytes
                        / metrics->insert_latency_total);
    printf("\t%s insert_latency (min, avg, max) %f, %f, %f\n",
                  name, metrics->insert_latency_min,
                  metrics->insert_latency_total / metrics->insert_ops,
                  metrics->insert_latency_max);
  }
  if (metrics->find_ops) {
    printf("\t%s find_#ops                      %lu (%f/sec)\n",
                  name, (long unsigned int)metrics->find_ops,
                  (double)metrics->find_ops / metrics->find_latency_total);
    printf("\t%s find_throughput                %f/sec\n",
                  name, (double)metrics->find_bytes
                        / metrics->find_latency_total);
    printf("\t%s find_latency (min, avg, max)   %f, %f, %f\n",
                  name, metrics->find_latency_min,
                  metrics->find_latency_total / metrics->find_ops,
                  metrics->find_latency_max);
  }
  if (metrics->erase_ops) {
    printf("\t%s erase_#ops                     %lu (%f/sec)\n",
                  name, (long unsigned int)metrics->erase_ops,
                  (double)metrics->erase_ops / metrics->erase_latency_total);
    printf("\t%s erase_latency (min, avg, max)  %f, %f, %f\n",
                  name, metrics->erase_latency_min,
                  metrics->erase_latency_total / metrics->erase_ops,
                  metrics->erase_latency_max);
  }
  if (!conf->inmemory) {
    if (!strcmp(name, "hamsterdb"))
      printf("\t%s filesize                       %lu\n",
                  name, (long unsigned int)boost::filesystem::file_size("test-ham.db"));
    else
      printf("\t%s filesize                       %lu\n",
                  name, (long unsigned int)boost::filesystem::file_size("test-berk.db"));
  }

  // print journal compression ratio
  if (conf->journal_compression && !strcmp(name, "hamsterdb")) {
    float ratio;
    if (metrics->hamster_metrics.journal_bytes_before_compression == 0)
      ratio = 1.f;
    else
      ratio = (float)metrics->hamster_metrics.journal_bytes_after_compression
                  / metrics->hamster_metrics.journal_bytes_before_compression;
    printf("\t%s journal_compression            %.3f\n", name, ratio);
  }

  // print record compression ratio
  if (conf->record_compression && !strcmp(name, "hamsterdb")) {
    float ratio;
    if (metrics->hamster_metrics.record_bytes_before_compression == 0)
      ratio = 1.f;
    else
      ratio = (float)metrics->hamster_metrics.record_bytes_after_compression
                  / metrics->hamster_metrics.record_bytes_before_compression;
    printf("\t%s record_compression             %.3f\n", name, ratio);
  }

  // print key compression ratio
  if (conf->key_compression && !strcmp(name, "hamsterdb")) {
    float ratio;
    if (metrics->hamster_metrics.key_bytes_before_compression == 0)
      ratio = 1.f;
    else
      ratio = (float)metrics->hamster_metrics.key_bytes_after_compression
                  / metrics->hamster_metrics.key_bytes_before_compression;
    printf("\t%s key_compression                %.3f\n", name, ratio);
  }

  if (conf->metrics != Configuration::kMetricsAll || strcmp(name, "hamsterdb"))
    return;

  printf("\thamsterdb mem_total_allocations       %lu\n",
          (long unsigned int)metrics->hamster_metrics.mem_total_allocations);
  printf("\thamsterdb mem_current_usage           %lu\n",
          (long unsigned int)metrics->hamster_metrics.mem_current_usage);
  printf("\thamsterdb mem_peak_usage              %lu\n",
          (long unsigned int)metrics->hamster_metrics.mem_peak_usage);
  printf("\thamsterdb page_count_fetched          %lu\n",
          (long unsigned int)metrics->hamster_metrics.page_count_fetched);
  printf("\thamsterdb page_count_flushed          %lu\n",
          (long unsigned int)metrics->hamster_metrics.page_count_flushed);
  printf("\thamsterdb page_count_type_index       %lu\n",
          (long unsigned int)metrics->hamster_metrics.page_count_type_index);
  printf("\thamsterdb page_count_type_blob        %lu\n",
          (long unsigned int)metrics->hamster_metrics.page_count_type_blob);
  printf("\thamsterdb page_count_type_page_manager %lu\n",
          (long unsigned int)metrics->hamster_metrics.page_count_type_page_manager);
  printf("\thamsterdb freelist_hits               %lu\n",
          (long unsigned int)metrics->hamster_metrics.freelist_hits);
  printf("\thamsterdb freelist_misses             %lu\n",
          (long unsigned int)metrics->hamster_metrics.freelist_misses);
  printf("\thamsterdb cache_hits                  %lu\n",
          (long unsigned int)metrics->hamster_metrics.cache_hits);
  printf("\thamsterdb cache_misses                %lu\n",
          (long unsigned int)metrics->hamster_metrics.cache_misses);
  printf("\thamsterdb blob_total_allocated        %lu\n",
          (long unsigned int)metrics->hamster_metrics.blob_total_allocated);
  printf("\thamsterdb blob_total_read             %lu\n",
          (long unsigned int)metrics->hamster_metrics.blob_total_read);
  printf("\thamsterdb btree_smo_split             %lu\n",
          (long unsigned int)metrics->hamster_metrics.btree_smo_split);
  printf("\thamsterdb btree_smo_merge             %lu\n",
          (long unsigned int)metrics->hamster_metrics.btree_smo_merge);
  printf("\thamsterdb extended_keys               %lu\n",
          (long unsigned int)metrics->hamster_metrics.extended_keys);
  printf("\thamsterdb extended_duptables          %lu\n",
          (long unsigned int)metrics->hamster_metrics.extended_duptables);
  printf("\thamsterdb journal_bytes_flushed       %lu\n",
          (long unsigned int)metrics->hamster_metrics.journal_bytes_flushed);
  printf("\thamsterdb simd_lane_width             %d\n",
          metrics->hamster_metrics.simd_lane_width);
}

struct Callable
{
  Callable(int id, Configuration *conf)
    : m_conf(conf), m_db(new HamsterDatabase(id, conf)), m_id(id),
        m_generator(0) {
      if (m_conf->filename.empty())
        m_generator = new RuntimeGenerator(m_id, m_conf, m_db, false);
      else
        m_generator = new ParserGenerator(m_id, m_conf, m_db, false);
  }

  ~Callable() {
    // TODO delete m_db!
    delete m_generator;
  }

  void operator()() {
    while (m_generator->execute())
      ;
  }

  void get_metrics(Metrics *metrics) {
    m_generator->get_metrics(metrics);
  }

  Configuration *m_conf;
  Database *m_db;
  int m_id;
  ::Generator *m_generator;
};

static void
thread_callback(Callable *c)
{
  return (*c)();
}

static void
add_metrics(Metrics *metrics, const Metrics *other)
{
  metrics->insert_ops += other->insert_ops;
  metrics->erase_ops += other->erase_ops;
  metrics->find_ops += other->find_ops;
  metrics->txn_commit_ops += other->txn_commit_ops;
  metrics->other_ops += other->other_ops;
  metrics->insert_bytes += other->insert_bytes;
  metrics->find_bytes += other->find_bytes;
  metrics->insert_latency_total += other->insert_latency_total;
  metrics->erase_latency_total += other->erase_latency_total;
  metrics->find_latency_total += other->find_latency_total;
  metrics->txn_commit_latency_total += other->txn_commit_latency_total;
}

template<typename DatabaseType, typename GeneratorType>
static bool
run_single_test(Configuration *conf)
{
  Database *db = new DatabaseType(0, conf);
  GeneratorType generator(0, conf, db, true);

  // create additional hamsterdb threads
  std::vector<boost::thread *> threads;
  std::vector<Callable *> callables;
  for (int i = 1; i < conf->num_threads; i++) {
    Callable *c = new Callable(i, conf);
    callables.push_back(c);
    threads.push_back(new boost::thread(thread_callback, c));
  }

  //int op = 0;
  while (generator.execute()) {
#if 0
    op++;
    if (op > 0
          && conf->fullcheck_frequency != 0
          && (op % conf->fullcheck_frequency) == 0
          && db->is_open()) {
      generator.tee("FULLCHECK");
      ham_status_t st = db->check_integrity();
      if (st != 0) {
        LOG_ERROR(("fullcheck failed: hamster integrity status %d\n", st));
        return false;
      }
    }
#endif
  }

  // have to collect metrics now while the database was not yet closed
  Metrics metrics;
  generator.get_metrics(&metrics);

  // "add up" the metrics from the other threads and join the other threads
  std::vector<boost::thread *>::iterator it;
  std::vector<Callable *>::iterator cit = callables.begin();
  for (it = threads.begin(); it != threads.end(); it++, cit++) {
    Metrics m;
    (*cit)->get_metrics(&m);
    add_metrics(&metrics, &m);
    (*it)->join();
    delete *it;
    delete *cit;
  }

  // reopen (if required)
  if (conf->reopen) {
    db->close_env();
    db->open_env();
    generator.open();
  }
  generator.close();
  db->close_env();
  delete db;

  bool ok = generator.was_successful();

  if (ok) {
    printf("\n[OK] %s\n", conf->filename.c_str());
    if (!conf->quiet || conf->metrics != Configuration::kMetricsNone) {
      printf("\ttotal elapsed time (sec)                 %f\n",
                  metrics.elapsed_wallclock_seconds);
      print_metrics(&metrics, conf);
    }
  }
  else
    printf("\n[FAIL] %s\n", conf->filename.c_str());
  return (ok);
}

#ifdef HAM_WITH_BERKELEYDB
static bool
are_keys_equal(ham_key_t *key1, ham_key_t *key2)
{
  if (key1->size != key2->size) {
    LOG_ERROR(("keys are not equal - hamsterdb size %u, berkeleydb %u\n",
                            key1->size, key2->size));
    return (false);
  }

  if (key1->size == 0)
    return (true);

  if (::memcmp(key1->data, key2->data, key1->size)) {
    LOG_ERROR(("keys are not equal - data differs\n"));
    return (false);
  }
  return (true);
}

static bool
are_records_equal(const ham_record_t *rec1, const ham_record_t *rec2)
{
  if (rec1->size != rec2->size) {
    LOG_ERROR(("records are not equal - hamsterdb size %u, berkeleydb %u\n",
                            rec1->size, rec2->size));
    return (false);
  }

  if (rec1->size == 0)
    return (true);

  if (::memcmp(rec1->data, rec2->data, rec1->size)) {
    LOG_ERROR(("records are not equal - data differs\n"));
    return (false);
  }
  return (true);
}

static bool
run_fullcheck(Configuration *conf, ::Generator *gen1, ::Generator *gen2)
{
  ham_status_t st1, st2;
  Database::Cursor *c1 = gen1->get_db()->cursor_create();
  Database::Cursor *c2 = gen2->get_db()->cursor_create();

  gen1->tee("FULLCHECK");

  // perform an integrity check
  st1 = gen1->get_db()->check_integrity();
  if (st1 != 0) {
    LOG_ERROR(("integrity check failed: hamster integrity status %d\n", st1));
    return (false);
  }

  do {
    ham_key_t key1 = {0};
    ham_record_t rec1 = {0};
    ham_key_t key2 = {0};
    ham_record_t rec2 = {0};

    // iterate over both databases
    if (conf->fullcheck == Configuration::kFullcheckFind) {
      st2 = gen2->get_db()->cursor_get_next(c2, &key2, &rec2, true);
      if (st2 == HAM_KEY_NOT_FOUND)
        goto bail;
      st1 = gen1->get_db()->find(0, &key2, &rec1);
      key1 = key2; // make sure are_keys_equal() returns true
    }
    else if (conf->fullcheck == Configuration::kFullcheckReverse) {
      st1 = gen1->get_db()->cursor_get_previous(c1, &key1, &rec1, false);
      st2 = gen2->get_db()->cursor_get_previous(c2, &key2, &rec2, false);
    }
    else {
      st1 = gen1->get_db()->cursor_get_next(c1, &key1, &rec1, false);
      st2 = gen2->get_db()->cursor_get_next(c2, &key2, &rec2, false);
    }

    if (st1 == st2 && st1 == HAM_KEY_NOT_FOUND)
      break;

    bool failed = false;

    // compare status
    if (st1 != st2) {
      LOG_ERROR(("fullcheck failed: hamster status %d, berkeley status %d\n",
                              st1, st2));
      return (false);
    }
    // compare keys
    if (!are_keys_equal(&key1, &key2))
      failed = true;
    // compare records
    if (!are_records_equal(&rec1, &rec2))
      failed = true;

    if (failed || conf->verbose > 1) {
      std::string s1, s2;
      switch (conf->key_type) {
        case Configuration::kKeyUint8:
          printf("fullcheck %d/%d, keys %d/%d, blob size %d/%d\n", st1, st2,
                  key1.data ? (int)*(char *)key1.data : 0,
                  key2.data ? (int)*(char *)key2.data : 0,
                  rec1.size, rec2.size);
          break;
        case Configuration::kKeyUint16:
          printf("fullcheck %d/%d, keys %d/%d, blob size %d/%d\n", st1, st2,
                  key1.data ? (int)*(uint16_t *)key1.data : 0,
                  key2.data ? (int)*(uint16_t *)key2.data : 0,
                  rec1.size, rec2.size);
          break;
        case Configuration::kKeyUint32:
          printf("fullcheck %d/%d, keys %u/%u, blob size %d/%d\n", st1, st2,
                  key1.data ? *(uint32_t *)key1.data : 0,
                  key2.data ? *(uint32_t *)key2.data : 0,
                  rec1.size, rec2.size);
          break;
        case Configuration::kKeyUint64:
          printf("fullcheck %d/%d, keys %lu/%lu, blob size %d/%d\n", st1, st2,
                  key1.data ? *(uint64_t *)key1.data : 0,
                  key2.data ? *(uint64_t *)key2.data : 0,
                  rec1.size, rec2.size);
          break;
        case Configuration::kKeyReal32:
          printf("fullcheck %d/%d, keys %f/%f, blob size %d/%d\n", st1, st2,
                  key1.data ? *(float *)key1.data : 0,
                  key2.data ? *(float *)key2.data : 0,
                  rec1.size, rec2.size);
          break;
        case Configuration::kKeyReal64:
          printf("fullcheck %d/%d, keys %f/%f, blob size %d/%d\n", st1, st2,
                  key1.data ? (float)*(double *)key1.data : 0,
                  key2.data ? (float)*(double *)key2.data : 0,
                  rec1.size, rec2.size);
          break;
        default:
          s1 = std::string((const char *)key1.data, key1.size);
          s2 = std::string((const char *)key2.data, key2.size);
          printf("fullcheck %d/%d, keys %s/%s, blob size %d/%d\n", st1, st2,
                  s1.c_str(), s2.c_str(), rec1.size, rec2.size);
          break;
      }

      if (failed)
        return (false);
    }
  } while (st1 == 0 && st2 == 0);

bail:
  gen1->get_db()->cursor_close(c1);
  gen2->get_db()->cursor_close(c2);

  // everything was ok
  return (true);
}

template<typename GeneratorType>
static bool
run_both_tests(Configuration *conf)
{
  if (conf->num_threads != 1) {
    printf("sorry, only one thread supported if running with both databases\n");
    exit(-1);
  }

  bool ok = true;
  Database *db1 = new HamsterDatabase(0, conf);
  Database *db2 = new BerkeleyDatabase(1, conf);
  GeneratorType generator1(0, conf, db1, true);
  GeneratorType generator2(0, conf, db2, false);
  uint64_t op = 0;
  while (generator1.execute()) {
    bool b = generator2.execute();
    assert(b);
    op++;

    bool fullcheck = false;
    if (generator1.get_status() == ::Generator::kCommandFullcheck) {
      fullcheck = true;
    }
    else if (conf->fullcheck != Configuration::kFullcheckNone) {
      if (op > 0
            && conf->fullcheck_frequency != 0
            && (op % conf->fullcheck_frequency) == 0
            && generator1.get_db()->is_open()
            && generator2.get_db()->is_open())
        fullcheck = true;
    }

    if (fullcheck) {
      ok = run_fullcheck(conf, &generator1, &generator2);
      if (!ok)
        break;
    }
    else {
      if (generator1.get_status() != generator2.get_status()) {
        LOG_ERROR(("Status mismatch - %d vs %d\n",
              generator1.get_status(), generator2.get_status()));
        ok = false;
        break;
      }

      if (!are_records_equal(generator1.get_record(),
                  generator2.get_record())) {
        LOG_ERROR(("Record mismatch\n"));
        ok = false;
        break;
      }
    }
  }

  if (ok)
    assert(false == generator2.execute());

  // have to collect metrics now while the database was not yet closed
  Metrics metrics1;
  generator1.get_metrics(&metrics1);
  Metrics metrics2;
  generator2.get_metrics(&metrics2);

  // now reopen and run another fullcheck
  if (ok && conf->reopen) {
    generator1.close();
    generator2.close();
    generator1.open();
    generator2.open();

    if (conf->fullcheck != Configuration::kFullcheckNone)
      ok = run_fullcheck(conf, &generator1, &generator2);
  }

  generator1.close();
  generator2.close();
  delete db1;
  delete db2;

  if (!generator1.was_successful())
    ok = false;

  if (ok) {
    printf("[OK] %s\n", conf->filename.c_str());
    if (!conf->quiet && conf->metrics != Configuration::kMetricsNone)
      printf("\ttotal elapsed time (sec)                 %f\n",
                  metrics1.elapsed_wallclock_seconds);
      print_metrics(&metrics1, conf);
      print_metrics(&metrics2, conf);
  }
  else
    printf("[FAIL] %s\n", conf->filename.c_str());
  return (ok);
}
#endif

int
main(int argc, char **argv)
{
  Configuration c;
  parse_config(argc, argv, &c);

  // ALWAYS set the seed!
  if (c.seed == 0)
    c.seed = (long)::time(0);

  if (!c.quiet)
    print_banner("ham_bench");

  if (ham_is_debug()) {
    printf("\t!!!!!!!! DEBUG BUILD\n");
    printf("\tDebug builds contain many integrity checks and are "
           "extremely\n\tslow. Please do not use for "
           "benchmarking!\n\n");
  }

  // ALWAYS dump the configuration
  c.print();

  // set a limit
  if (!c.limit_bytes && !c.limit_seconds && !c.limit_ops)
    c.limit_ops = 1000000;

  if (c.verbose && c.metrics == Configuration::kMetricsDefault)
    c.metrics = Configuration::kMetricsAll;

  bool ok = true;

  // if berkeleydb is disabled, and hamsterdb runs in only one thread:
  // just execute the test single-threaded
  if (c.use_hamsterdb && !c.use_berkeleydb) {
    if (c.filename.empty())
      ok = run_single_test<HamsterDatabase, RuntimeGenerator>(&c);
    else
      ok = run_single_test<HamsterDatabase, ParserGenerator>(&c);
  }
  else if (c.use_berkeleydb && !c.use_hamsterdb) {
#ifdef HAM_WITH_BERKELEYDB
    if (c.filename.empty())
      ok = run_single_test<BerkeleyDatabase, RuntimeGenerator>(&c);
    else
      ok = run_single_test<BerkeleyDatabase, ParserGenerator>(&c);
#else
    printf("[FAIL] I was built without support for berkeleydb!\n");
    ok = false;
#endif
  }
  else {
#ifdef HAM_WITH_BERKELEYDB
    if (c.filename.empty())
      ok = run_both_tests<RuntimeGenerator>(&c);
    else
      ok = run_both_tests<ParserGenerator>(&c);
#else
    printf("[FAIL] I was built without support for berkeleydb!\n");
    ok = false;
#endif
  }

  return (ok ? 0 : 1);
}
