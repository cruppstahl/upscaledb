/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef CONFIGURATION_H__
#define CONFIGURATION_H__

#include <string>
#include <cstdio>
#include <ham/hamsterdb.h>
#include <boost/thread.hpp>
#include <boost/cstdint.hpp> // MSVC 2008 does not have stdint
using namespace boost;

struct Configuration
{
  enum {
    kKeyBinary = 0,
    kKeyString,
    kKeyCustom,
    kKeyUint8,
    kKeyUint16,
    kKeyUint32,
    kKeyUint64,
    kKeyReal32,
    kKeyReal64
  };

  enum {
    kFullcheckDefault = 0,
    kFullcheckFind,
    kFullcheckReverse,
    kFullcheckNone
  };

  enum {
    kDistributionRandom = 0,
    kDistributionAscending,
    kDistributionDescending,
    kDistributionZipfian
  };

  enum {
    kDuplicateDisabled = 0,
    kDuplicateFirst,
    kDuplicateLast
  };

  enum {
    kMetricsNone,
    kMetricsDefault,
    kMetricsPng,
    kMetricsAll
  };

  enum {
    kDefaultKeysize = 16,
    kDefaultRecsize = 1024
  };

  Configuration()
    : profile(true), verbose(0), no_progress(false), reopen(false), open(false),
      quiet(false), key_type(kKeyBinary),
      rec_size_fixed(HAM_RECORD_SIZE_UNLIMITED), force_records_inline(false),
      distribution(kDistributionRandom), seed(0), limit_ops(0),
      limit_seconds(0), limit_bytes(0), key_size(kDefaultKeysize),
      btree_key_size(0), key_is_fixed_size(false), rec_size(kDefaultRecsize),
      erase_pct(0), find_pct(0), table_scan_pct(0), use_encryption(false),
      use_remote(false), duplicate(kDuplicateDisabled), overwrite(false),
      transactions_nth(0), use_fsync(false), inmemory(false),
      use_recovery(false), use_transactions(false), no_mmap(false),
      cacheunlimited(false), cachesize(0), hints(0), pagesize(0),
      num_threads(1), use_cursors(false), direct_access(false),
      use_berkeleydb(false), use_hamsterdb(true), fullcheck(kFullcheckDefault),
      fullcheck_frequency(100), metrics(kMetricsDefault), extended_keys(false) {
  }

  void print() const {
    printf("Configuration: --seed=%lu ", seed);
    if (use_encryption)
      printf("--use-encryption ");
    if (use_remote)
      printf("--use-remote ");
    if (use_fsync)
      printf("--use-fsync ");
    if (use_recovery)
      printf("--use-recovery ");
    if (use_cursors)
      printf("--use-cursors ");
    if (duplicate == kDuplicateFirst)
      printf("--duplicate=first ");
    else if (duplicate == kDuplicateLast)
      printf("--duplicate=last ");
    if (overwrite)
      printf("--overwrite ");
    if (inmemory)
      printf("--inmemorydb ");
    if (no_mmap)
      printf("--no-mmap ");
    if (extended_keys)
      printf("--use-extended ");
    if (cacheunlimited)
      printf("--cache=unlimited ");
    if (cachesize)
      printf("--cache=%d ", cachesize);
    if (pagesize)
      printf("--pagesize=%d ", pagesize);
    if (num_threads > 1)
      printf("--num-threads=%d ", num_threads);
    if (direct_access)
      printf("--direct-access ");
    if (use_berkeleydb)
      printf("--use-berkeleydb ");
    if (!use_hamsterdb)
      printf("--use-hamsterdb=false ");
    if (use_transactions) {
      if (transactions_nth)
        printf("--use-transactions=tmp ");
      else if (transactions_nth == 0xffffffffu)
        printf("--use-transactions=all ");
      else
        printf("--use-transactions=%d ", transactions_nth);
    }
    if (hints & HAM_HINT_APPEND)
      printf("--hints=HAM_HINT_APPEND ");
    else if (hints & HAM_HINT_PREPEND)
      printf("--hints=HAM_HINT_PREPEND ");
    if (fullcheck == kFullcheckFind)
      printf("--fullcheck=find ");
    if (fullcheck == kFullcheckReverse)
      printf("--fullcheck=reverse ");
    if (fullcheck == kFullcheckNone)
      printf("--fullcheck=none ");
    if (!filename.empty()) {
      printf("%s\n", filename.c_str());
    }
    else {
      if (key_type == kKeyCustom)
        printf("--key=custom ");
      else if (key_type == kKeyUint8)
        printf("--key=uint8 ");
      else if (key_type == kKeyUint16)
        printf("--key=uint16 ");
      else if (key_type == kKeyUint32)
        printf("--key=uint32 ");
      else if (key_type == kKeyUint64)
        printf("--key=uint64 ");
      else if (key_type == kKeyReal32)
        printf("--key=real32 ");
      else if (key_type == kKeyReal64)
        printf("--key=real64 ");
      if (key_size != kDefaultKeysize)
        printf("--keysize=%d ", key_size);
      if (btree_key_size)
        printf("--btree-keysize=%d ", btree_key_size);
      if (key_is_fixed_size)
        printf("--keysize-fixed ");
      if (rec_size_fixed != HAM_RECORD_SIZE_UNLIMITED)
        printf("--recsize-fixed=%d ", rec_size_fixed);
      if (force_records_inline)
        printf("--force-records-inline ");
      printf("--recsize=%d ", rec_size);
      if (distribution == kDistributionRandom)
        printf("--distribution=random ");
      if (distribution == kDistributionAscending)
        printf("--distribution=ascending ");
      if (distribution == kDistributionDescending)
        printf("--distribution=descending ");
      if (distribution == kDistributionZipfian)
        printf("--distribution=zipfian ");
      if (limit_ops)
        printf("--stop-ops=%lu ", limit_ops);
      if (limit_seconds)
        printf("--stop-seconds=%lu ", limit_seconds);
      if (limit_bytes)
        printf("--stop-bytes=%ld ", limit_bytes);
      if (erase_pct)
        printf("--erase-pct=%d ", erase_pct);
      if (find_pct)
        printf("--find-pct=%d ", find_pct);
      if (table_scan_pct)
        printf("--table-scan-pct=%d ", table_scan_pct);
      printf("\n");
    }
  }

  bool profile;
  unsigned verbose;
  bool no_progress;
  bool reopen;
  bool open;
  std::string filename;
  bool quiet;
  int key_type;
  unsigned rec_size_fixed;
  bool force_records_inline;
  int distribution;
  long seed;
  uint64_t limit_ops;
  uint64_t limit_seconds;
  uint64_t limit_bytes;
  int key_size;
  int btree_key_size;
  bool key_is_fixed_size;
  int rec_size;
  int erase_pct;
  int find_pct;
  int table_scan_pct;
  bool use_encryption;
  bool use_remote;
  int duplicate;
  bool overwrite;
  uint32_t transactions_nth;
  bool use_fsync;
  bool inmemory;
  bool use_recovery;
  bool use_transactions;
  bool no_mmap;
  bool cacheunlimited;
  int cachesize;
  int hints;
  int pagesize;
  int num_threads;
  bool use_cursors;
  bool direct_access;
  bool use_berkeleydb;
  bool use_hamsterdb;
  int fullcheck;
  int fullcheck_frequency;
  std::string tee_file;
  int metrics;
  bool extended_keys;
};

#endif /* CONFIGURATION_H__ */
