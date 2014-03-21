/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include <stdexcept>
#include <cstring>
#include <vector>
#include <sstream>

#include <ham/hamsterdb_int.h>
#include "../src/env_local.h"
#include "../src/errorinducer.h"
#include "os.hpp"

using namespace hamsterdb;

#define NUM_STEPS     10

int
default_compressor() {
#ifdef HAM_COMPRESSOR_SNAPPY
  return (HAM_COMPRESSOR_SNAPPY);
#else
  return (2);
#endif
}

ham_parameter_t *
get_parameters(bool use_compression, int page_size = 0) {
  static ham_parameter_t params[3] = {{0, 0}};
  int p = 0;
  if (use_compression) {
    params[p].name = HAM_PARAM_ENABLE_JOURNAL_COMPRESSION;
    params[p].value = default_compressor();
    p++;
  }
  if (page_size) {
    params[p].name = HAM_PARAM_PAGE_SIZE;
    params[p].value = page_size;
    p++;
  }
  return (&params[0]);
}

void
create_key(ham_key_t *key, int i) {
  char *p = (char *)key->data;
  *(int *)&p[0] = i;
  // also set at the end of the key to force reloading of extended key blobs
  if (key->size > 8)
    *(int *)&p[key->size - sizeof(int)] = i;
}

void
usage() {
  printf("usage: ./recovery insert <key_size> <rec_size> <i> <dupes> "
       "<use_compression> <inducer>\n");
  printf("usage: ./recovery erase <key_size> <i> <dupes> "
       "<use_compression> <inducer>\n");
  printf("usage: ./recovery recover <use_compression>\n");
  printf("usage: ./recovery verify <key_size> <rec_size> <i> <dupes> "
       "<use_compression> <exist>\n");
}

void
insert(int argc, char **argv) {
  if (argc != 8) {
    usage();
    exit(-1);
  }

  ham_status_t st;
  ham_db_t *db;
  ham_env_t *env;

  int key_size = (int)strtol(argv[2], 0, 0);
  int rec_size = (int)strtol(argv[3], 0, 0);
  int i     = (int)strtol(argv[4], 0, 0);
  int dupes   = (int)strtol(argv[5], 0, 0);
  int use_compression = (int)strtol(argv[6], 0, 0);
  int inducer = (int)strtol(argv[7], 0, 0);
  printf("insert: key_size=%d, rec_size=%d, i=%d, dupes=%d, "
         "use_compression=%d, inducer=%d\n", key_size, rec_size, i,
         dupes, use_compression, inducer);

  ham_key_t key = {0};
  key.data = malloc(key_size);
  key.size = key_size;
  memset(key.data, 0, key.size);

  ham_record_t rec = {0};
  rec.data = malloc(rec_size);
  rec.size = rec_size;
  memset(rec.data, 0, rec.size);

  // if db does not yet exist: create it, otherwise open it
  st = ham_env_open(&env, "recovery.db",
            HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY,
            get_parameters(use_compression));
  if (st == HAM_FILE_NOT_FOUND) {
    st = ham_env_create(&env, "recovery.db",
              HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY, 0644,
              get_parameters(use_compression, 1024));
    if (st) {
      printf("ham_env_create failed: %d\n", (int)st);
      exit(-1);
    }

    st = ham_env_create_db(env, &db, 1, HAM_ENABLE_DUPLICATE_KEYS, 0);
    if (st) {
      printf("ham_env_create_db failed: %d\n", (int)st);
      exit(-1);
    }
  }
  else if (st) {
    printf("ham_env_open failed: %d\n", (int)st);
    exit(-1);
  }
  else {
    st = ham_env_open_db(env, &db, 1, 0, 0);
    if (st) {
      printf("ham_env_open_db failed: %d\n", (int)st);
      exit(-1);
    }
  }

  // create a new txn and insert the new key/value pair.
  // flushing the txn will fail b/c of the error inducer
  ham_txn_t *txn = 0;
  st = ham_txn_begin(&txn, env, 0, 0, 0);
  if (st) {
    printf("ham_txn_begin failed: %d\n", (int)st);
    exit(-1);
  }

  ErrorInducer *ei = new ErrorInducer();
  ((LocalEnvironment *)env)->get_changeset().m_inducer = ei;
  ei->add(ErrorInducer::kChangesetFlush, inducer);

  for (int j = 0; j < NUM_STEPS; j++) {
    create_key(&key, i * NUM_STEPS + j);
    st = ham_db_insert(db, txn, &key, &rec, dupes ? HAM_DUPLICATE : 0);
    if (st) {
      if (st == HAM_INTERNAL_ERROR)
        break;
      printf("ham_db_insert failed: %d (%s)\n", (int)st, ham_strerror(st));
      exit(-1);
    }
  }

  if (txn)
    st = ham_txn_commit(txn, 0);
  if (st == 0)
    exit(0); // we must have skipped all induced errors
  else if (st != HAM_INTERNAL_ERROR) {
    printf("ham_txn_commit failed: %d\n", (int)st);
    exit(-1);
  }
}

void
erase(int argc, char **argv) {
  if (argc != 7) {
    usage();
    exit(-1);
  }

  ham_status_t st = 0;
  ham_db_t *db;
  ham_env_t *env;

  int key_size = (int)strtol(argv[2], 0, 0);
  int i     = (int)strtol(argv[3], 0, 0);
  int dupes   = (int)strtol(argv[4], 0, 0);
  int use_compression = (int)strtol(argv[5], 0, 0);
  int inducer = (int)strtol(argv[6], 0, 0);
  printf("erase: key_size=%d, i=%d, dupes=%d, use_compression=%d, inducer=%d\n",
      key_size, i, dupes, use_compression, inducer);

  ham_key_t key = {0};
  key.data = malloc(key_size);
  key.size = key_size;
  memset(key.data, 0, key.size);

  st = ham_env_open(&env, "recovery.db",
            HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY,
            get_parameters(use_compression));
  if (st) {
    printf("ham_env_open failed: %d\n", (int)st);
    exit(-1);
  }
  st = ham_env_open_db(env, &db, 1, 0, 0);
  if (st) {
    printf("ham_env_open_db failed: %d\n", (int)st);
    exit(-1);
  }

  // create a new txn and erase the keys
  // flushing the txn will fail b/c of the error inducer
  ham_txn_t *txn = 0;
  st = ham_txn_begin(&txn, env, 0, 0, 0);
  if (st) {
    printf("ham_txn_begin failed: %d\n", (int)st);
    exit(-1);
  }

  ErrorInducer *ei = new ErrorInducer();
  ((LocalEnvironment *)env)->get_changeset().m_inducer = ei;
  ei->add(ErrorInducer::kChangesetFlush, inducer);

  for (int j = 0; j < NUM_STEPS; j++) {
    create_key(&key, i * NUM_STEPS + j);
    if (dupes) {
      st = ham_db_erase(db, txn, &key, 0);
      if (st) {
        if (st == HAM_INTERNAL_ERROR)
          break;
        printf("ham_db_erase failed: %d (%s)\n", (int)st, ham_strerror(st));
        exit(-1);
      }
    }
    else {
      st = ham_db_erase(db, txn, &key, 0);
      if (st) {
        if (st == HAM_INTERNAL_ERROR)
          break;
        printf("ham_db_erase failed: %d (%s)\n", (int)st, ham_strerror(st));
        exit(-1);
      }
    }
  }

  if (txn)
    st = ham_txn_commit(txn, 0);
  if (st == 0)
    exit(0); // we must have skipped all induced errors
  else if (st != HAM_INTERNAL_ERROR) {
    printf("ham_txn_commit failed: %d\n", (int)st);
    exit(-1);
  }
}

void
recover(int argc, char **argv) {
  if (argc != 3) {
    usage();
    exit(-1);
  }

  int use_compression = (int)strtol(argv[2], 0, 0);
  printf("recover: use_compression=%d\n", use_compression);

  ham_status_t st;
  ham_env_t *env;

  st = ham_env_open(&env, "recovery.db",
            HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY,
            get_parameters(use_compression));
  if (st == 0)
    exit(0);
  if (st != HAM_NEED_RECOVERY) {
    printf("ham_env_open failed: %d\n", (int)st);
    exit(-1);
  }

  st = ham_env_open(&env, "recovery.db",
            HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY,
            get_parameters(use_compression));
  if (st != 0) {
    printf("ham_env_open failed: %d\n", (int)st);
    exit(-1);
  }

  st = ham_env_close(env, 0);
  if (st != 0) {
    printf("ham_env_close failed: %d\n", (int)st);
    exit(-1);
  }

  exit(0);
}

void
verify(int argc, char **argv) {
  if (argc != 8) {
    usage();
    exit(-1);
  }

  int key_size = (int)strtol(argv[2], 0, 0);
  int rec_size = (int)strtol(argv[3], 0, 0);
  int maxi    = (int)strtol(argv[4], 0, 0);
  int dupes   = (int)strtol(argv[5], 0, 0);
  int use_compression = (int)strtol(argv[6], 0, 0);
  int exist   = (int)strtol(argv[7], 0, 0);
  printf("verify: key_size=%d, rec_size=%d, i=%d, dupes=%d, "
         "use_compression=%d, exist=%d\n", key_size, rec_size, maxi,
         dupes, use_compression, exist);

  ham_status_t st;
  ham_db_t *db;
  ham_env_t *env;

  ham_key_t key = {0};
  key.data = malloc(key_size);
  key.size = key_size;
  memset(key.data, 0, key.size);

  ham_record_t rec = {0};
  rec.data = malloc(rec_size);
  rec.size = rec_size;
  memset(rec.data, 0, rec.size);

  ham_record_t rec2 = {0};

  st = ham_env_open(&env, "recovery.db", HAM_READ_ONLY, 0);
  if (st) {
    printf("ham_env_open failed: %d\n", (int)st);
    exit(-1);
  }

  st = ham_env_open_db(env, &db, 1, 0, 0);
  if (st) {
    printf("ham_env_open_db failed: %d\n", (int)st);
    exit(-1);
  }

  st = ham_db_check_integrity(db, 0);
  if (st) {
    printf("ham_db_check_integrity failed: %d\n", (int)st);
    exit(-1);
  }

  for (int i = 0; i <= maxi; i++) {
    for (int j = 0; j < NUM_STEPS; j++) {
      create_key(&key, i * NUM_STEPS + j);

      st = ham_db_find(db, 0, &key, &rec2, 0);
      if (i < maxi && st != 0) {
        printf("ham_db_find failed but shouldn't: %d, i=%d, j=%d\n",
                        (int)st, i, j);
        exit(-1);
      }
      else if (i == maxi) {
        if (exist && st != 0) {
          printf("ham_db_find failed but shouldn't: %d, i=%d, j=%d\n",
                          (int)st, i, j);
          exit(-1);
        }
        if (!exist && st != HAM_KEY_NOT_FOUND) {
          printf("ham_db_find succeeded but shouldn't: %d, i=%d, j=%d\n",
                          (int)st, i, j);
          exit(-1);
        }
      }

      if (exist) {
        if (rec.size != rec2.size
            || memcmp(rec.data, rec2.data, rec2.size)) {
          printf("record mismatch\n");
          exit(-1);
        }
      }
    }
  }
}

int
main(int argc, char **argv) {
  if (argc == 1) {
    usage();
    exit(-1);
  }

  const char *mode = argv[1];
  if (!strcmp("insert", mode)) {
    insert(argc, argv);
    return 0;
  }
  if (!strcmp("erase", mode)) {
    erase(argc, argv);
    return 0;
  }
  if (!strcmp("recover", mode)) {
    recover(argc, argv);
    return 0;
  }
  if (!strcmp("verify", mode)) {
    verify(argc, argv);
    return 0;
  }
  usage();
  return -1;
}

