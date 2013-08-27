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

#include "../src/config.h"

#include <stdexcept>
#include <cstring>
#include <vector>
#include <sstream>

#include "../src/log.h"
#include "../src/env_local.h"
#include "../src/errorinducer.h"
#include "os.hpp"

/* this function pointer is defined in changeset.c */
extern "C" {
typedef void (*hook_func_t)(void);
  extern hook_func_t g_CHANGESET_POST_LOG_HOOK;
  extern hook_func_t g_BTREE_INSERT_SPLIT_HOOK;
}

using namespace hamsterdb;

#define NUM_STEPS     10

void
usage() {
  printf("usage: ./recovery insert <keysize> <recsize> <i> <dupes> "
       "<use_txn> <inducer>\n");
  printf("usage: ./recovery erase <keysize> <i> <dupes> "
       "<use_txn> <inducer>\n");
  printf("usage: ./recovery recover <use_txn>\n");
  printf("usage: ./recovery verify <keysize> <recsize> <i> <dupes> "
       "<use_txn> <exist>\n");
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

  int keysize = (int)strtol(argv[2], 0, 0);
  int recsize = (int)strtol(argv[3], 0, 0);
  int i     = (int)strtol(argv[4], 0, 0);
  int dupes   = (int)strtol(argv[5], 0, 0);
  int use_txn = (int)strtol(argv[6], 0, 0);
  int inducer = (int)strtol(argv[7], 0, 0);
  printf("insert: keysize=%d, recsize=%d, i=%d, dupes=%d, use_txn=%d, "
       "inducer=%d\n", keysize, recsize, i, dupes, use_txn, inducer);

  ham_key_t key = {0};
  key.data = malloc(keysize);
  key.size = keysize;
  memset(key.data, 0, key.size);

  ham_record_t rec = {0};
  rec.data = malloc(recsize);
  rec.size = keysize;
  memset(rec.data, 0, rec.size);

  // if db does not yet exist: create it, otherwise open it
  st = ham_env_open(&env, "recovery.db",
          (use_txn ? HAM_ENABLE_TRANSACTIONS : 0)
          | HAM_ENABLE_RECOVERY, 0);
  if (st == HAM_FILE_NOT_FOUND) {
    ham_parameter_t params[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    st = ham_env_create(&env, "recovery.db",
              (use_txn ? HAM_ENABLE_TRANSACTIONS : 0)
              | HAM_ENABLE_RECOVERY, 0644,
            &params[0]);
    if (st) {
      printf("ham_env_create failed: %d\n", (int)st);
      exit(-1);
    }

    ham_parameter_t dbparams[] = {
      { HAM_PARAM_KEYSIZE, 100 },
      { 0, 0 }
    };
    st = ham_env_create_db(env, &db, 1,
              HAM_ENABLE_DUPLICATE_KEYS | HAM_ENABLE_EXTENDED_KEYS,
              &dbparams[0]);
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
  if (use_txn) {
    st = ham_txn_begin(&txn, env, 0, 0, 0);
    if (st) {
      printf("ham_txn_begin failed: %d\n", (int)st);
      exit(-1);
    }
  }

  ErrorInducer *ei = new ErrorInducer();
  ((LocalEnvironment *)env)->get_changeset().m_inducer = ei;
  ei->add(ErrorInducer::kChangesetFlush, inducer);

  for (int j = 0; j < NUM_STEPS; j++) {
    // modify key at end of buffer to make sure that extended keys
    // are fully loaded
    char *p = (char *)key.data;
    if (dupes)
      *(int *)&p[key.size - sizeof(int)] = i * NUM_STEPS;
    else
      *(int *)&p[key.size - sizeof(int)] = (i * NUM_STEPS)+j;
    st = ham_db_insert(db, txn, &key, &rec, dupes ? HAM_DUPLICATE : 0);
    if (st) {
      if (st == HAM_INTERNAL_ERROR && !use_txn)
        break;
      printf("ham_db_insert failed: %d (%s)\n", (int)st, ham_strerror(st));
      exit(-1);
    }
    // only loop once if transactions are disabled
    if (!use_txn)
      break;
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

  int keysize = (int)strtol(argv[2], 0, 0);
  int i     = (int)strtol(argv[3], 0, 0);
  int dupes   = (int)strtol(argv[4], 0, 0);
  int use_txn = (int)strtol(argv[5], 0, 0);
  int inducer = (int)strtol(argv[6], 0, 0);
  printf("erase: keysize=%d, i=%d, dupes=%d, use_txn=%d, inducer=%d\n",
      keysize, i, dupes, use_txn, inducer);

  ham_key_t key = {0};
  key.data = malloc(keysize);
  key.size = keysize;
  memset(key.data, 0, key.size);

  st = ham_env_open(&env, "recovery.db",
          (use_txn ? HAM_ENABLE_TRANSACTIONS : 0)
          | HAM_ENABLE_RECOVERY, 0);
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
  if (use_txn) {
    st = ham_txn_begin(&txn, env, 0, 0, 0);
    if (st) {
      printf("ham_txn_begin failed: %d\n", (int)st);
      exit(-1);
    }
  }

  ErrorInducer *ei = new ErrorInducer();
  ((LocalEnvironment *)env)->get_changeset().m_inducer = ei;
  ei->add(ErrorInducer::kChangesetFlush, inducer);

  for (int j = 0; j < NUM_STEPS; j++) {
    // modify key at end of buffer to make sure that extended keys
    // are fully loaded
    char *p = (char *)key.data;
    if (dupes) {
      *(int *)&p[key.size - sizeof(int)] = i * NUM_STEPS;
      st = ham_db_erase(db, txn, &key, 0);
      if (st) {
        if (st == HAM_INTERNAL_ERROR && !use_txn)
          break;
        printf("ham_db_erase failed: %d (%s)\n", (int)st, ham_strerror(st));
        exit(-1);
      }
      break;
    }
    else {
      *(int *)&p[key.size - sizeof(int)] = (i * NUM_STEPS) + j;
      st = ham_db_erase(db, txn, &key, 0);
      if (st) {
        if (st == HAM_INTERNAL_ERROR && !use_txn)
          break;
        printf("ham_db_erase failed: %d (%s)\n", (int)st, ham_strerror(st));
        exit(-1);
      }
    }
    // only loop once if transactions are disabled
    if (!use_txn)
      break;
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

  int use_txn = (int)strtol(argv[2], 0, 0);
  printf("recover: use_txn=%d\n", use_txn);

  ham_status_t st;
  ham_env_t *env;

  st = ham_env_open(&env, "recovery.db",
        (use_txn ? HAM_ENABLE_TRANSACTIONS : 0 ) | HAM_ENABLE_RECOVERY, 0);
  if (st == 0)
    exit(0);
  if (st != HAM_NEED_RECOVERY) {
    printf("ham_env_open failed: %d\n", (int)st);
    exit(-1);
  }

  st = ham_env_open(&env, "recovery.db",
        (use_txn ? HAM_ENABLE_TRANSACTIONS : 0 ) |HAM_AUTO_RECOVERY, 0);
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

  int keysize = (int)strtol(argv[2], 0, 0);
  int recsize = (int)strtol(argv[3], 0, 0);
  int i     = (int)strtol(argv[4], 0, 0);
  int dupes   = (int)strtol(argv[5], 0, 0);
  int use_txn = (int)strtol(argv[6], 0, 0);
  int exist   = (int)strtol(argv[7], 0, 0);
  printf("verify: keysize=%d, recsize=%d, i=%d, dupes=%d, use_txn=%d, "
       "exist=%d\n", keysize, recsize, i, dupes, use_txn, exist);

  ham_status_t st;
  ham_db_t *db;
  ham_env_t *env;

  ham_key_t key = {0};
  key.data = malloc(keysize);
  key.size = keysize;
  memset(key.data, 0, key.size);

  ham_record_t rec = {0};
  rec.data = malloc(recsize);
  rec.size = keysize;
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

  for (int j = 0; j < NUM_STEPS; j++) {
    // modify key at end of buffer to make sure that extended keys
    // are fully loaded
    char *p = (char *)key.data;
    if (dupes)
      *(int *)&p[key.size - sizeof(int)] = i * NUM_STEPS;
    else
      *(int *)&p[key.size - sizeof(int)] = (i * NUM_STEPS) + j;

    st = ham_db_find(db, 0, &key, &rec2, 0);
    if (exist && st != 0) {
      printf("ham_db_find failed but shouldn't: %d\n", (int)st);
      exit(-1);
    }
    if (!exist && st != HAM_KEY_NOT_FOUND) {
      printf("ham_db_find succeeded but shouldn't: %d\n", (int)st);
      exit(-1);
    }

    if (exist) {
      if (rec.size != rec2.size
          || memcmp(rec.data, rec2.data, rec2.size)) {
        printf("key mismatch\n");
        exit(-1);
      }
    }
    // only loop once if transactions are disabled
    if (!use_txn)
      break;
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

