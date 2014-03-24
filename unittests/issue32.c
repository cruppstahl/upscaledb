#include <assert.h>
#include <ham/hamsterdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

ham_status_t st;
#define CHECK(status)       \
  while ((st = (status))) { \
    printf("Error in line %d: %s\n", __LINE__, ham_strerror(st)); \
    exit(-2); \
  }

static void insert ()
{
  ham_env_t *db_env = NULL;
  ham_db_t *db = NULL;
  ham_txn_t *txn = NULL;

  ham_key_t db_key1;
  ham_key_t db_key2;

  ham_record_t db_data1;
  ham_record_t db_data2;

  CHECK(
    ham_env_create (&db_env, "test.db", HAM_ENABLE_TRANSACTIONS, 0644, NULL));
  CHECK(ham_env_create_db (db_env, &db, 1, 0, NULL));
  CHECK(ham_txn_begin (&txn, db_env, NULL, NULL, 0));

  memset (&db_key1, 0, sizeof (ham_key_t));
  memset (&db_data1, 0, sizeof (ham_record_t));

  memset (&db_key2, 0, sizeof (ham_key_t));
  memset (&db_data2, 0, sizeof (ham_record_t));

  db_key1.data = "Hello, world 1!";
  db_key1.size = 16;

  db_data1.data = strdup ("Goodbye, world 1.");
  db_data1.size = 18;

  CHECK(ham_db_insert (db, txn, &db_key1, &db_data1, HAM_OVERWRITE));

  db_key2.data = "Hello, world 2!";
  db_key2.size = 16;

  db_data2.data = strdup ("Goodbye, world 2.");
  db_data2.size = 18;

  CHECK(ham_db_insert (db, txn, &db_key2, &db_data2, HAM_OVERWRITE));
  CHECK(ham_txn_commit (txn, 0));

  exit (0);
}

static void recover ()
{
  ham_env_t *db_env = NULL;
  ham_db_t *db = NULL;
  int ret = ham_env_open 
    (&db_env, "test.db", HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, NULL);

  if (ret != HAM_SUCCESS)
    {
      printf ("recovery failed: %s\n", ham_strerror (ret));
      exit (1);
    }
  CHECK(ham_env_open_db (db_env, &db, 1, 0, NULL));

  ham_key_t db_key1;
  ham_key_t db_key2;
  memset (&db_key1, 0, sizeof (ham_key_t));
  memset (&db_key2, 0, sizeof (ham_key_t));

  db_key1.data = "Hello, world 1!";
  db_key1.size = 16;
  db_key2.data = "Hello, world 2!";
  db_key2.size = 16;

  ham_record_t rec;
  
  memset(&rec, 0, sizeof(rec));
  CHECK(ham_db_find (db, 0, &db_key1, &rec, 0));
  CHECK(strcmp((char *)rec.data, "Goodbye, world 1."));

  memset(&rec, 0, sizeof(rec));
  CHECK(ham_db_find (db, 0, &db_key2, &rec, 0));
  CHECK(strcmp((char *)rec.data, "Goodbye, world 2."));

  exit (0);
}

int main (int argc, char *argv[])
{
  if (argc == 2)
    {
      if (strcmp (argv[1], "-i") == 0)
        insert ();
      else if (strcmp (argv[1], "-r") == 0)
        recover ();
    }

  printf
    ("Usage: %s -i -> insert and die; %s -r -> recover\n", argv[0], argv[0]);
  return 1;
}
