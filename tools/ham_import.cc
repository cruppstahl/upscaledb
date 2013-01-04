/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <ham/hamsterdb.h>

#include "getopts.h"
#include "export.pb.h"

#define ARG_HELP          1
#define ARG_STDIN         2
#define ARG_MERGE         3


/*
 * command line parameters
 */
static option_t opts[] = {
  {
    ARG_HELP,               // symbolic name of this option
    "h",                    // short option
    "help",                 // long option
    "this help screen",     // help string
    0 },                    // no flags
  {
    ARG_STDIN,
    "stdin",
    "stdin",
    "read database dump from stdin",
    0 },
  {
    ARG_MERGE,
    "merge",
    "merge",
    "merge database dump into existing file",
    0 },
  { 0, 0, 0, 0, 0 } /* terminating element */
};

static void
error(const char *foo, ham_status_t st) {
  fprintf(stderr, "%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

class Importer {
  public:
    Importer(FILE *f, ham_env_t *env, const char *outfilename)
      : m_f(f), m_env(env), m_outfilename(outfilename) {
    }

    virtual ~Importer() { }

    virtual void run() = 0;

  protected:
    FILE *m_f;
    ham_env_t *m_env;
    const char *m_outfilename;
};

class BinaryImporter : public Importer {
  public:
    BinaryImporter(FILE *f, ham_env_t *env, const char *outfilename)
      : Importer(f, env, outfilename), m_db(0), m_insert_flags(0),
        m_db_counter(0), m_item_counter(0) {
      m_buffer = (char *)malloc(1024 * 1024);
    }

    ~BinaryImporter() {
      free(m_buffer);
      if (m_env)
        ham_env_close(m_env, HAM_AUTO_CLEANUP);
      printf("Imported %u databases with %u items.\n",
              (unsigned)m_db_counter, (unsigned)m_item_counter);
    }

    virtual void run() {
      while (!feof(m_f)) {
        // read the next message from the stream
        ham_u32_t size = read_size();
        if (!size)
          return;

        m_buffer = (char *)realloc(0, size);
        if (size != fread(m_buffer, 1, size, m_f)) {
          fprintf(stderr, "Error reading %u bytes: %s\n", size,
                  strerror(errno));
          exit(-1);
        }

        // unpack serialized datum
        HamsterTool::Datum datum;
        datum.ParseFromArray(m_buffer, size);

        switch (datum.type()) {
          case HamsterTool::Datum::ENVIRONMENT:
            read_environment(datum);
            break;
          case HamsterTool::Datum::DATABASE:
            read_database(datum);
            m_db_counter++;
            break;
          case HamsterTool::Datum::ITEM:
            read_item(datum);
            m_item_counter++;
            break;
          default:
            fprintf(stderr, "Unknown message type\n");
            exit(-1);
        }
      }
    }

  private:
    void read_environment(HamsterTool::Datum &datum) {
      // only process if the Environment does not yet exist
      if (m_env)
        return;

      const HamsterTool::Environment &e = datum.env();

      // create environment (if it does not yet exist)
      ham_parameter_t params[] = {
        { HAM_PARAM_PAGESIZE, e.pagesize() },
        { HAM_PARAM_MAX_DATABASES, e.max_databases() },
        { 0, 0 },
      };

      ham_status_t st = ham_env_create(&m_env, m_outfilename, e.flags(),
                            0644, &params[0]);
      if (st)
        error("ham_env_create", st);
    }

    void read_database(HamsterTool::Datum &datum) {
      const HamsterTool::Database &db = datum.db();

      // create database (if it does not yet exist)
      ham_parameter_t params[] = {
        { HAM_PARAM_KEYSIZE, db.keysize() },
        { 0, 0 },
      };

      if (m_db) {
        ham_db_close(m_db, 0);
        m_db = 0;
      }

      if (db.flags() & HAM_ENABLE_DUPLICATES)
        m_insert_flags |= HAM_DUPLICATE;
      else
        m_insert_flags &= ~HAM_DUPLICATE;

      ham_status_t st = ham_env_open_db(m_env, &m_db, db.name(),
                            db.flags() & ~HAM_ENABLE_DUPLICATES, 0);
      if (st == 0)
        return;
      if (st != HAM_DATABASE_NOT_FOUND)
        error("ham_env_open_db", st);

      st = ham_env_create_db(m_env, &m_db, db.name(), db.flags(), &params[0]);
      if (st)
        error("ham_env_create_db", st);
    }

    void read_item(HamsterTool::Datum &datum) {
      const HamsterTool::Item &item = datum.item();

      const std::string &skey(item.key());
      const std::string &srec(item.record());

      ham_key_t k = {};
      k.data = (void *)skey.data();
      k.size = skey.size();
      ham_record_t r = {};
      r.data = (void *)srec.data();
      r.size = srec.size();

      ham_status_t st = ham_db_insert(m_db, 0, &k, &r, m_insert_flags);
      if (st)
        error("ham_db_insert", st);
    }

    ham_u32_t read_size() {
      int n;
      ham_u32_t size;
      n = fread(&size, 1, sizeof(size), m_f);
      if (n == 0)
        return (0);
      if (n < 0 || n != sizeof(size)) {
        fprintf(stderr, "Error reading %u bytes: %s\n", (unsigned)sizeof(size),
                strerror(errno));
        exit(-1);
      }
      return (size);
    }

    char *m_buffer;
    ham_db_t *m_db;
    ham_u32_t m_insert_flags;
    size_t m_db_counter;
    size_t m_item_counter;
};

int
main(int argc, char **argv) {
  unsigned opt;
  char *param, *dumpfilename = 0, *envfilename = 0;
  bool merge = false;
  bool use_stdin = false;

  ham_u32_t maj, min, rev;
  const char *licensee, *product;
  ham_get_license(&licensee, &product);
  ham_get_version(&maj, &min, &rev);

  getopts_init(argc, argv, "ham_import");

  while ((opt = getopts(&opts[0], &param))) {
    switch (opt) {
      case ARG_STDIN:
        use_stdin = true;
        break;
      case ARG_MERGE:
        merge = true;
        break;
      case GETOPTS_PARAMETER:
        if (!dumpfilename)
          dumpfilename = param;
        else {
          if (envfilename) {
            fprintf(stderr, "Multiple files specified. Please specify max. two "
                  "filenames.\n");
            return (-1);
          }
          envfilename = param;
        }
        break;
      case ARG_HELP:
        printf("hamsterdb %d.%d.%d - Copyright (C) 2005-2013 "
              "Christoph Rupp (chris@crupp.de).\n\n", maj, min, rev);

        if (licensee[0] == '\0')
          printf("This program is free software; you can redistribute "
                 "it and/or modify it\nunder the terms of the GNU "
                 "General Public License as published by the Free\n"
                 "Software Foundation; either version 2 of the License,\n"
                 "or (at your option) any later version.\n\n"
                 "See file COPYING.GPL2 and COPYING.GPL3 for License "
                 "information.\n\n");
        else
          printf("Commercial version; licensed for %s (%s)\n\n",
                 licensee, product);

        printf("usage: ham_import [--stdin] [--merge] <data> <environ>\n");
        printf("usage: ham_import --help\n");
        printf("       --help:       this help screen\n");
        printf("       --stdin:      read dump data from stdin\n");
        printf("       --merge:      merge data into existing environment\n");
        printf("       <data>:       filename with exported data\n");
        printf("       <environ>:    hamsterdb environment which will be created (or filled)\n");
        return (0);
      default:
        fprintf(stderr, "Invalid or unknown parameter `%s'. "
               "Enter `ham_import --help' for usage.", param);
        return (-1);
    }
  }

  if (!dumpfilename && !use_stdin) {
      fprintf(stderr, "Data filename is missing. "
            "Enter `ham_import --help' for usage.\n");
      return (-1);
  }
  if (!envfilename) {
      fprintf(stderr, "Environment filename is missing. "
            "Enter `ham_import --help' for usage.\n");
      return (-1);
  }

  // open the file with the exported data
  FILE *f = stdin;
  if (dumpfilename) {
    f = fopen(dumpfilename, "rb");
    if (!f) {
      fprintf(stderr, "Cannot open %s: %s\n", dumpfilename, strerror(errno));
      return (-1);
    }
  }

  // read the first 4 bytes; if it's the magic: create a binary importer;
  // otherwise fail
  unsigned magic;
  if (sizeof(unsigned) < fread(&magic, 1, sizeof(unsigned), f)) {
    fprintf(stderr, "Cannot read input file: %s\n", strerror(errno));
    return (-1);
  }
  if (magic != 0x1234321) {
    fprintf(stderr, "Unknown binary format\n");
    return (-1);
  }

  // if the file already exists then fail unless merge == true
  ham_env_t *env = 0;
  ham_status_t st = ham_env_open(&env, envfilename, 0, 0);
  if (st == 0) {
    if (merge == false) {
      fprintf(stderr, "File %s already exists, aborting...\n", envfilename);
      return (-1);
    }
  }
  else if (st != HAM_FILE_NOT_FOUND) {
    fprintf(stderr, "Error opening %s: %s\n", envfilename, ham_strerror(st));
    return (-1);
  }

  // now run the import; the importer will create the environment
  Importer *importer = new BinaryImporter(f, env, envfilename);
  importer->run();
  delete importer;

  return (0);
}
