/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <ups/upscaledb.h>

#include "getopts.h"
#include "common.h"
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
error(const char *foo, ups_status_t st) {
  fprintf(stderr, "%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

class Importer {
  public:
    Importer(FILE *f, ups_env_t *env, const char *outfilename)
      : m_f(f), m_env(env), m_outfilename(outfilename) {
    }

    virtual ~Importer() { }

    virtual void run() = 0;

  protected:
    FILE *m_f;
    ups_env_t *m_env;
    const char *m_outfilename;
};

class BinaryImporter : public Importer {
  public:
    BinaryImporter(FILE *f, ups_env_t *env, const char *outfilename)
      : Importer(f, env, outfilename), m_db(0), m_insert_flags(0),
        m_db_counter(0), m_item_counter(0) {
      m_buffer = (char *)malloc(1024 * 1024);
    }

    ~BinaryImporter() {
      free(m_buffer);
      if (m_env)
        ups_env_close(m_env, UPS_AUTO_CLEANUP);
      printf("Imported %u databases with %u items.\n",
              (unsigned)m_db_counter, (unsigned)m_item_counter);
    }

    virtual void run() {
      while (!feof(m_f)) {
        // read the next message from the stream
        uint32_t size = read_size();
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
      ups_parameter_t params[] = {
        { UPS_PARAM_PAGESIZE, e.page_size() },
        { 0, 0 },
      };

      ups_status_t st = ups_env_create(&m_env, m_outfilename, e.flags(),
                            0644, &params[0]);
      if (st)
        error("ups_env_create", st);
    }

    void read_database(HamsterTool::Datum &datum) {
      const HamsterTool::Database &db = datum.db();

      // create database (if it does not yet exist)
      ups_parameter_t params[] = {
        { UPS_PARAM_KEY_SIZE, db.key_size() },
        { UPS_PARAM_KEY_TYPE,
                db.has_key_type()
                    ? db.key_type()
                    : UPS_TYPE_BINARY },
        { UPS_PARAM_RECORD_SIZE,
                db.has_record_size()
                    ? db.record_size()
                    : UPS_RECORD_SIZE_UNLIMITED },
        { 0, 0 },
      };

      if (m_db) {
        ups_db_close(m_db, 0);
        m_db = 0;
      }

      if (db.flags() & UPS_ENABLE_DUPLICATE_KEYS)
        m_insert_flags |= UPS_DUPLICATE;
      else {
        m_insert_flags &= ~UPS_DUPLICATE;
        m_insert_flags |= UPS_OVERWRITE;
      }

      uint32_t open_flags = db.flags();
      open_flags &= ~UPS_ENABLE_DUPLICATE_KEYS;
      open_flags &= ~UPS_IGNORE_MISSING_CALLBACK;

      ups_status_t st = ups_env_open_db(m_env, &m_db, db.name(), open_flags, 0);
      if (st == 0)
        return;
      if (st != UPS_DATABASE_NOT_FOUND)
        error("ups_env_open_db", st);

      st = ups_env_create_db(m_env, &m_db, db.name(), db.flags(), &params[0]);
      if (st)
        error("ups_env_create_db", st);
    }

    void read_item(HamsterTool::Datum &datum) {
      const HamsterTool::Item &item = datum.item();

      const std::string &skey(item.key());
      const std::string &srec(item.record());

      ups_key_t k = {};
      k.data = (void *)skey.data();
      k.size = skey.size();
      ups_record_t r = {};
      r.data = (void *)srec.data();
      r.size = srec.size();

      ups_status_t st = ups_db_insert(m_db, 0, &k, &r, m_insert_flags);
      if (st)
        error("ups_db_insert", st);
    }

    uint32_t read_size() {
      int n;
      uint32_t size;
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
    ups_db_t *m_db;
    uint32_t m_insert_flags;
    size_t m_db_counter;
    size_t m_item_counter;
};

int
main(int argc, char **argv) {
  unsigned opt;
  const char *param, *dumpfilename = 0, *envfilename = 0;
  bool merge = false;
  bool use_stdin = false;

  getopts_init(argc, argv, "ups_import");

  while ((opt = getopts(&opts[0], &param))) {
    switch (opt) {
      case ARG_STDIN:
        use_stdin = true;
        break;
      case ARG_MERGE:
        merge = true;
        break;
      case GETOPTS_PARAMETER:
        if (!dumpfilename && !use_stdin)
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
        print_banner("ups_import");

        printf("usage: ups_import [--stdin] [--merge] <data> <environ>\n");
        printf("usage: ups_import --help\n");
        printf("       --help:       this help screen\n");
        printf("       --stdin:      read dump data from stdin\n");
        printf("       --merge:      merge data into existing environment\n");
        printf("       <data>:       filename with exported data\n");
        printf("       <environ>:    upscaledb environment which will be created (or filled)\n");
        return (0);
      default:
        fprintf(stderr, "Invalid or unknown parameter `%s'. "
               "Enter `ups_import --help' for usage.", param);
        return (-1);
    }
  }

  if (!dumpfilename && !use_stdin) {
      fprintf(stderr, "Data filename is missing. "
            "Enter `ups_import --help' for usage.\n");
      return (-1);
  }
  if (!envfilename) {
      fprintf(stderr, "Environment filename is missing. "
            "Enter `ups_import --help' for usage.\n");
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
    fclose(f);
    return (-1);
  }
  if (magic != 0x1234321) {
    fprintf(stderr, "Unknown binary format\n");
    fclose(f);
    return (-1);
  }

  // if the file already exists then fail unless merge == true
  ups_env_t *env = 0;
  ups_status_t st = ups_env_open(&env, envfilename, 0, 0);
  if (st == 0) {
    if (merge == false) {
      fprintf(stderr, "File %s already exists, aborting...\n", envfilename);
      fclose(f);
      return (-1);
    }
  }
  else if (st != UPS_FILE_NOT_FOUND) {
    fprintf(stderr, "Error opening %s: %s\n", envfilename, ups_strerror(st));
    fclose(f);
    return (-1);
  }

  // now run the import; the importer will create the environment
  Importer *importer = new BinaryImporter(f, env, envfilename);
  importer->run();
  delete importer;
  fclose(f);

  return (0);
}
