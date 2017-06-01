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
#define ARG_OUTPUT        2


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
    ARG_OUTPUT,
    "out",
    "output",
    "the file name with the exported data (or stdout, if none is specified)",
    GETOPTS_NEED_ARGUMENT },
  { 0, 0, 0, 0, 0 } /* terminating element */
};

static void
error(const char *foo, ups_status_t st) {
  fprintf(stderr, "%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

class Exporter {
  public:
    Exporter() { } 
    virtual ~Exporter() { } 
    virtual void append_environment(ups_env_t *env) = 0;
    virtual void append_database(ups_db_t *db) = 0;
    virtual void append_item(ups_key_t *key, ups_record_t *record) = 0;
    virtual void close_environment(ups_env_t *env) { }
    virtual void close_database(ups_db_t *db) { }
};

class BinaryExporter : public Exporter {
  public:
    BinaryExporter(const char *outfilename)
      : m_db_counter(0), m_item_counter(0) {
      if (outfilename) {
        f = fopen(outfilename, "wb");
        if (!f) {
          fprintf(stderr, "File %s was not created: %s\n", outfilename,
                  strerror(errno));
          exit(-1);
        }
      }
      else
        f = stdout;

      // write a magic marker
      unsigned magic = 0x1234321;
      fwrite(&magic, sizeof(magic), 1, f);
    }

    ~BinaryExporter() {
      fprintf(stderr, "Exported %u databases with %u items.\n",
              (unsigned)m_db_counter, (unsigned)m_item_counter);
      fclose(f);
    }

    virtual void append_environment(ups_env_t *env) {
      ups_parameter_t params[] = {
        { UPS_PARAM_FLAGS, 0 },
        { UPS_PARAM_PAGESIZE, 0 },
        { UPS_PARAM_MAX_DATABASES, 0 },
        { 0, 0 },
      };
      ups_status_t st = ups_env_get_parameters(env, params);
      if (st)
        error("ups_env_get_parameters", st);

      params[0].value &= ~UPS_READ_ONLY;

      HamsterTool::Datum d;
      d.set_type(HamsterTool::Datum::ENVIRONMENT);
      HamsterTool::Environment *e = d.mutable_env();
      e->set_flags((int)params[0].value);
      e->set_page_size((int)params[1].value);
      e->set_max_databases((int)params[2].value);

      std::string s;
      if (!d.SerializeToString(&s)) {
        fprintf(stderr, "Error serializing Environment\n");
        exit(-1);
      }
      write_string(s);
    }

    virtual void append_database(ups_db_t *db) {
      m_db_counter++;

      ups_parameter_t params[] = {
        { UPS_PARAM_DATABASE_NAME, 0 },
        { UPS_PARAM_FLAGS, 0 },
        { UPS_PARAM_KEY_SIZE, 0 },
        { UPS_PARAM_KEY_TYPE, 0 },
        { UPS_PARAM_RECORD_SIZE, 0 },
        { 0, 0 },
      };
      ups_status_t st = ups_db_get_parameters(db, params);
      if (st)
        error("ups_db_get_parameters", st);

      params[1].value &= ~UPS_READ_ONLY;

      HamsterTool::Datum d;
      d.set_type(HamsterTool::Datum::DATABASE);
      HamsterTool::Database *pdb = d.mutable_db();
      pdb->set_name((uint32_t)params[0].value);
      pdb->set_flags((int)params[1].value);
      pdb->set_key_size((int)params[2].value);
      pdb->set_key_type((int)params[3].value);
      pdb->set_record_size((int)params[4].value);

      std::string s;
      if (!d.SerializeToString(&s)) {
        fprintf(stderr, "Error serializing Database\n");
        exit(-1);
      }
      write_string(s);
    }

    virtual void append_item(ups_key_t *key, ups_record_t *record) {
      m_item_counter++;

      HamsterTool::Datum d;
      d.set_type(HamsterTool::Datum::ITEM);
      HamsterTool::Item *item = d.mutable_item();
      item->set_key(key->data, key->size);
      item->set_record(record->data, record->size);

      std::string s;
      if (!d.SerializeToString(&s)) {
        fprintf(stderr, "Error serializing Item\n");
        exit(-1);
      }
      write_string(s);
    }

  private:
    virtual void write_string(const std::string &s) {
      unsigned size = s.size();
      if (sizeof(size) != fwrite(&size, 1, sizeof(size), f)) {
        fprintf(stderr, "Error writing to file: %s\n", strerror(errno));
        exit(-1);
      }
      if (size != fwrite(s.data(), 1, size, f)) {
        fprintf(stderr, "Error writing to file: %s\n", strerror(errno));
        exit(-1);
      }
    }

    FILE *f;
    size_t m_db_counter;
    size_t m_item_counter;
};

static void
export_database(ups_db_t *db, Exporter *exporter) {
  ups_cursor_t *cursor;
  ups_status_t st;
  ups_key_t key = {};
  ups_record_t rec = {};

  exporter->append_database(db);

  st = ups_cursor_create(&cursor, db, 0, 0);
  if (st)
    error("ups_cursor_create", st);

  while (1) {
    st = ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT);
    if (st != UPS_SUCCESS) {
      /* reached end of the database? */
      if (st == UPS_KEY_NOT_FOUND)
        break;
      else
        error("ups_cursor_next", st);
    }

    exporter->append_item(&key, &rec);
  }

  ups_cursor_close(cursor);

  exporter->close_database(db);
}

int
main(int argc, char **argv) {
  unsigned opt;
  const char *param, *infilename = 0, *outfilename = 0;

  uint32_t maj, min, rev;
  ups_get_version(&maj, &min, &rev);

  getopts_init(argc, argv, "ups_export");

  while ((opt = getopts(&opts[0], &param))) {
    switch (opt) {
      case ARG_OUTPUT:
        if (outfilename) {
          fprintf(stderr, "Multiple files specified. Please specify only one "
                "filename.\n");
          return (-1);
        }
        outfilename = param;
        break;
      case GETOPTS_PARAMETER:
        if (infilename) {
          fprintf(stderr, "Multiple files specified. Please specify only one "
                "filename.\n");
          return (-1);
        }
        infilename = param;
        break;
      case ARG_HELP:
        print_banner("ups_export");

        printf("usage: ups_export [--output=file] [file]\n");
        printf("usage: ups_export --help\n");
        printf("       --help:       this help screen\n");
        printf("       --output:     filename of exported file (stdout if empty)\n");
        return (0);
      default:
        fprintf(stderr, "Invalid or unknown parameter `%s'. "
               "Enter `ups_export --help' for usage.", param);
        return (-1);
    }
  }

  if (!infilename) {
      fprintf(stderr, "Filename is missing. Enter `ups_export --help' for "
              "usage.\n");
      return (-1);
  }

  ups_env_t *env;
  ups_db_t *db;

  Exporter *exporter = new BinaryExporter(outfilename);

  /* open the environment */
  ups_status_t st = ups_env_open(&env, infilename,
                  UPS_READ_ONLY | UPS_IGNORE_MISSING_CALLBACK, 0);
  if (st == UPS_FILE_NOT_FOUND) {
    fprintf(stderr, "File `%s' not found or unable to open it\n", infilename);
    return (-1);
  }
  else if (st != UPS_SUCCESS)
    error("ups_env_open", st);

  exporter->append_environment(env);

  /* get a list of all databases */
  uint16_t names[1024];
  uint32_t names_count = 1024;
  st = ups_env_get_database_names(env, names, &names_count);
  if (st != UPS_SUCCESS)
    error("ups_env_get_database_names", st);

  /* for each database: print information about the database */
  for (uint32_t i = 0; i < names_count; i++) {
    st = ups_env_open_db(env, &db, names[i], 0, 0);
    if (st)
      error("ups_env_open_db", st);

    export_database(db, exporter);

    st = ups_db_close(db, 0);
    if (st)
      error("ups_db_close", st);
  }

  exporter->close_environment(env);
  delete exporter;

  st = ups_env_close(env, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  return (0);
}
