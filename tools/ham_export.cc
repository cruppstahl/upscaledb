/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "base64.h"
#include "export.pb.h"

#define ARG_HELP          1
#define ARG_ENCODING      2
#define ARG_OUTPUT        3


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
    ARG_ENCODING,
    "enc",
    "encoding",
    "the encoding of the exported data; either json or binary (default)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_OUTPUT,
    "out",
    "output",
    "the file name with the exported data (or stdout, if none is specified)",
    GETOPTS_NEED_ARGUMENT },
  { 0, 0, 0, 0, 0 } /* terminating element */
};

static void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

class Exporter {
  public:
    Exporter() { } 
    virtual ~Exporter() { } 
    virtual void append_environment(ham_env_t *env) = 0;
    virtual void append_database(ham_db_t *db) = 0;
    virtual void append_item(ham_key_t *key, ham_record_t *record) = 0;
    virtual void close_environment(ham_env_t *env) { }
    virtual void close_database(ham_db_t *db) { }
};

class JsonExporter : public Exporter {
  public:
    JsonExporter(const char *outfilename) {
      if (outfilename) {
        f = fopen(outfilename, "wb");
        if (!f) {
          printf("File %s was not created: %s\n", outfilename, strerror(errno));
          exit(-1);
        }
      }
      else
        f = stdout;
    }

    ~JsonExporter() {
      fclose(f);
    }

    virtual void append_environment(ham_env_t *env) {
      ham_parameter_t params[] = {
        { HAM_PARAM_FLAGS, 0 },
        { HAM_PARAM_PAGESIZE, 0 },
        { HAM_PARAM_MAX_DATABASES, 0 },
        { 0, 0 },
      };
      ham_status_t st = ham_env_get_parameters(env, params);
      if (st)
        error("ham_env_get_parameters", st);
      fprintf(f, "{\n\t\"flags\": %u,\n"
            "\t\"pagesize\": %u,\n"
            "\t\"max_databases\": %u,\n"
            "\t\"databases\": [\n",
            (unsigned)params[0].value,
            (unsigned)params[1].value,
            (unsigned)params[2].value);
    }

    virtual void close_environment(ham_env_t *env) {
      fprintf(f, "\t]\n}\n");
    }

    virtual void append_database(ham_db_t *db) {
      ham_parameter_t params[] = {
        { HAM_PARAM_DATABASE_NAME, 0 },
        { HAM_PARAM_FLAGS, 0 },
        { HAM_PARAM_KEYSIZE, 0 },
        { 0, 0 },
      };
      ham_status_t st = ham_db_get_parameters(db, params);
      if (st)
        error("ham_db_get_parameters", st);
      fprintf(f, "\t{\n\t\t\"name\": %u,\n"
            "\t\t\"flags\": %u,\n"
            "\t\t\"keysize\": %u,\n"
            "\t\t\"items\": [\n",
            (unsigned)params[0].value,
            (unsigned)params[1].value,
            (unsigned)params[2].value);
    }

    virtual void close_database(ham_db_t *db) {
      fprintf(f, "\t\t]\n\t},\n");
    }

    virtual void append_item(ham_key_t *key, ham_record_t *rec) {
      size_t enckey_length;
      size_t encrec_length;
      char *enckey = m_base64.encode((unsigned char *)key->data, key->size,
                                    &enckey_length);
      char *encrec = m_base64.encode((unsigned char *)rec->data, rec->size,
                                    &encrec_length);
      fprintf(f, "\t\t\t{ \"key\": \"%s\",\n"
            "\t\t\t  \"record\": \"%s\" },\n",
            enckey, encrec);
      free(enckey);
      free(encrec);
    }

  private:
    FILE *f;
    Base64Encoder m_base64;
};

class BinaryExporter : public Exporter {
  public:
    BinaryExporter(const char *outfilename) {
      if (outfilename) {
        f = fopen(outfilename, "wb");
        if (!f) {
          printf("File %s was not created: %s\n", outfilename, strerror(errno));
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
      fclose(f);
    }

    virtual void append_environment(ham_env_t *env) {
      ham_parameter_t params[] = {
        { HAM_PARAM_FLAGS, 0 },
        { HAM_PARAM_PAGESIZE, 0 },
        { HAM_PARAM_MAX_DATABASES, 0 },
        { 0, 0 },
      };
      ham_status_t st = ham_env_get_parameters(env, params);
      if (st)
        error("ham_env_get_parameters", st);

      HamsterTool::Environment e;
      e.set_flags(params[0].value);
      e.set_pagesize(params[1].value);
      e.set_max_databases(params[2].value);
      std::string s;
      if (!e.SerializeToString(&s)) {
        printf("Error serializing Environment\n");
        exit(-1);
      }
      if (s.size() != fwrite(s.data(), 1, s.size(), f)) {
        printf("Error writing to file: %s\n", strerror(errno));
        exit(-1);
      }
    }

    virtual void append_database(ham_db_t *db) {
      ham_parameter_t params[] = {
        { HAM_PARAM_DATABASE_NAME, 0 },
        { HAM_PARAM_FLAGS, 0 },
        { HAM_PARAM_KEYSIZE, 0 },
        { 0, 0 },
      };
      ham_status_t st = ham_db_get_parameters(db, params);
      if (st)
        error("ham_db_get_parameters", st);

      HamsterTool::Database d;
      d.set_name(params[0].value);
      d.set_flags(params[1].value);
      d.set_keysize(params[2].value);
      std::string s;
      if (!d.SerializeToString(&s)) {
        printf("Error serializing Database\n");
        exit(-1);
      }
      if (s.size() != fwrite(s.data(), 1, s.size(), f)) {
        printf("Error writing to file: %s\n", strerror(errno));
        exit(-1);
      }
    }

    virtual void append_item(ham_key_t *key, ham_record_t *record) {
      HamsterTool::Item item;
      item.set_key(key->data, key->size);
      item.set_record(record->data, record->size);
      std::string s;
      if (!item.SerializeToString(&s)) {
        printf("Error serializing Item\n");
        exit(-1);
      }
      if (s.size() != fwrite(s.data(), 1, s.size(), f)) {
        printf("Error writing to file: %s\n", strerror(errno));
        exit(-1);
      }
    }

  private:
    FILE *f;
};

static void
export_database(ham_db_t *db, Exporter *exporter) {
  ham_cursor_t *cursor;
  ham_status_t st;
  ham_key_t key = {};
  ham_record_t rec = {};

  exporter->append_database(db);

  st = ham_cursor_create(&cursor, db, 0, 0);
  if (st)
    error("ham_cursor_create", st);

  while (1) {
    st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT);
    if (st != HAM_SUCCESS) {
      /* reached end of the database? */
      if (st == HAM_KEY_NOT_FOUND)
        break;
      else
        error("ham_cursor_next", st);
    }

    exporter->append_item(&key, &rec);
  }

  ham_cursor_close(cursor);

  exporter->close_database(db);
}

int
main(int argc, char **argv) {
  unsigned opt;
  char *param, *infilename = 0, *outfilename = 0;
  bool use_json = false;

  ham_u32_t maj, min, rev;
  const char *licensee, *product;
  ham_get_license(&licensee, &product);
  ham_get_version(&maj, &min, &rev);

  getopts_init(argc, argv, "ham_export");

  while ((opt = getopts(&opts[0], &param))) {
    switch (opt) {
      case ARG_OUTPUT:
        if (outfilename) {
          printf("Multiple files specified. Please specify only one "
                "filename.\n");
          return (-1);
        }
        outfilename = param;
        break;
      case ARG_ENCODING:
        if (!strcmp(param, "json"))
          use_json = true;
        else if (strcmp(param, "binary")) {
          printf("Unknown encoding; supported values: 'binary', 'json'\n");
          return (-1);
        }
        break;
      case GETOPTS_PARAMETER:
        if (infilename) {
          printf("Multiple files specified. Please specify only one "
                "filename.\n");
          return (-1);
        }
        infilename = param;
        break;
      case ARG_HELP:
        printf("hamsterdb %d.%d.%d - Copyright (C) 2005-2012 "
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

        printf("usage: ham_export [--output=file] [--encoding=binary|json] [file]\n");
        printf("usage: ham_export --help\n");
        printf("       --help:       this help screen\n");
        printf("       --output:     filename of exported file (stdout if empty)\n");
        printf("       --encoding:   encoding of exported data (binary is default)\n");
        return (0);
      default:
        printf("Invalid or unknown parameter `%s'. "
               "Enter `ham_export --help' for usage.", param);
        return (-1);
    }
  }

  if (!infilename) {
      printf("Filename is missing. Enter `ham_export --help' for usage.\n");
      return (-1);
  }

  ham_env_t *env;
  ham_db_t *db;

  Exporter *exporter;
  if (use_json)
    exporter = new JsonExporter(outfilename);
  else
    exporter = new BinaryExporter(outfilename);

  /* open the environment */
  ham_status_t st = ham_env_open(&env, infilename, HAM_READ_ONLY, 0);
  if (st == HAM_FILE_NOT_FOUND) {
    printf("File `%s' not found or unable to open it\n", infilename);
    return (-1);
  }
  else if (st != HAM_SUCCESS)
    error("ham_env_open", st);

  exporter->append_environment(env);

  /* get a list of all databases */
  ham_u16_t names[1024];
  ham_size_t names_count = 1024;
  st = ham_env_get_database_names(env, names, &names_count);
  if (st != HAM_SUCCESS)
    error("ham_env_get_database_names", st);

  /* for each database: print information about the database */
  for (ham_size_t i = 0; i < names_count; i++) {
    st = ham_env_open_db(env, &db, names[i], 0, 0);
    if (st)
      error("ham_env_open_db", st);

    export_database(db, exporter);

    st = ham_db_close(db, 0);
    if (st)
      error("ham_db_close", st);
  }

  exporter->close_environment(env);

  st = ham_env_close(env, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  return (0);
}
