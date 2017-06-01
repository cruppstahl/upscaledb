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

#include <cassert>

#include "misc.h"
#include "configuration.h"
#include "generator_parser.h"

ParserGenerator::ParserGenerator(int id, Configuration *conf, Database *db,
                bool show_progress)
  : Generator(id, conf, db), m_txn(0), m_cursor(0), m_progress(0),
    m_success(true), m_cur_line(0), m_data_size(0), m_data_ptr(0)
{
  memset(&m_metrics, 0, sizeof(m_metrics));
  m_metrics.insert_latency_min = 9999999.99;
  m_metrics.erase_latency_min = 9999999.99;
  m_metrics.find_latency_min = 9999999.99;
  m_metrics.txn_commit_latency_min = 9999999.99;

  read_file();

  if (show_progress) {
    if (!conf->no_progress && !conf->quiet && !conf->verbose)
      m_progress = new boost::progress_display(get_line_count());
  }
}

bool
ParserGenerator::execute()
{
  if (m_cur_line >= m_lines.size())
    return (false);

  const char *flags = 0;
  const char *keydata = 0;
  const char *recdata = 0;
  int cmd = get_next_command(&flags, &keydata, &recdata);

  switch (cmd) {
    case Generator::kCommandCreate:
      create();
      break;
    case Generator::kCommandOpen:
      open();
      break;
    case Generator::kCommandClose:
      close();
      break;
    case Generator::kCommandInsert:
      insert(keydata, recdata, flags);
      break;
    case Generator::kCommandErase:
      erase(keydata);
      break;
    case Generator::kCommandFind:
      find(keydata);
      break;
    case Generator::kCommandTablescan:
      tablescan();
      break;
    case Generator::kCommandBeginTxn:
      txn_begin();
      break;
    case Generator::kCommandAbortTxn:
      txn_abort();
      break;
    case Generator::kCommandCommitTxn:
      txn_commit();
      break;
    case Generator::kCommandFullcheck:
      m_last_status = Generator::kCommandFullcheck;
      break;
    case Generator::kCommandFlush:
      flush();
      break;
    case Generator::kCommandNop:
      break;
    default:
      assert(!"shouldn't be here");
  }

  m_cur_line++;

  if (m_progress)
    (*m_progress) += 1;

  return (true);
}

void
ParserGenerator::create()
{
  m_db->create_env();
  m_last_status = m_db->create_db(m_id);
  
  if (m_config->use_cursors)
    m_cursor = m_db->cursor_create();

  if (m_last_status != 0)
    m_success = false;

  m_metrics.other_ops++;
  m_is_active = true;
}

void
ParserGenerator::open()
{
  m_db->open_env();
  m_last_status = m_db->open_db(m_id);

  if (m_config->use_cursors)
    m_cursor = m_db->cursor_create();

  if (m_last_status != 0)
    m_success = false;

  m_metrics.other_ops++;
  m_is_active = true;
}
 
void
ParserGenerator::close()
{
  if (m_cursor) {
    m_db->cursor_close(m_cursor);
    m_cursor = 0;
  }

  m_last_status = m_db->close_db();
  if (m_last_status != 0)
    m_success = false;

  m_db->close_env();

  m_metrics.other_ops++;
  m_metrics.elapsed_wallclock_seconds = m_start.seconds();

  m_is_active = false;
}

void
ParserGenerator::insert(const char *keydata, const char *recdata,
                const char *flags)
{
  char buffer[64];
  ups_key_t key = generate_key(keydata, buffer);
  ups_record_t rec = generate_record(recdata);

  Timer<boost::chrono::high_resolution_clock> t;

  if (m_cursor)
    m_last_status = m_db->cursor_insert(m_cursor, &key, &rec);
  else
    m_last_status = m_db->insert(m_txn, &key, &rec);

  double elapsed = t.seconds();
  if (m_metrics.insert_latency_min > elapsed)
    m_metrics.insert_latency_min = elapsed;
  if (m_metrics.insert_latency_max < elapsed)
    m_metrics.insert_latency_max = elapsed;
  m_metrics.insert_latency_total += elapsed;

  if (m_last_status != 0 && m_last_status != UPS_DUPLICATE_KEY)
    m_success = false;

  if (m_last_status == 0) {
    m_metrics.insert_bytes += key.size + rec.size;
    if (m_progress && m_config->limit_bytes)
      (*m_progress) += key.size + rec.size;
  }

  m_metrics.insert_ops++;
}

void
ParserGenerator::erase(const char *keydata)
{
  char buffer[64];
  ups_key_t key = generate_key(keydata, buffer);

  Timer<boost::chrono::high_resolution_clock> t;

  if (m_cursor)
    m_last_status = m_db->cursor_erase(m_cursor, &key);
  else
    m_last_status = m_db->erase(m_txn, &key);

  double elapsed = t.seconds();
  if (m_metrics.erase_latency_min > elapsed)
    m_metrics.erase_latency_min = elapsed;
  if (m_metrics.erase_latency_max < elapsed)
    m_metrics.erase_latency_max = elapsed;
  m_metrics.erase_latency_total += elapsed;

  if (m_last_status != 0 && m_last_status != UPS_KEY_NOT_FOUND)
    m_success = false;

  m_metrics.erase_ops++;
}

void
ParserGenerator::find(const char *keydata)
{
  char buffer[64];
  ups_key_t key = generate_key(keydata, buffer);
  ups_record_t m_record = {0};
  memset(&m_record, 0, sizeof(m_record));

  Timer<boost::chrono::high_resolution_clock> t;

  if (m_cursor)
    m_last_status = m_db->cursor_find(m_cursor, &key, &m_record);
  else
    m_last_status = m_db->find(m_txn, &key, &m_record);

  double elapsed = t.seconds();
  if (m_metrics.find_latency_min > elapsed)
    m_metrics.find_latency_min = elapsed;
  if (m_metrics.find_latency_max < elapsed)
    m_metrics.find_latency_max = elapsed;
  m_metrics.find_latency_total += elapsed;

  if (m_last_status != 0 && m_last_status != UPS_KEY_NOT_FOUND)
    m_success = false;

  m_metrics.find_bytes += m_record.size;
  m_metrics.find_ops++;
}

void
ParserGenerator::tablescan()
{
  Database::Cursor *cursor = m_cursor;
  if (!cursor)
    cursor = m_db->cursor_create();

  ups_key_t key = {0};
  ups_record_t rec = {0};

  while (true) {
    ups_status_t st = m_db->cursor_get_next(cursor, &key, &rec, false);
    if (st == UPS_KEY_NOT_FOUND)
      break;
    if (st != 0) {
      printf("[FAIL] Unexpected status %d in table scan\n", st);
      exit(-1);
    }
  }

  if (!m_cursor)
    m_db->cursor_close(cursor);
}

void
ParserGenerator::txn_begin()
{
  assert(m_txn == 0);

  if (m_cursor) {
    m_db->cursor_close(m_cursor);
    m_cursor = 0;
  }

  m_txn = m_db->txn_begin();

  if (m_config->use_cursors)
    m_cursor = m_db->cursor_create();

  m_metrics.other_ops++;
}

void
ParserGenerator::txn_abort()
{
  assert(m_txn != 0);

  if (m_cursor) {
    m_db->cursor_close(m_cursor);
    m_cursor = 0;
  }

  m_last_status = m_db->txn_abort(m_txn);
  m_txn = 0;

  if (m_last_status != 0)
    m_success = false;

  m_metrics.other_ops++;
}

void
ParserGenerator::txn_commit()
{
  assert(m_txn != 0);

  if (m_cursor) {
    m_db->cursor_close(m_cursor);
    m_cursor = 0;
  }

  Timer<boost::chrono::high_resolution_clock> t;

  m_last_status = m_db->txn_commit(m_txn);
  m_txn = 0;

  double elapsed = t.seconds();
  if (m_metrics.txn_commit_latency_min > elapsed)
    m_metrics.txn_commit_latency_min = elapsed;
  if (m_metrics.txn_commit_latency_max < elapsed)
    m_metrics.txn_commit_latency_max = elapsed;
  m_metrics.txn_commit_latency_total += elapsed;

  if (m_last_status != 0)
    m_success = false;

  m_metrics.txn_commit_ops++;
}

void
ParserGenerator::flush()
{
  m_last_status = m_db->flush();

  if (m_last_status != 0)
    m_success = false;
}

ups_key_t
ParserGenerator::generate_key(const char *keydata, char *buffer) const
{
  ups_key_t key = {0};
  key.data = buffer;

  switch (m_config->key_type) {
    case Configuration::kKeyBinary:
    case Configuration::kKeyString:
    case Configuration::kKeyCustom:
      key.data = (void *)keydata;
      key.size = (uint16_t)strlen(keydata);
      break;
    case Configuration::kKeyUint8:
      *(uint16_t *)buffer = (uint8_t)strtoul(keydata, 0, 0);
      key.size = 1;
      break;
    case Configuration::kKeyUint16:
      *(uint16_t *)buffer = (uint16_t)strtoul(keydata, 0, 0);
      key.size = sizeof(uint16_t);
      break;
    case Configuration::kKeyUint32:
      *(uint32_t *)buffer = (uint32_t)strtoul(keydata, 0, 0);
      key.size = sizeof(uint32_t);
      break;
    case Configuration::kKeyUint64:
      *(uint64_t *)buffer = (uint64_t)strtoul(keydata, 0, 0);
      key.size = sizeof(uint64_t);
      break;
    default:
      assert(!"shouldn't be here");
  }
  return (key);
}

ups_record_t
ParserGenerator::generate_record(const char *recdata)
{
  ups_record_t rec = {0};

  /* allocate and initialize data */
  uint32_t data_size = 0;
  switch (m_config->record_type) {
    case Configuration::kKeyUint8:
      data_size = 1;
      break;
    case Configuration::kKeyUint16:
      data_size = 2;
      break;
    case Configuration::kKeyReal32:
    case Configuration::kKeyUint32:
      data_size = 4;
      break;
    case Configuration::kKeyReal64:
    case Configuration::kKeyUint64:
      data_size = 8;
      break;
    default:
      data_size = (uint32_t)::strtoul(recdata, 0, 0);
      break;
  }

  if (data_size) {
    if (data_size > m_data_size) {
      m_data_size = data_size;
      m_data_ptr = ::realloc(m_data_ptr, data_size);
    }
    /* always start with a pseudo-random number - otherwise berkeleydb fails
     * too often when duplicate keys are inserted with duplicate
     * records */
    for (uint32_t i = 0; i < data_size; i++)
      ((char *)m_data_ptr)[i] = (m_cur_line + i) & 0xff;
    if (data_size >= sizeof(unsigned))
      *(unsigned *)m_data_ptr = m_cur_line;

    rec.data = m_data_ptr;
    rec.size = data_size;
  }

  return (rec);
}

int
ParserGenerator::get_next_command(const char **pflags, const char **pkeydata,
            const char **precdata)
{
  // create a local copy because the string will be modified
  m_tokens = tokenize(m_lines[m_cur_line]);
  if (m_tokens.empty())
    return (kCommandNop);
  if (m_tokens[0] == "CREATE") {
    LOG_VERBOSE(("%d: line %u: reading token '%s' .......................\n", 
          m_db->get_id(), m_cur_line + 1, m_tokens[0].c_str()));
    if (strstr(m_lines[m_cur_line].c_str(), "NUMERIC_KEY"))
      m_config->key_type = Configuration::kKeyUint32;
    return (kCommandCreate);
  }
  if (m_tokens[0] == "OPEN") {
    LOG_VERBOSE(("%d: line %u: reading token '%s' .......................\n", 
          m_db->get_id(), m_cur_line + 1, m_tokens[0].c_str()));
    if (strstr(m_lines[m_cur_line].c_str(), "NUMERIC_KEY"))
      m_config->key_type = Configuration::kKeyUint32;
    return (kCommandOpen);
  }
  if (m_tokens[0] == "INSERT") {
    if (m_tokens.size() == 3) {
      *pflags = m_tokens[1].c_str();
      *pkeydata = "";
      *precdata = m_tokens[2].c_str();
    }
    else if (m_tokens.size() == 4) {
      *pflags  = m_tokens[1].c_str();
      *pkeydata = m_tokens[2].c_str();
      *precdata = m_tokens[3].c_str();
    }
    else {
      LOG_ERROR(("line %d (INSERT): parser error\n", m_cur_line + 1));
      exit(-1);
    }
    LOG_VERBOSE(("%d: line %u: reading token '%s' (%s)...................\n", 
          m_db->get_id(), m_cur_line + 1, m_tokens[0].c_str(), *pkeydata));
    if (!*precdata)
      *precdata = "";
    return (kCommandInsert);
  }
  if (m_tokens[0] == "ERASE") {
    if (m_tokens.size() < 3) {
      LOG_ERROR(("line %d (ERASE): parser error\n", m_cur_line + 1));
      exit(-1);
    }
    *pflags = m_tokens[1].c_str();
    *pkeydata = m_tokens[2].c_str();
    LOG_VERBOSE(("%d: line %u: reading token '%s' (%s)...................\n", 
          m_db->get_id(), m_cur_line + 1, m_tokens[0].c_str(), *pkeydata));
    return (kCommandErase);
  }
  if (m_tokens[0] == "FIND") {
    if (m_tokens.size() != 3) {
      LOG_ERROR(("line %d (FIND): parser error\n", m_cur_line + 1));
      exit(-1);
    }
    *pflags = m_tokens[1].c_str();
    *pkeydata = m_tokens[2].c_str();
    LOG_VERBOSE(("%d: line %u: reading token '%s' (%s)...................\n", 
          m_db->get_id(), m_cur_line + 1, m_tokens[0].c_str(), *pkeydata));
    return (kCommandFind);
  }

  LOG_VERBOSE(("%d: line %u: reading token '%s'........................\n", 
        m_db->get_id(), m_cur_line + 1, m_tokens[0].c_str()));

  if (m_tokens[0] == "BREAK") {
    printf("[info] break at %s:%u\n", __FILE__, __LINE__);
    return (kCommandNop);
  }
  if (m_tokens[0] == "--") {
    return (kCommandNop);
  }
  if (m_tokens[0] == "FULLCHECK") {
    return (kCommandFullcheck);
  }
  if (m_tokens[0] == "TABLESCAN") {
    return (kCommandTablescan);
  }
  if (m_tokens[0] == "BEGIN_TXN" || m_tokens[0] == "TXN_BEGIN") {
    return (kCommandBeginTxn);
  }
  if (m_tokens[0] == "TXN_COMMIT" || m_tokens[0] == "COMMIT_TXN") {
    return (kCommandCommitTxn);
  }
  if (m_tokens[0] == "CLOSE") {
    return (kCommandClose);
  }
  if (m_tokens[0] == "FLUSH") {
    return (kCommandFlush);
  }

  LOG_ERROR(("line %d: invalid token '%s'\n", m_cur_line, m_tokens[0].c_str()));
  ::exit(-1);
  return (0);
}

void
ParserGenerator::read_file()
{
  FILE *f = stdin;
  if (!m_config->filename.empty())
    f = fopen(m_config->filename.c_str(), "rt");

  if (!f) {
    LOG_ERROR(("failed to open %s\n", m_config->filename.c_str()));
    exit(-1);
  }

  char line[1024 * 16];

  while (!feof(f)) {
    char *p = fgets(line, sizeof(line), f);
    if (!p)
      break;

    m_lines.push_back(p);
  }

  if (f != stdin)
    fclose(f);
}

std::vector<std::string> 
ParserGenerator::tokenize(const std::string &str)
{
  std::vector<std::string> tokens;
  std::string delimiters = " \t\n\r()\",";
  // Skip delimiters at beginning.
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  // Find first "non-delimiter".
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);

  while (std::string::npos != pos || std::string::npos != lastPos) {
    // Found a token, add it to the vector.
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    // Skip delimiters.  Note the "not_of"
    lastPos = str.find_first_not_of(delimiters, pos);
    // Find next "non-delimiter"
    pos = str.find_first_of(delimiters, lastPos);
  }
  return tokens;
}

