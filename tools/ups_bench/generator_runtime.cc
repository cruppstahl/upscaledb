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

#include "configuration.h"
#include "datasource_numeric.h"
#include "datasource_binary.h"
#include "datasource_string.h"
#include "generator_runtime.h"

#define kZipfianLimit       (1024 * 1024)

#undef min // avoid MSVC conflicts with std::min

RuntimeGenerator::RuntimeGenerator(int id, Configuration *conf, Database *db,
                bool show_progress)
  : Generator(id, conf, db), m_state(0), m_opcount(0),
    m_datasource(0), m_u01(m_rng), m_elapsed_seconds(0.0), m_txn(0),
    m_cursor(0), m_progress(0), m_success(true), m_erase_only(false)
{
  if (conf->seed)
    m_rng.seed(conf->seed);

  memset(&m_metrics, 0, sizeof(m_metrics));
  m_metrics.insert_latency_min = 9999999.99;
  m_metrics.erase_latency_min = 9999999.99;
  m_metrics.find_latency_min = 9999999.99;
  m_metrics.txn_commit_latency_min = 9999999.99;

  if (show_progress) {
    if (!conf->no_progress && !conf->quiet && !conf->verbose)
      m_progress = new boost::progress_display(std::max(
                              std::max(conf->limit_bytes, conf->limit_ops),
                              conf->limit_seconds));
  }

  if (!m_config->tee_file.empty())
    m_tee.open(m_config->tee_file.c_str(), std::ios::out);

  switch (conf->key_type) {
    case Configuration::kKeyUint8:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new NumericRandomDatasource<uint8_t>(conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new NumericAscendingDatasource<uint8_t>();
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new NumericDescendingDatasource<uint8_t>();
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new NumericZipfianDatasource<uint8_t>(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
    case Configuration::kKeyUint16:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new NumericRandomDatasource<uint16_t>(conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new NumericAscendingDatasource<uint16_t>();
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new NumericDescendingDatasource<uint16_t>();
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new NumericZipfianDatasource<uint16_t>(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
    case Configuration::kKeyUint32:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new NumericRandomDatasource<uint32_t>(conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new NumericAscendingDatasource<uint32_t>();
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new NumericDescendingDatasource<uint32_t>();
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new NumericZipfianDatasource<uint32_t>(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
        case Configuration::kDistributionClustered:
          m_datasource = new NumericClusteredDatasource(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
      }
      break;
    case Configuration::kKeyUint64:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new NumericRandomDatasource<uint64_t>(conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new NumericAscendingDatasource<uint64_t>();
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new NumericDescendingDatasource<uint64_t>();
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new NumericZipfianDatasource<uint64_t>(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
    case Configuration::kKeyCustom:
    case Configuration::kKeyBinary:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new BinaryRandomDatasource(conf->key_size,
                          conf->key_is_fixed_size, conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new BinaryAscendingDatasource(conf->key_size,
                          conf->key_is_fixed_size);
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new BinaryDescendingDatasource(conf->key_size,
                          conf->key_is_fixed_size);
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new BinaryZipfianDatasource(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->key_size, conf->key_is_fixed_size, conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
    case Configuration::kKeyString:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new StringRandomDatasource(conf->key_size,
                          conf->key_is_fixed_size, conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new StringAscendingDatasource(conf->key_size,
                          conf->key_is_fixed_size);
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new StringDescendingDatasource(conf->key_size,
                          conf->key_is_fixed_size);
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new StringZipfianDatasource(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->key_size, conf->key_is_fixed_size, conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
    case Configuration::kKeyReal32:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new NumericRandomDatasource<float>(conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new NumericAscendingDatasource<float>();
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new NumericDescendingDatasource<float>();
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new NumericZipfianDatasource<float>(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
    case Configuration::kKeyReal64:
      switch (conf->distribution) {
        case Configuration::kDistributionRandom:
          m_datasource = new NumericRandomDatasource<double>(conf->seed);
          break;
        case Configuration::kDistributionAscending:
          m_datasource = new NumericAscendingDatasource<double>();
          break;
        case Configuration::kDistributionDescending:
          m_datasource = new NumericDescendingDatasource<double>();
          break;
        case Configuration::kDistributionZipfian:
          m_datasource = new NumericZipfianDatasource<double>(
                          conf->limit_ops ? conf->limit_ops : kZipfianLimit,
                          conf->seed);
          break;
        case Configuration::kDistributionClustered:
          printf("'clustered' distribution only available for --key=uint32!\n");
          break;
      }
      break;
  }
  assert(m_datasource != 0);
}

bool
RuntimeGenerator::execute()
{
  if (m_state == kStateStopped)
    return (false);

  double insert_latency = 0.0;
  double erase_latency = 0.0;
  double find_latency = 0.0;
  double commit_latency = 0.0;

  int cmd = get_next_command();
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
      insert_latency = insert();
      break;
    case Generator::kCommandErase:
      erase_latency = erase();
      break;
    case Generator::kCommandFind:
      find_latency = find();
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
      commit_latency = txn_commit();
      break;
    default:
      assert(!"shouldn't be here");
  }

  m_opcount++;

  if (m_progress && m_config->limit_ops)
    (*m_progress) += 1;

  // write page fetch/flush graphs?
  if (m_graph) {
    Metrics m;
    m_db->get_metrics(&m);

    double elapsed = m_start.seconds();

    uint32_t flushes = 0;
    uint32_t fetches = 0;
    if (m.upscaledb_metrics.page_count_flushed
            > m_metrics.upscaledb_metrics.page_count_flushed) {
      flushes = m.upscaledb_metrics.page_count_flushed
              - m_metrics.upscaledb_metrics.page_count_flushed;
      m_metrics.upscaledb_metrics.page_count_flushed
              = m.upscaledb_metrics.page_count_flushed;
    }
    if (m.upscaledb_metrics.page_count_fetched
            > m_metrics.upscaledb_metrics.page_count_fetched) {
      fetches = m.upscaledb_metrics.page_count_fetched
              - m_metrics.upscaledb_metrics.page_count_fetched;
      m_metrics.upscaledb_metrics.page_count_fetched
              = m.upscaledb_metrics.page_count_fetched;
    }

    m_graph->add_latency_metrics(elapsed, insert_latency, find_latency,
                erase_latency, commit_latency, fetches, flushes);
  }


  return (true);
}

void
RuntimeGenerator::create()
{
  tee("CREATE");
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
RuntimeGenerator::open()
{
  tee("OPEN");
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
RuntimeGenerator::close()
{
  if (m_cursor) {
    m_db->cursor_close(m_cursor);
    m_cursor = 0;
  }

  if (m_txn)
    txn_commit(); // sets m_txn to 0

  tee("CLOSE");

  m_last_status = m_db->close_db();
  if (m_last_status != 0)
    m_success = false;

  m_db->close_env();

  m_metrics.other_ops++;
  m_metrics.elapsed_wallclock_seconds = m_start.seconds();
  m_is_active = false;
}

double
RuntimeGenerator::insert()
{
  ups_key_t key = generate_key();
  ups_record_t rec = generate_record();

  tee("INSERT", &key, &rec);

  Timer<boost::chrono::high_resolution_clock> t;

  if (m_cursor)
    m_last_status = m_db->cursor_insert(m_cursor, &key, &rec);
  else
    m_last_status = m_db->insert(m_txn, &key, &rec);

  double elapsed = t.seconds();

  m_opspersec[kCommandInsert]++;

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

  return (elapsed);
}

double
RuntimeGenerator::erase()
{
  ups_key_t key = generate_key();

  tee("ERASE", &key);

  Timer<boost::chrono::high_resolution_clock> t;

  if (m_cursor)
    m_last_status = m_db->cursor_erase(m_cursor, &key);
  else
    m_last_status = m_db->erase(m_txn, &key);

  double elapsed = t.seconds();

  m_opspersec[kCommandErase]++;

  if (m_metrics.erase_latency_min > elapsed)
    m_metrics.erase_latency_min = elapsed;
  if (m_metrics.erase_latency_max < elapsed)
    m_metrics.erase_latency_max = elapsed;
  m_metrics.erase_latency_total += elapsed;

  if (m_last_status != 0 && m_last_status != UPS_KEY_NOT_FOUND)
    m_success = false;

  m_metrics.erase_ops++;

  return (elapsed);
}

double
RuntimeGenerator::find()
{
  ups_key_t key = generate_key();
  ups_record_t m_record = {0};
  memset(&m_record, 0, sizeof(m_record));

  tee("FIND", &key);

  Timer<boost::chrono::high_resolution_clock> t;

  if (m_cursor)
    m_last_status = m_db->cursor_find(m_cursor, &key, &m_record);
  else
    m_last_status = m_db->find(m_txn, &key, &m_record);

  double elapsed = t.seconds();

  m_opspersec[kCommandFind]++;

  if (m_metrics.find_latency_min > elapsed)
    m_metrics.find_latency_min = elapsed;
  if (m_metrics.find_latency_max < elapsed)
    m_metrics.find_latency_max = elapsed;
  m_metrics.find_latency_total += elapsed;

  if (m_last_status != 0 && m_last_status != UPS_KEY_NOT_FOUND)
    m_success = false;

  m_metrics.find_bytes += m_record.size;
  m_metrics.find_ops++;

  return (elapsed);
}

void
RuntimeGenerator::tablescan()
{
  tee("TABLESCAN");

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
RuntimeGenerator::txn_begin()
{
  tee("BEGIN_TXN");
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
RuntimeGenerator::txn_abort()
{
  tee("TXN_ABORT");
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

double
RuntimeGenerator::txn_commit()
{
  tee("TXN_COMMIT");
  assert(m_txn != 0);

  if (m_cursor) {
    m_db->cursor_close(m_cursor);
    m_cursor = 0;
  }

  Timer<boost::chrono::high_resolution_clock> t;

  m_last_status = m_db->txn_commit(m_txn);
  m_txn = 0;

  double elapsed = t.seconds();

  m_opspersec[kCommandCommitTxn]++;

  if (m_metrics.txn_commit_latency_min > elapsed)
    m_metrics.txn_commit_latency_min = elapsed;
  if (m_metrics.txn_commit_latency_max < elapsed)
    m_metrics.txn_commit_latency_max = elapsed;
  m_metrics.txn_commit_latency_total += elapsed;

  if (m_last_status != 0)
    m_success = false;

  m_metrics.txn_commit_ops++;
  return (elapsed);
}

ups_key_t
RuntimeGenerator::generate_key()
{
  ups_key_t key = {0};
  m_datasource->next(m_key_data);

  // append terminating 0 byte
  m_key_data.resize(m_key_data.size() + 1);
  m_key_data[m_key_data.size() - 1] = 0;

  key.data = &m_key_data[0];
  key.size = m_key_data.size() - 1;
  return (key);
}

ups_record_t
RuntimeGenerator::generate_record()
{
  ups_record_t rec = {0};
  m_record_data.resize(m_config->rec_size);
  // make the record unique (more or less)
  size_t size = std::min((int)sizeof(m_opcount), m_config->rec_size);
  memcpy(&m_record_data[0], &m_opcount, size);
  for (int i = size; i < m_config->rec_size; i++)
    m_record_data[i] = (uint8_t)i;

  rec.data = &m_record_data[0];
  rec.size = m_record_data.size();
  return (rec);
}

int
RuntimeGenerator::get_next_command()
{
  // limit reached - last command? then either delete everything or 'close'
  if (limit_reached()) {
    if (m_erase_only == false && m_config->bulk_erase) {
      m_opcount = 0;
      m_erase_only = true;
      m_datasource->reset();
      return (Generator::kCommandErase);
    }

    if (m_state == kStateRunning) {
      if (m_txn)
        return (Generator::kCommandCommitTxn);
      m_state = kStateStopped;
      return (Generator::kCommandClose);
    }
  }

  // only send erase?
  if (m_erase_only)
    return (Generator::kCommandErase);

  // first command? then either 'create' or 'reopen', depending on flags
  if (m_opcount == 0) {
    if (m_config->open)
      return (Generator::kCommandOpen);
    else
      return (Generator::kCommandCreate);
  }

  // begin/abort/commit transactions!
  if (m_config->transactions_nth) {
    if (!m_txn)
      return (Generator::kCommandBeginTxn);
    // add +2 because txn_begin/txn_commit are also counted in m_opcount
    if (m_opcount % (m_config->transactions_nth + 2) == 0)
      return (Generator::kCommandCommitTxn);
  }

  // perform "real" work
  if (m_config->erase_pct || m_config->find_pct || m_config->table_scan_pct) {
    double d = m_u01();
    if (d * 100 < m_config->erase_pct)
      return (Generator::kCommandErase);
    if (d * 100 >= m_config->erase_pct
        && d * 100 < (m_config->erase_pct + m_config->find_pct))
      return (Generator::kCommandFind);
    if (d * 100 >= (m_config->erase_pct + m_config->find_pct)
        && d * 100 < (m_config->erase_pct + m_config->find_pct
                + m_config->table_scan_pct))
      return (Generator::kCommandTablescan);
  }
  return (Generator::kCommandInsert);
}

bool
RuntimeGenerator::limit_reached()
{
  // reached IOPS limit?
  if (m_config->limit_ops) {
    if (m_opcount >= m_config->limit_ops)
      return (true);
  }

  // reached time limit and/or update latency graphs?
  if (m_config->limit_seconds || m_graph) {
    double new_elapsed = m_start.seconds();
    if (new_elapsed - m_elapsed_seconds >= 1.) {
      if (m_progress)
        (*m_progress) += (unsigned)(new_elapsed - m_elapsed_seconds);
      m_elapsed_seconds = new_elapsed;
      if (m_graph)
        m_graph->add_opspersec_graph(m_elapsed_seconds, m_opspersec[0],
                        m_opspersec[1], m_opspersec[2], m_opspersec[3]);
      memset(&m_opspersec, 0, sizeof(m_opspersec));
    }
    if (m_config->limit_seconds && new_elapsed > m_config->limit_seconds) {
      m_elapsed_seconds = new_elapsed;
      return (true);
    }
  }

  // check inserted bytes
  if (m_config->limit_bytes) {
    if (m_metrics.insert_bytes >= m_config->limit_bytes)
      return (true);
  }

  return (false);
}

void
RuntimeGenerator::tee(const char *foo, const ups_key_t *key,
                    const ups_record_t *record)
{
  if (!m_config->tee_file.empty() || m_config->verbose) {
    std::stringstream ss;
    ss << foo;
    if (key) {
      switch (m_config->key_type) {
        case Configuration::kKeyBinary:
        case Configuration::kKeyCustom:
          ss << " (0, \"" << (const char *)key->data << '"';
          break;
        case Configuration::kKeyUint8:
          ss << " (0, \"" << (int)*(const char *)key->data << '"';
          break;
        case Configuration::kKeyUint16:
          ss << " (0, \"" << *(uint16_t *)key->data << '"';
          break;
        case Configuration::kKeyUint32:
          ss << " (0, \"" << *(uint32_t *)key->data << '"';
          break;
        case Configuration::kKeyUint64:
          ss << " (0, \"" << *(uint64_t *)key->data << '"';
          break;
        case Configuration::kKeyReal32:
          ss << " (0, \"" << *(float *)key->data << '"';
          break;
        case Configuration::kKeyReal64:
          ss << " (0, \"" << *(double *)key->data << '"';
          break;
        case Configuration::kKeyString:
          ss << " (0, \"" << (const char *)key->data << '"';
          break;
        default:
          assert(!"shouldn't be here");
      }
    }
    if (record)
      ss << ", " << (uint64_t)record->size;
    if (key || record)
      ss << ")";

    if (!m_config->tee_file.empty())
      m_tee << ss.str() << std::endl;
    else
      std::cout << m_db->get_id() << ": " << ss.str() << std::endl;
  }
}
