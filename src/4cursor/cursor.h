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

/*
 * A Cursor is an object which is used to traverse a database.
 *
 * A Cursor structure is separated into 3 components:
 * 1. The btree cursor
 *      This cursor can traverse btrees. It is described and implemented
 *      in btree_cursor.h.
 * 2. The txn cursor
 *      This cursor can traverse txn-trees. It is described and implemented
 *      in txn_cursor.h.
 * 3. The upper layer
 *      This layer acts as a kind of dispatcher for both cursors. If
 *      Txns are used, then it also uses a duplicate cache for
 *      consolidating the duplicate keys from both cursors. This layer is
 *      described and implemented in cursor.h (this file).
 *
 * A Cursor can have several states. It can be
 * 1. NIL (not in list) - this is the default state, meaning that the Cursor
 *      does not point to any key. If the Cursor was initialized, then it's
 *      "NIL". If the Cursor was erased (i.e. with ups_cursor_erase) then it's
 *      also "NIL".
 *
 *      relevant functions:
 *          Cursor::is_nil
 *          Cursor::set_to_nil
 *
 * 2. Coupled to the txn-cursor - meaning that the Cursor points to a key
 *      that is modified in a Txn. Technically, the txn-cursor points
 *      to a TxnOperation structure.
 *
 *      relevant functions:
 *          Cursor::is_coupled_to_txnop
 *          Cursor::couple_to_txnop
 *
 * 3. Coupled to the btree-cursor - meaning that the Cursor points to a key
 *      that is stored in a Btree. A Btree cursor itself can then be coupled
 *      (it directly points to a page in the cache) or uncoupled, meaning that
 *      the page was purged from the cache and has to be fetched from disk when
 *      the Cursor is used again. This is described in btree_cursor.h.
 *
 *      relevant functions:
 *          Cursor::is_coupled_to_btree
 *          Cursor::couple_to_btree
 *
 * The dupecache is used when information from the btree and the txn-tree
 * is merged. The btree cursor has its private dupecache. The dupecache
 * increases performance (and complexity).
 *
 * The cursor interface is used in db_local.cc. Many of the functions use
 * a high-level cursor interface (i.e. @ref cursor_create, @ref cursor_clone)
 * while some directly use the low-level interfaces of btree_cursor.h and
 * txn_cursor.h. Over time i will clean this up, trying to maintain a clear
 * separation of the 3 layers, and only accessing the top-level layer in
 * cursor.h. This is work in progress.
 *
 * In order to speed up Cursor::move() we keep track of the last compare
 * between the two cursors. i.e. if the btree cursor is currently pointing to
 * a larger key than the txn-cursor, the 'lastcmp' field is <0 etc.
 */

#ifndef UPS_CURSOR_H
#define UPS_CURSOR_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

// A helper structure; ups_cursor_t is declared in ups/upscaledb.h as an
// opaque C structure, but internally we use a C++ class. The ups_cursor_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ups_cursor_t
{
  bool _dummy;
};

namespace upscaledb {

struct Db;
struct Txn;
struct LocalDb;

//
// the Database Cursor
//
struct Cursor {
  // Constructor
  Cursor(Db *db_, Txn *txn_ = 0)
    : db(db_), txn(txn_), next(0), previous(0) {
  }

  // Copy constructor; used for cloning a Cursor
  Cursor(Cursor &other)
    : db(other.db), txn(other.txn), next(0), previous(0) {
  }

  // Destructor
  virtual ~Cursor() {
  }

  // Overwrites the record of a cursor (ups_cursor_overwrite)
  virtual ups_status_t overwrite(ups_record_t *record, uint32_t flags) = 0;

  // Returns position in duplicate list (ups_cursor_get_duplicate_position)
  virtual uint32_t get_duplicate_position() = 0;

  // Returns number of duplicates (ups_cursor_get_duplicate_count)
  virtual uint32_t get_duplicate_count(uint32_t flags) = 0;

  // Get current record size (ups_cursor_get_record_size)
  virtual uint32_t get_record_size() = 0;

  // Closes the cursor
  virtual void close() = 0;

  // The database that this cursor operates on
  Db *db;

  // Pointer to the Txn; can be null
  Txn *txn;

  // Linked list of all Cursors in this database
  Cursor *next, *previous;
};

} // namespace upscaledb

#endif /* UPS_CURSOR_H */
