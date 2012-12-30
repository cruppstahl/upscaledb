/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief provides forward declarations of internally used types
 *
 * This header file provides these forward declarations to prevent several
 * cyclic dependencies; in particular, the @ref Page is a type used
 * throughout, but a @ref Page contains several other types, which
 * again reference @ref Page pointers either directly or indirectly.
 *
 * To solve this self-referential issue once and for all, all major hamster
 * internal types are forward declared here; when the code requires the actual
 * implementation of a type it can include the related header file any time it
 * wishes.
 *
 * @remark This way of solving the cyclic dependency conundrum has the added
 *     benefit of having a single, well known spot where the actual
 *     'typedef' lines reside, so there's zero risk of 'double defined types'.
 */

#ifndef HAM_FWD_DECL_CMN_TYPES_H__
#define HAM_FWD_DECL_CMN_TYPES_H__

#include <ham/hamsterdb.h>
#include <ham/hamsterdb_int.h>

#include "config.h"

namespace hamsterdb {

#define OFFSETOF(type, member) ((size_t) &((type *)0)->member)

class Allocator;

class Cursor;

class Database;

class Device;

class Environment;

class Transaction;

class Page;

class Cache;

class Log;

class Journal;

struct extkey_t;
typedef struct extkey_t extkey_t;

class ExtKeyCache;

class Freelist;
struct FreelistEntry;

struct freelist_hints_t;
typedef struct freelist_hints_t freelist_hints_t;

struct runtime_statistics_pagedata_t;
typedef struct runtime_statistics_pagedata_t runtime_statistics_pagedata_t;

struct freelist_global_hints_t;
typedef struct freelist_global_hints_t freelist_global_hints_t;

struct find_hints_t;
typedef struct find_hints_t find_hints_t;

struct insert_hints_t;
typedef struct insert_hints_t insert_hints_t;

struct erase_hints_t;
typedef struct erase_hints_t erase_hints_t;

struct TransactionCursor;

#include "packstart.h"

struct FreelistPayload;

#include "packstop.h"


#include "packstart.h"

struct BtreeKey;

#include "packstop.h"

} // namespace hamsterdb

#include <boost/version.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/tss.hpp>
#include <boost/thread/condition.hpp>

typedef boost::mutex::scoped_lock ScopedLock;
typedef boost::thread Thread;
typedef boost::condition Condition;

class Mutex : public boost::mutex {
public:
#if BOOST_VERSION < 103500
  typedef boost::detail::thread::lock_ops<boost::mutex> Ops;

  void lock() { Ops::lock(*this); }

  void unlock() { Ops::unlock(*this); }
#endif
};

#endif

