/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

class Cursor;

class Database;

class Device;

class Environment;

class Transaction;

class Page;

class Cache;

class Log;

class Journal;

class ExtKeyCache;

class Freelist;
struct FreelistEntry;

struct RuntimePageStatistics;
typedef struct RuntimePageStatistics RuntimePageStatistics;

struct find_hints_t;
typedef struct find_hints_t find_hints_t;

struct insert_hints_t;
typedef struct insert_hints_t insert_hints_t;

struct erase_hints_t;
typedef struct erase_hints_t erase_hints_t;

class TransactionCursor;

#include "packstart.h"

struct PFreelistPayload;

#include "packstop.h"


#include "packstart.h"

struct PBtreeKey;

#include "packstop.h"

} // namespace hamsterdb

#endif

