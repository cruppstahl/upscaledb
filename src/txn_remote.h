/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_TXN_REMOTE_H__
#define HAM_TXN_REMOTE_H__

#ifdef HAM_ENABLE_REMOTE

#include "txn.h"

namespace hamsterdb {

//
// A remote Transaction
//
class RemoteTransaction : public Transaction
{
  public:
    // Constructor; "begins" the Transaction
    // supported flags: HAM_TXN_READ_ONLY, HAM_TXN_TEMPORARY
    RemoteTransaction(Environment *env, const char *name, ham_u32_t flags);

    // Commits the Transaction
    virtual void commit(ham_u32_t flags = 0);

    // Aborts the Transaction
    virtual void abort(ham_u32_t flags = 0);

    // Returns the remote Transaction handle
    ham_u64_t get_remote_handle() const {
      return (m_remote_handle);
    }

    // Sets the remote Transaction handle
    void set_remote_handle(ham_u64_t handle) {
      m_remote_handle = handle;
    }

  private:
    // The remote Transaction handle
    ham_u64_t m_remote_handle;
};


} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

#endif /* HAM_TXN_REMOTE_H__ */
