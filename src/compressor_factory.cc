/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 *
 * See files COPYING.* for License information.
 *
 */

#include "error.h"

#include "compressor_factory.h"
#include "compressor_zlib.h"
#include "compressor_snappy.h"
#include "compressor_lzf.h"
#include "compressor_lzop.h"

namespace hamsterdb {

Compressor *
CompressorFactory::create(int type)
{
#ifdef HAM_ENABLE_COMPRESSION
  switch (type) {
    case HAM_COMPRESSOR_ZLIB:
#ifdef HAVE_ZLIB_H
      return (new ZlibCompressor());
#else
      ham_log(("hamsterdb was built without support for zlib compression"));
      throw Exception(HAM_INV_PARAMETER);
#endif
    case HAM_COMPRESSOR_SNAPPY:
#ifdef HAVE_SNAPPY_H
      return (new SnappyCompressor());
#else
      ham_log(("hamsterdb was built without support for snappy compression"));
      throw Exception(HAM_INV_PARAMETER);
#endif
    case HAM_COMPRESSOR_LZO:
#ifdef HAVE_LZO_LZO1X_H
      return (new LzopCompressor());
#else
      ham_log(("hamsterdb was built without support for lzop compression"));
      throw Exception(HAM_INV_PARAMETER);
#endif
    case HAM_COMPRESSOR_LZF:
      // this is always available
      return (new LzfCompressor());
    default:
      ham_log(("Unknown compressor type %d", type));
      throw Exception(HAM_INV_PARAMETER);
  }
#endif // HAM_ENABLE_COMPRESSION
  ham_log(("hamsterdb was built without compression"));
  throw Exception(HAM_INV_PARAMETER);
}

}; // namespace hamsterdb
