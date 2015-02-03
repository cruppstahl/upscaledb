/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2compressor/compressor_factory.h"
#include "2compressor/compressor_zlib.h"
#include "2compressor/compressor_snappy.h"
#include "2compressor/compressor_lzf.h"
#include "2compressor/compressor_lzop.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

bool
CompressorFactory::is_available(int type)
{
#ifdef HAM_ENABLE_COMPRESSION
  switch (type) {
    case HAM_COMPRESSOR_UINT32_VARBYTE:
    case HAM_COMPRESSOR_UINT32_SIMDCOMP:
    case HAM_COMPRESSOR_UINT32_GROUPVARINT:
    case HAM_COMPRESSOR_UINT32_STREAMVBYTE:
      return (true);
    case HAM_COMPRESSOR_ZLIB:
#ifdef HAVE_ZLIB_H
      return (true);
#else
      return (false);
#endif
    case HAM_COMPRESSOR_SNAPPY:
#ifdef HAVE_SNAPPY_H
      return (true);
#else
      return (false);
#endif
    case HAM_COMPRESSOR_LZO:
#ifdef HAVE_LZO_LZO1X_H
      return (true);
#else
      return (false);
#endif
    case HAM_COMPRESSOR_LZF:
      // this is always available
      return (true);
    default:
      return (false);
  }
#endif // HAM_ENABLE_COMPRESSION
  return (false);
}

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
