/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
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
    case HAM_COMPRESSOR_UINT32_MASKEDVBYTE:
    case HAM_COMPRESSOR_UINT32_BLOCKINDEX:
    case HAM_COMPRESSOR_UINT32_FOR:
    case HAM_COMPRESSOR_UINT32_SIMDFOR:
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
