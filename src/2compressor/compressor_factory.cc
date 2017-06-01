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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2compressor/compressor_factory.h"
#include "2compressor/compressor_zlib.h"
#include "2compressor/compressor_snappy.h"
#include "2compressor/compressor_lzf.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

bool
CompressorFactory::is_available(int type)
{
  switch (type) {
    case UPS_COMPRESSOR_UINT32_STREAMVBYTE:
    case UPS_COMPRESSOR_UINT32_SIMDFOR:
    case UPS_COMPRESSOR_UINT32_SIMDCOMP:
#ifdef HAVE_SSE2
      return true;
#else
      return false;
#endif
    case UPS_COMPRESSOR_UINT32_VARBYTE:
    case UPS_COMPRESSOR_UINT32_GROUPVARINT:
    case UPS_COMPRESSOR_UINT32_FOR:
      return true;
    case UPS_COMPRESSOR_ZLIB:
#ifdef HAVE_ZLIB_H
      return true;
#else
      return false;
#endif
    case UPS_COMPRESSOR_SNAPPY:
#ifdef HAVE_SNAPPY_H
      return true;
#else
      return false;
#endif
    case UPS_COMPRESSOR_LZF:
      // this is always available
      return true;
    default:
      return false;
  }
}

Compressor *
CompressorFactory::create(int type)
{
  switch (type) {
    case UPS_COMPRESSOR_ZLIB:
#ifdef HAVE_ZLIB_H
      return new CompressorImpl<ZlibCompressor>();
#else
      ups_log(("upscaledb was built without support for zlib compression"));
      throw Exception(UPS_INV_PARAMETER);
#endif
    case UPS_COMPRESSOR_SNAPPY:
#ifdef HAVE_SNAPPY_H
      return new CompressorImpl<SnappyCompressor>();
#else
      ups_log(("upscaledb was built without support for snappy compression"));
      throw Exception(UPS_INV_PARAMETER);
#endif
    case UPS_COMPRESSOR_LZF:
      // this is always available
      return new CompressorImpl<LzfCompressor>();
    default:
      ups_log(("Unknown compressor type %d", type));
      throw Exception(UPS_INV_PARAMETER);
  }
  throw Exception(UPS_INV_PARAMETER);
}

}; // namespace upscaledb
