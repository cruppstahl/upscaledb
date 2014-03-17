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

#include "compressor.h"

namespace hamsterdb {

class CompressorFactory {
  public:
    // Creates a new Compressor instance for the specified |type| (being
    // HAM_COMPRESSOR_ZLIB, HAM_COMPRESSOR_SNAPPY etc)
    //
    // The compression |level| is currently only used for zlib.
    static Compressor *create(int type, int level = 7);
};

}; // namespace hamsterdb
