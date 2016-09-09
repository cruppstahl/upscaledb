/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

/*
 * Helper function to create scan visitors
 */

#ifndef UPS_UPSCALEDB_SCANVISITORFACOTRYHELPER_H
#define UPS_UPSCALEDB_SCANVISITORFACOTRYHELPER_H

#include "0root/root.h"

#include <string>

#include "ups/upscaledb_uqi.h"

#include "4uqi/type_wrapper.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct ScanVisitor;
struct SelectStatement;
struct DbConfig;

#define TW(t) TypeWrapper<t>

struct ScanVisitorFactoryHelper
{
  /*
   * Creates and initializes a ScanVisitor object based on various
   * runtime parameters
   */
  template<template <typename, typename> class T>
  static ScanVisitor *create(const DbConfig *cfg, SelectStatement *stmt) {
    // only numeric input accepted?
    if (T< TW(uint8_t), TW(uint8_t) >::kOnlyNumericInput) {
      if (ISSET(stmt->function.flags, UQI_STREAM_RECORD)
          && ISSET(stmt->function.flags, UQI_STREAM_KEY)) {
        ups_trace(("function does not accept binary input"));
        return 0;
      }

      int type = cfg->key_type;
      if (ISSET(stmt->function.flags, UQI_STREAM_RECORD))
        type = cfg->record_type;

      if (type == UPS_TYPE_CUSTOM || type == UPS_TYPE_BINARY) {
        ups_trace(("function does not accept binary input"));
        return 0;
      }
    }

    // decide whether keys, records or both streams need to be processed
    if (!T< TW(uint8_t), TW(uint8_t) >::kRequiresBothStreams) {
      stmt->requires_keys = ISSET(stmt->function.flags, UQI_STREAM_KEY);
      stmt->requires_records = ISSET(stmt->function.flags, UQI_STREAM_RECORD);
      if (stmt->predicate_plg) {
        if (ISSET(stmt->predicate_plg->flags, UQI_PLUGIN_REQUIRE_BOTH_STREAMS)) {
          stmt->requires_keys = true;
          stmt->requires_records = true;
        }
        if (ISSET(stmt->predicate.flags, UQI_STREAM_KEY))
          stmt->requires_keys = true;
        if (ISSET(stmt->predicate.flags, UQI_STREAM_RECORD))
          stmt->requires_records = true;
      }
    }

    switch (cfg->key_type) {
      case UPS_TYPE_UINT8: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(uint8_t), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(uint8_t), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(uint8_t), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(uint8_t), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(uint8_t), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(uint8_t), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(uint8_t), TW(char) >(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_UINT16: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(uint16_t), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(uint16_t), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(uint16_t), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(uint16_t), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(uint16_t), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(uint16_t), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(uint16_t), TW(char) >(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_UINT32: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(uint32_t), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(uint32_t), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(uint32_t), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(uint32_t), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(uint32_t), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(uint32_t), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(uint32_t), TW(char) >(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_UINT64: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(uint64_t), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(uint64_t), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(uint64_t), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(uint64_t), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(uint64_t), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(uint64_t), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(uint64_t), TW(char) >(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_REAL32: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(float), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(float), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(float), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(float), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(float), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(float), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(float), TW(char) >(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_REAL64: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(double), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(double), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(double), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(double), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(double), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(double), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(double), TW(char) >(cfg, stmt));
        }
        break;
      }
      default: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T< TW(char), TW(uint8_t) >(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T< TW(char), TW(uint16_t) >(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T< TW(char), TW(uint32_t) >(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T< TW(char), TW(uint64_t) >(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T< TW(char), TW(float) >(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T< TW(char), TW(double) >(cfg, stmt));
          default:
            return (new T< TW(char), TW(char) >(cfg, stmt));
        }
        break;
      }
    }

    return (0);
  }
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_SCANVISITORFACOTRYHELPER_H */
