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

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct ScanVisitor;
struct SelectStatement;
struct DatabaseConfiguration;

struct ScanVisitorFactoryHelper
{
  /*
   * Creates and initializes a ScanVisitor object based on various
   * runtime parameters
   */
  template<template <typename, typename> class T>
  static ScanVisitor *create(const DatabaseConfiguration *cfg,
                  SelectStatement *stmt) {
    // validate() ignores the template parameters
    if (!T<uint8_t, uint8_t>::validate(cfg, stmt))
      return (0);

    switch (cfg->key_type) {
      case UPS_TYPE_UINT8: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<uint8_t, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<uint8_t, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<uint8_t, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<uint8_t, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<uint8_t, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<uint8_t, double>(cfg, stmt));
          default:
            return (new T<uint8_t, char>(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_UINT16: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<uint16_t, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<uint16_t, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<uint16_t, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<uint16_t, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<uint16_t, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<uint16_t, double>(cfg, stmt));
          default:
            return (new T<uint16_t, char>(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_UINT32: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<uint32_t, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<uint32_t, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<uint32_t, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<uint32_t, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<uint32_t, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<uint32_t, double>(cfg, stmt));
          default:
            return (new T<uint32_t, char>(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_UINT64: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<uint64_t, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<uint64_t, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<uint64_t, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<uint64_t, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<uint64_t, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<uint64_t, double>(cfg, stmt));
          default:
            return (new T<uint64_t, char>(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_REAL32: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<float, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<float, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<float, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<float, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<float, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<float, double>(cfg, stmt));
          default:
            return (new T<float, char>(cfg, stmt));
        }
        break;
      }
      case UPS_TYPE_REAL64: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<double, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<double, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<double, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<double, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<double, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<double, double>(cfg, stmt));
          default:
            return (new T<double, char>(cfg, stmt));
        }
        break;
      }
      default: {
        switch (cfg->record_type) {
          case UPS_TYPE_UINT8:
            return (new T<char, uint8_t>(cfg, stmt));
          case UPS_TYPE_UINT16:
            return (new T<char, uint16_t>(cfg, stmt));
          case UPS_TYPE_UINT32:
            return (new T<char, uint32_t>(cfg, stmt));
          case UPS_TYPE_UINT64:
            return (new T<char, uint64_t>(cfg, stmt));
          case UPS_TYPE_REAL32:
            return (new T<char, float>(cfg, stmt));
          case UPS_TYPE_REAL64:
            return (new T<char, double>(cfg, stmt));
          default:
            return (new T<char, char>(cfg, stmt));
        }
        break;
      }
    }

    return (0);
  }
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_SCANVISITORFACOTRYHELPER_H */
