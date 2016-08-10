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

package de.crupp.upscaledb;

/**
 * Describes a database operation (for Database::bulk_operations)
 */
public class Operation {
  /** Constructor */
  public Operation(int type_, byte[] key_, byte[] record_, int flags_) {
    this.type = type_;
    this.key = key_;
    this.record = record_;
    this.flags = flags_;
  }

  /** The type of the operation - Const.UPS_DB_INSERT, Const.UPS_DB_ERASE
   * or CONST.UPS_DB_FIND */
  public int type;

  /** The key */
  public byte[] key;

  /** The record - can be null */
  public byte[] record;

  /** The flags as specified for Database::insert(), Database::erase() or
   * Database::find() */
  public int flags;

  /** The result, as returned by the upscaledb engine */
  public int result;
}
