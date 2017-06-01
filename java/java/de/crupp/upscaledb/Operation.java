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
