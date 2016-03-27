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

namespace Upscaledb
{
  using System;

  /// <summary>
  /// A UQI Result class
  /// </summary>
  public class Result : IDisposable
  {
    /// <summary>
    /// Constructor which creates a new Result
    /// </summary>
    /// <param name="handle">The internal upscaledb handle</param>
    internal Result(IntPtr handle) {
      this.handle = handle;
    }

    /// <summary>
    /// Closes the Cursor
    /// </summary>
    /// <remarks>
    /// This method wraps the native uqi_result_close function.
    /// <br />
    /// Closes this Result and frees allocated memory.
    /// </remarks>
    public void Close() {
      if (handle == IntPtr.Zero)
        return;
      NativeMethods.ResultClose(handle);
      handle = IntPtr.Zero;
    }

    /// <summary>
    /// Returns the number of rows.
    /// </summary>
    public int GetRowCount() {
      return NativeMethods.ResultGetRowCount(handle);
    }

    /// <summary>
    /// Returns the key type.
    /// </summary>
    public int GetKeyType() {
      return NativeMethods.ResultGetKeyType(handle);
    }

    /// <summary>
    /// Returns the record type.
    /// </summary>
    public int GetRecordType() {
      return NativeMethods.ResultGetRecordType(handle);
    }

    /// <summary>
    /// Returns the key of a specific row.
    /// </summary>
    public byte[] GetKey(int row) {
      return NativeMethods.ResultGetKey(handle, row);
    }

    // public extern void *ResultGetKeyData(IntPtr handle, ref int size);
    // TODO

    /// <summary>
    /// Returns the record of a specific row.
    /// </summary>
    public byte[] GetRecord(int row) {
      return NativeMethods.ResultGetRecord(handle, row);
    }

    // public void *ResultGetRecordData(IntPtr handle, ref int size);
    // TODO

    /// <summary>
    /// Closes the Result.
    /// </summary>
    /// <see cref="Close" />
    public void Dispose() {
      Close();
    }

    private IntPtr handle;
  }
}
