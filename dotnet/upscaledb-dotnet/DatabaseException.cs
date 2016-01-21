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

using System;
using System.Runtime.Serialization;

[assembly: CLSCompliant(true)]
namespace Upscaledb
{
  /// <summary>
  /// An Exception class for upscaledb errors
  /// </summary>
  [Serializable]
  public class DatabaseException : System.Exception
  {
    /// <summary>
    /// Empty Constructor
    /// </summary>
    public DatabaseException() {
    }

    /// <summary>
    /// Constructor accepting an error code
    /// </summary>
    /// <param name="error">A upscaledb error code</param>
    public DatabaseException(int error) {
      this.error = error;
    }

    /// <summary>
    /// Constructor accepting an error message
    /// </summary>
    /// <param name="message">An error message</param>
    public DatabaseException(string message)
      : base(message) {
    }

    /// <summary>
    /// Constructor provided for standard compliancy
    /// </summary>
    /// <param name="message">An error message</param>
    /// <param name="innerException">An inner exception</param>
    public DatabaseException(string message, Exception innerException)
      : base (message, innerException) {
    }

    /// <summary>
    /// Constructor accepting Serialization info
    /// </summary>
    /// <param name="info">The serialization info</param>
    /// <param name="context">The serialization context</param>
    protected DatabaseException(SerializationInfo info,
      StreamingContext context)
      : base(info, context) {
    }

    /// <summary>
    /// The upscaledb error code
    /// </summary>
    public int ErrorCode {
      get {
        return error;
      }
      set {
        error = value;
      }
    }

    /// <summary>
    /// The upscaledb error message
    /// </summary>
    public override String Message {
      get {
        return NativeMethods.StringError(error);
      }
    }

    private int error;
  }
}
