/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

using System;
using System.Runtime.Serialization;

[assembly: CLSCompliant(true)]
namespace Hamster
{
  /// <summary>
  /// An Exception class for hamsterdb errors
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
    /// <param name="error">A hamsterdb error code</param>
    public DatabaseException(int error) {
      this.error=error;
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
    /// The hamsterdb error code
    /// </summary>
    public int ErrorCode {
      get {
        return error;
      }
      set {
        error=value;
      }
    }

    /// <summary>
    /// The hamsterdb error message
    /// </summary>
    public override String Message {
      get {
        return NativeMethods.StringError(error);
      }
    }

    private int error;
  }
}
