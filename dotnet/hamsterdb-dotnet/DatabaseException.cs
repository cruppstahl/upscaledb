/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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
