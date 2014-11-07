/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    /// The hamsterdb error code
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
