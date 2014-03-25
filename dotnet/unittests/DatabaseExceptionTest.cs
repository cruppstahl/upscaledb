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
using System.Collections.Generic;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Hamster;

namespace Unittests
{
    [TestClass()]
    [DeploymentItem("..\\win32\\msvc2008\\out\\dll_debug\\hamsterdb-2.1.7.dll")]
    public class DatabaseExceptionTest
    {
        [TestMethod()]
        public void GetErrno() {
            DatabaseException e = new DatabaseException(13);
            Assert.AreEqual(13, e.ErrorCode);
        }

        [TestMethod()]
        public void GetMessage() {
            DatabaseException e = new DatabaseException(-8);
            Assert.AreEqual("Invalid parameter", e.Message);
        }
    }
}
