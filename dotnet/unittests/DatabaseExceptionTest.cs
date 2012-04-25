/**
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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
using System.Collections.Generic;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Hamster;

namespace Unittests
{
    [TestClass()]
    [DeploymentItem("..\\win32\\out\\dll_debug\\hamsterdb-2.0.2.dll")]
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
