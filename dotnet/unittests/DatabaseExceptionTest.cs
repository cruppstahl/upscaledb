/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
using System.Collections.Generic;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Upscaledb;

namespace Unittests
{
    public class DatabaseExceptionTest
    {
        private void GetErrno() {
            DatabaseException e = new DatabaseException(13);
            Assert.AreEqual(13, e.ErrorCode);
        }

        private void GetMessage() {
            DatabaseException e = new DatabaseException(-8);
            Assert.AreEqual("Invalid parameter", e.Message);
        }

        public void Run()
        {
            Console.WriteLine("DatabaseExceptionTest.GetErrno");
            GetErrno();

            //Console.WriteLine("DatabaseExceptionTest.GetMessage");
            //GetMessage();
        }

    }
}
