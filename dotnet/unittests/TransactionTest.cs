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

using Upscaledb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;

namespace Unittests
{
    /// <summary>
    /// This is a test class for TransactionTest and is intended
    /// to contain all TransactionTest Unit Tests
    ///</summary>
    public class TransactionTest
    {
        private Upscaledb.Environment env;
        private Database db;

        private void SetUp()
        {
            env = new Upscaledb.Environment();
            env.Create("test.db", UpsConst.UPS_ENABLE_TRANSACTIONS);
            db = env.CreateDatabase(1);
        }

        private void TearDown()
        {
            db.Close();
            env.Close();
        }

        private void AbortTest()
        {
            Transaction t = env.Begin();
            t.Abort();
        }

        private void CommitTest()
        {
            Transaction t = env.Begin();
            t.Commit();
        }

        private void InsertFindCommitTest()
        {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Transaction t = env.Begin();
            db.Insert(t, k, r);
            db.Find(t, k);
            try {
                db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_TXN_CONFLICT, e.ErrorCode);
            }
            t.Commit();
            db.Find(k);
        }

        private void InsertFindAbortTest()
        {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Transaction t = env.Begin();
            db.Insert(t, k, r);
            db.Find(t, k);
            t.Abort();
            try {
                db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
        }

        private void EraseFindCommitTest()
        {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Transaction t = env.Begin();
            db.Insert(t, k, r);
            db.Find(t, k);
            try {
                db.Erase(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_TXN_CONFLICT, e.ErrorCode);
            }
            t.Commit();
            db.Erase(k);
        }

        private void CursorTest()
        {
            Transaction t = env.Begin();
            Cursor c = new Cursor(db, t);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            c.Insert(k, r);
            db.Find(t, k);
            c.Close();
            t.Commit();
            db.Find(k);
        }

        private void GetKeyCountTest()
        {
            Transaction t = env.Begin();

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Assert.AreEqual(0, db.GetCount());
            db.Insert(t, k, r);
            Assert.AreEqual(1, db.GetCount(t, 0));
            t.Commit();
            Assert.AreEqual(1, db.GetCount());
        }

        public void Run()
        {
            Console.WriteLine("TransactionTest.AbortTest");
            SetUp();
            AbortTest();
            TearDown();

            Console.WriteLine("TransactionTest.CommitTest");
            SetUp();
            CommitTest();
            TearDown();

            Console.WriteLine("TransactionTest.InsertFindAbortTest");
            SetUp();
            InsertFindAbortTest();
            TearDown();

            Console.WriteLine("TransactionTest.InsertFindCommitTest");
            SetUp();
            InsertFindCommitTest();
            TearDown();

            Console.WriteLine("TransactionTest.EraseFindCommitTest");
            SetUp();
            EraseFindCommitTest();
            TearDown();

            Console.WriteLine("TransactionTest.CursorTest"); 
            SetUp();
            CursorTest();
            TearDown();

            Console.WriteLine("TransactionTest.GetKeyCountTest"); 
            SetUp();
            GetKeyCountTest();
            TearDown();
        }
    }
}
