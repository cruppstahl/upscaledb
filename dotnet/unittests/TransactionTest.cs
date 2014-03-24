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

using Hamster;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Unittests
{
    /// <summary>
    ///This is a test class for TransactionTest and is intended
    ///to contain all TransactionTest Unit Tests
    ///</summary>
    [TestClass()]
    [DeploymentItem("..\\win32\\msvc2008\\out\\dll_debug\\hamsterdb-2.1.7.dll")]
    public class TransactionTest
    {
        private Hamster.Environment env;
        private Database db;

        [TestInitialize()]
        public void MyTestInitialize()
        {
            env = new Hamster.Environment();
            env.Create("test.db", HamConst.HAM_ENABLE_TRANSACTIONS);
            db = env.CreateDatabase(1);
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            db.Close();
            env.Close();
        }

        [DeploymentItem("HamsterDb-dotnet.dll")]

        [TestMethod()]
        public void AbortTest()
        {
            Transaction t = env.Begin();
            t.Abort();
        }

        [TestMethod()]
        public void CommitTest()
        {
            Transaction t = env.Begin();
            t.Commit();
        }

        [TestMethod()]
        public void InsertFindCommitTest() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Transaction t=env.Begin();
            db.Insert(t, k, r);
            db.Find(t, k);
            try {
                db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_TXN_CONFLICT, e.ErrorCode);
            }
            t.Commit();
            db.Find(k);
        }

        [TestMethod()]
        public void InsertFindAbortTest() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Transaction t=env.Begin();
            db.Insert(t, k, r);
            db.Find(t, k);
            t.Abort();
            try {
                db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
        }

        [TestMethod()]
        public void EraseFindCommitTest() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Transaction t=env.Begin();
            db.Insert(t, k, r);
            db.Find(t, k);
            try {
                db.Erase(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_TXN_CONFLICT, e.ErrorCode);
            }
            t.Commit();
            db.Erase(k);
        }

        [TestMethod()]
        public void CursorTest() {
            Transaction t=env.Begin();
            Cursor c = new Cursor(db, t);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            c.Insert(k, r);
            db.Find(t, k);
            c.Close();
            t.Commit();
            db.Find(k);
        }

        [TestMethod()]
        public void GetKeyCountTest() {
            Transaction t=env.Begin();

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Assert.AreEqual(0, db.GetKeyCount());
            db.Insert(t, k, r);
            Assert.AreEqual(1, db.GetKeyCount(t));
            t.Commit();
            Assert.AreEqual(1, db.GetKeyCount());
        }
    }
}
