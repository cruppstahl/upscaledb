/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

using Upscaledb;
using System;
using Xunit;

namespace Unittests
{
    /// <summary>
    /// This is a test class for TransactionTest and is intended
    /// to contain all TransactionTest Unit Tests
    ///</summary>
    public class TransactionTest : IDisposable
    {
        private readonly Upscaledb.Environment env;
        private readonly Database db;
        
        public TransactionTest()
        {
            env = new Upscaledb.Environment();
            env.Create("test.db", UpsConst.UPS_ENABLE_TRANSACTIONS);
            db = env.CreateDatabase(1);
        }
        public void Dispose()
        {
            db.Dispose();
            env.Dispose();
        }

        [Fact]
        public void AbortTest()
        {
            Transaction t = env.Begin();
            t.Abort();
        }

        [Fact]
        public void CommitTest()
        {
            Transaction t = env.Begin();
            t.Commit();
        }

        [Fact]
        public void InsertFindCommitTest()
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
                Assert.Equal(UpsConst.UPS_TXN_CONFLICT, e.ErrorCode);
            }
            t.Commit();
            db.Find(k);
        }

        [Fact]
        public void InsertFindAbortTest()
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
                Assert.Equal(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
        }

        [Fact]
        public void EraseFindCommitTest()
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
                Assert.Equal(UpsConst.UPS_TXN_CONFLICT, e.ErrorCode);
            }
            t.Commit();
            db.Erase(k);
        }

        [Fact]
        public void CursorTest()
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

        [Fact]
        public void GetKeyCountTest()
        {
            Transaction t = env.Begin();

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Assert.Equal(0, db.GetCount());
            db.Insert(t, k, r);
            Assert.Equal(1, db.GetCount(t, 0));
            t.Commit();
            Assert.Equal(1, db.GetCount());
        }
    }
}
