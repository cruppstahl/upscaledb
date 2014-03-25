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
    public class CursorTest
    {
        private int counter;

        private int Callback(byte[] b1, byte[] b2) {
            counter++;
            if (b1.GetLength(0) < b2.GetLength(0))
                return (-1);
            if (b1.GetLength(0) > b2.GetLength(0))
                return (+1);
            for (int i = b1.GetLength(0); --i >= 0; ) {
                if (b1[i] < b2[i])
                    return (-1);
                if (b1[i] > b2[i])
                    return (+1);
            }
            return 0;
        }

        private Hamster.Environment env;
        private Database db;

        [TestInitialize()]
        public void SetUp() {
            env = new Hamster.Environment();
            db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1, HamConst.HAM_ENABLE_DUPLICATE_KEYS);
        }

        [TestCleanup()]
        public void TearDown() {
            env.Close(HamConst.HAM_AUTO_CLEANUP);
        }

        [TestMethod()]
        public void Create() {
            Cursor c = new Cursor(db);
            c.Close();
        }

        [TestMethod()]
        public void Clone() {
            Cursor c1 = new Cursor(db);
            Cursor c2 = c1.Clone();
        }

        [TestMethod()]
        public void Move() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            k[0] = 0;
            db.Insert(k, r);
            k[0] = 1;
            db.Insert(k, r);
            k[0] = 2;
            db.Insert(k, r);
            k[0] = 3;
            db.Insert(k, r);
            k[0] = 4;
            db.Insert(k, r);

            c.Move(HamConst.HAM_CURSOR_NEXT);
            c.Move(HamConst.HAM_CURSOR_NEXT);
            c.Move(HamConst.HAM_CURSOR_PREVIOUS);
            c.Move(HamConst.HAM_CURSOR_LAST);
            c.Move(HamConst.HAM_CURSOR_FIRST);
        }

        [TestMethod()]
        public void MoveNegative() {
            Cursor c = new Cursor(db);
            try {
                c.Move(HamConst.HAM_CURSOR_NEXT);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
        }

        [TestMethod()]
        public void MoveFirst() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MoveFirst();
        }

        [TestMethod()]
        public void MoveLast() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MoveLast();
        }

        [TestMethod()]
        public void MoveNext() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MoveNext();
        }

        [TestMethod()]
        public void MovePrevious() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MovePrevious();
        }

        void checkEqual(byte[] lhs, byte[] rhs)
        {
            Assert.AreEqual(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.AreEqual(lhs[i], rhs[i]);
        }

        [TestMethod()]
        public void GetKey() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MovePrevious();
            byte[] f = c.GetKey();
            checkEqual(k, f);
        }

        [TestMethod()]
        public void GetRecord() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MovePrevious();
            byte[] f = c.GetRecord();
            checkEqual(r, f);
        }

        [TestMethod()]
        public void Overwrite() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            db.Insert(k, r1);
            c.MoveFirst();
            byte[] f = c.GetRecord();
            checkEqual(r1, f);
            c.Overwrite(r2);
            byte[] g = c.GetRecord();
            checkEqual(r2, g);
        }

        [TestMethod()]
        public void Find() {
            Cursor c = new Cursor(db);
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] k2 = new byte[5]; k2[0] = 6;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            db.Insert(k1, r1);
            db.Insert(k2, r2);
            c.Find(k1);
            byte[] f = c.GetRecord();
            checkEqual(r1, f);
            c.Find(k2);
            byte[] g = c.GetRecord();
            checkEqual(r2, g);
        }

        [TestMethod()]
        public void Insert() {
            Cursor c = new Cursor(db);
            byte[] q;
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] k2 = new byte[5]; k2[0] = 6;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            q = c.GetRecord();
            checkEqual(r1, q);
            q = c.GetKey();
            checkEqual(k1, q);

            c.Insert(k2, r2);
            q = c.GetRecord();
            checkEqual(r2, q);
            q = c.GetKey();
            checkEqual(k2, q);
        }

        [TestMethod()]
        public void InsertDuplicate() {
            Cursor c = new Cursor(db);
            byte[] q;
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            q = c.GetRecord();
            checkEqual(r1, q);
            q = c.GetKey();
            checkEqual(k1, q);

            c.Insert(k1, r2, HamConst.HAM_DUPLICATE);
            q = c.GetRecord();
            checkEqual(r2, q);
            q = c.GetKey();
            checkEqual(k1, q);
        }

        [TestMethod()]
        public void InsertNegative() {
            Cursor c = new Cursor(db);
            byte[] q;
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            q = c.GetRecord();
            checkEqual(r1, q);
            q = c.GetKey();
            checkEqual(k1, q);

            try {
                c.Insert(k1, r2);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_DUPLICATE_KEY, e.ErrorCode);
            }
        }

        [TestMethod()]
        public void Erase() {
            Cursor c = new Cursor(db);
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            c.Insert(k1, r1);
            c.Erase();
        }

        [TestMethod()]
        public void EraseNegative() {
            Cursor c = new Cursor(db);
            try {
                c.Erase();
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_CURSOR_IS_NIL, e.ErrorCode);
            }
        }

        [TestMethod()]
        public void GetDuplicateCount() {
            Cursor c = new Cursor(db);
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            byte[] r3 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            Assert.AreEqual(1, c.GetDuplicateCount());

            c.Insert(k1, r2, HamConst.HAM_DUPLICATE);
            Assert.AreEqual(2, c.GetDuplicateCount());

            c.Insert(k1, r3, HamConst.HAM_DUPLICATE);
            Assert.AreEqual(3, c.GetDuplicateCount());

            c.Erase();
            c.MoveFirst();
            Assert.AreEqual(2, c.GetDuplicateCount());
        }
    }
}
