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

using System;
using Upscaledb;
using Xunit;

// Trying to repro. behaviour of previous version
[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly)]

namespace Unittests
{
    public class CursorTest : IDisposable
    {
        private readonly Upscaledb.Environment env;
        private readonly Database db;

        public CursorTest()
        {
            env = new Upscaledb.Environment();
            db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1, UpsConst.UPS_ENABLE_DUPLICATE_KEYS);
        }

        public void Dispose()
        {
            db.Dispose();
            env.Dispose();
        }

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

        [Fact]
        public void Create() {
            Cursor c = new Cursor(db);
            c.Close();
        }

        [Fact]
        public void Clone() {
            Cursor c1 = new Cursor(db);
            Cursor c2 = c1.Clone();
        }

        [Fact]
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

            c.Move(UpsConst.UPS_CURSOR_NEXT);
            c.Move(UpsConst.UPS_CURSOR_NEXT);
            c.Move(UpsConst.UPS_CURSOR_PREVIOUS);
            c.Move(UpsConst.UPS_CURSOR_LAST);
            c.Move(UpsConst.UPS_CURSOR_FIRST);
        }

        [Fact]
        public void MoveNegative() {
            Cursor c = new Cursor(db);
            try {
                c.Move(UpsConst.UPS_CURSOR_NEXT);
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
        }

        [Fact]
        public void MoveFirst() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MoveFirst();
        }

        [Fact]
        public void MoveLast() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MoveLast();
        }

        [Fact]
        public void MoveNext() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MoveNext();
        }

        [Fact]
        public void MovePrevious() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MovePrevious();
        }

        private static void CheckEqual(byte[] lhs, byte[] rhs)
        {
            Assert.Equal(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.Equal(lhs[i], rhs[i]);
        }
        
        [Fact]
        public void TryMove()
        {
            Cursor c = new Cursor(db);
            byte[] k1 = BitConverter.GetBytes(1UL);
            byte[] r1 = BitConverter.GetBytes(2UL);
            db.Insert(k1, r1);
            byte[] k2 = null, r2 = null;
            Assert.True(c.TryMove(ref k2, ref r2, UpsConst.UPS_CURSOR_NEXT));
            CheckEqual(k1, k2);
            CheckEqual(r1, r2);
            Assert.False(c.TryMove(ref k2, ref r2, UpsConst.UPS_CURSOR_NEXT));
            Assert.Null(k2);
            Assert.Null(r2);
        }

        [Fact]
        public void GetKey() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MovePrevious();
            byte[] f = c.GetKey();
            CheckEqual(k, f);
        }

        [Fact]
        public void GetRecord() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            c.MovePrevious();
            byte[] f = c.GetRecord();
            CheckEqual(r, f);
        }

        [Fact]
        public void Overwrite() {
            Cursor c = new Cursor(db);
            byte[] k = new byte[5];
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            db.Insert(k, r1);
            c.MoveFirst();
            byte[] f = c.GetRecord();
            CheckEqual(r1, f);
            c.Overwrite(r2);
            byte[] g = c.GetRecord();
            CheckEqual(r2, g);
        }

        [Fact]
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
            CheckEqual(r1, f);
            c.Find(k2);
            byte[] g = c.GetRecord();
            CheckEqual(r2, g);
        }

        [Fact]
        public void TryFind()
        {
            Cursor c = new Cursor(db);
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] k2 = new byte[5]; k2[0] = 6;
            byte[] k3 = new byte[5]; k3[0] = 7;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            db.Insert(k1, r1);
            db.Insert(k2, r2);
            var f = c.TryFind(k1);
            CheckEqual(r1, f);
            var g = c.TryFind(k2);
            CheckEqual(r2, g);
            var h = c.TryFind(k3);
            Assert.Null(h);
        }

        [Fact]
        public void Insert() {
            Cursor c = new Cursor(db);
            byte[] q;
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] k2 = new byte[5]; k2[0] = 6;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            q = c.GetRecord();
            CheckEqual(r1, q);
            q = c.GetKey();
            CheckEqual(k1, q);

            c.Insert(k2, r2);
            q = c.GetRecord();
            CheckEqual(r2, q);
            q = c.GetKey();
            CheckEqual(k2, q);
        }

        [Fact]
        public void InsertDuplicate() {
            Cursor c = new Cursor(db);
            byte[] q;
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            q = c.GetRecord();
            CheckEqual(r1, q);
            q = c.GetKey();
            CheckEqual(k1, q);

            c.Insert(k1, r2, UpsConst.UPS_DUPLICATE);
            q = c.GetRecord();
            CheckEqual(r2, q);
            q = c.GetKey();
            CheckEqual(k1, q);
        }

        [Fact]
        public void InsertNegative() {
            Cursor c = new Cursor(db);
            byte[] q;
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            q = c.GetRecord();
            CheckEqual(r1, q);
            q = c.GetKey();
            CheckEqual(k1, q);

            try {
                c.Insert(k1, r2);
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_DUPLICATE_KEY, e.ErrorCode);
            }
        }

        [Fact]
        public void Erase() {
            Cursor c = new Cursor(db);
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            c.Insert(k1, r1);
            c.Erase();
        }

        [Fact]
        public void EraseNegative() {
            Cursor c = new Cursor(db);
            try {
                c.Erase();
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_CURSOR_IS_NIL, e.ErrorCode);
            }
        }

        [Fact]
        public void GetDuplicateCount() {
            Cursor c = new Cursor(db);
            byte[] k1 = new byte[5]; k1[0] = 5;
            byte[] r1 = new byte[5]; r1[0] = 1;
            byte[] r2 = new byte[5]; r2[0] = 2;
            byte[] r3 = new byte[5]; r2[0] = 2;
            c.Insert(k1, r1);
            Assert.Equal(1, c.GetDuplicateCount());

            c.Insert(k1, r2, UpsConst.UPS_DUPLICATE);
            Assert.Equal(2, c.GetDuplicateCount());

            c.Insert(k1, r3, UpsConst.UPS_DUPLICATE);
            Assert.Equal(3, c.GetDuplicateCount());

            c.Erase();
            c.MoveFirst();
            Assert.Equal(2, c.GetDuplicateCount());
        }

        //[Fact] This was never run as a test in the previous version
        internal void ApproxMatching()
        {
            Upscaledb.Environment env = new Upscaledb.Environment();
            byte[] k1 = new byte[5];
            byte[] r1 = new byte[5];
            k1[0] = 1; r1[0] = 1;
            byte[] k2 = new byte[5];
            byte[] r2 = new byte[5];
            k2[0] = 2; r2[0] = 2;
            byte[] k3 = new byte[5];
            byte[] r3 = new byte[5];
            k3[0] = 3; r3[0] = 3;
            try
            {
                env.Create("ntest.db");
                var db = env.CreateDatabase(1);
                db.Insert(k1, r1);
                db.Insert(k2, r2);
                db.Insert(k3, r3);

                Cursor c = new Cursor(db);
                byte[] r = c.Find(k2, UpsConst.UPS_FIND_GT_MATCH);
                CheckEqual(r, r3);
                CheckEqual(k2, k3);
                k2[0] = 2;
                r = c.Find(k2, UpsConst.UPS_FIND_GT_MATCH);
                CheckEqual(r, r1);
                CheckEqual(k2, k1);
                db.Close();
                env.Close();
            }
            catch (DatabaseException)
            {
                //unexpected exception
                Assert.False(true);
            }
        }
    }
}
