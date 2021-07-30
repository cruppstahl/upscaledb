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
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Upscaledb;
using Xunit;

namespace Unittests
{
    public class DatabaseTest : IDisposable
    {
        private readonly Upscaledb.Environment env;

        public DatabaseTest()
        {
            env = new Upscaledb.Environment();
        }
        public void Dispose()
        {
            env.Dispose();
        }

        private static int errorCounter = 0;

        static void MyErrorHandler(int level, String message) {
            Console.WriteLine("ErrorHandler: Level " + level + ", msg: " + message);
            errorCounter++;
        }

        [Fact]
        public void SetErrorHandler() {
            ErrorHandler eh = new ErrorHandler(MyErrorHandler);
            try
            {
                Database.SetErrorHandler(eh);
                env.Create(null);
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
                Assert.Equal(1, errorCounter);
            }
            finally
            {
                Database.SetErrorHandler(null);
            }
        }

        [Fact]
        public void CreateWithParameters()
        {
            env.Create("ntest.db");

            Parameter[] param = new Parameter[] {
                new Parameter {
                    name = UpsConst.UPS_PARAM_KEYSIZE, value = 32
                }
            };
            using (Database db = env.CreateDatabase(13, 0, param)) { }
        }

        [Fact]
        public void CreateWithParameters2()
        {
            env.Create("ntest.db");
            using (Database db = env.CreateDatabase(13, 0,
                        new Parameter[0])) { }
        }

        [Fact]
        public void GetVersion() {
            Upscaledb.Version v = Database.GetVersion();
            Assert.Equal(2, v.major);
            Assert.Equal(2, v.minor);
        }

        [Fact]
        public void DatabaseClose() {
            Database db = new Database();
            try {
                db.Close();
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateString() {
            try {
                env.Create("ntest.db");
                var db = env.CreateDatabase(1);
                db.Close();
                env.Close();
                env.Open("ntest.db");
                db = env.OpenDatabase(1);
                db.Close();
            }
            catch (DatabaseException) {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateInvalidParameter() {
            Parameter[] param = new Parameter[3];
            param[1] = new Parameter();
            param[2] = new Parameter();
            try {
                env.Create("ntest.db");
                var db = env.CreateDatabase(1, 0, param);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
            }
        }

        [Fact]
        public void CreateStringIntIntParameter() {
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_CACHESIZE,
                value = 1024
            };
            try {
                env.Create("ntest.db", 0, 0644, param);
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateStringIntIntParameterNeg() {
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_CACHESIZE,
                value = 1024
            };
            try {
                env.Create("ntest.db", UpsConst.UPS_IN_MEMORY, 0644, param);
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
            }
        }

        private int compareCounter;

        private int MyCompareFunc(IntPtr handle,
                    IntPtr lhs, int lhsLength,
                    IntPtr rhs, int rhsLength) {
            byte[] alhs = new byte[lhsLength];
            byte[] arhs = new byte[rhsLength];
            Marshal.Copy(lhs, alhs, 0, lhsLength);
            Marshal.Copy(rhs, arhs, 0, rhsLength);
            // always return a different value or upscaledb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        
        [Fact]
        public void SetComparator1() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_KEY_TYPE,
                value = UpsConst.UPS_TYPE_CUSTOM
            };

            compareCounter = 0;
            try {
                env.Create("ntest.db");
                var db = env.CreateDatabase(1, 0, param);
                db.SetCompareFunc(new Upscaledb.CompareFunc(MyCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                k[0] = 2;
                db.Insert(k, r);
                db.Close();
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
            Assert.Equal(2, compareCounter);
        }

        [Fact]
        public void SetComparator2()
        {
            Upscaledb.Database.RegisterCompare("cmp", MyCompareFunc);
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_KEY_TYPE,
                value = UpsConst.UPS_TYPE_CUSTOM
            };

            compareCounter = 0;
            try
            {
                env.Create("ntest.db");
                var db = env.CreateDatabase(1, 0, param);
                db.SetCompareFunc(new CompareFunc(MyCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                k[0] = 2;
                db.Insert(k, r);
                db.Close();
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
            Assert.Equal(2, compareCounter);
        }
        private static void CheckEqual(byte[] lhs, byte[] rhs)
        {
            Assert.Equal(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.Equal(lhs[i], rhs[i]);
        }

        [Fact]
        public void FindKey() {
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            r1[0] = 1;
            try {
                env.Create("ntest.db");
                var db = env.CreateDatabase(1);
                db.Insert(k, r1);
                byte[] r2 = db.Find(k);
                CheckEqual(r1, r2);
                db.Close();
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void FindKeyNull() {
            try {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1))
                {
                    byte[] r = db.Find(null);
                }
            }
            catch (NullReferenceException) {
            }
        }

        [Fact]
        public void FindUnknownKey() {
            byte[] k = new byte[5];
            try {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1))
                {
                    byte[] r = db.Find(k);
                }
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
        }

        [Fact]
        public void InsertKey() {
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            byte[] r2;
            try {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1))
                {
                    k[0] = 1;
                    r1[0] = 1;
                    db.Insert(k, r1);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);

                    k[0] = 2;
                    r1[0] = 2;
                    db.Insert(k, r1);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);

                    k[0] = 3;
                    r1[0] = 3;
                    db.Insert(k, r1);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);
                }
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void BulkOperations()
        {
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            byte[] r2;
            try
            {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1))
                {
                    k[0] = 1;
                    r1[0] = 1;

                    ///////// Insert and find
                    var op0 = new Operation { OperationType = OperationType.Insert, Key = k, Record = r1 };
                    var op1 = new Operation { OperationType = OperationType.Find, Key = k };
                    var ops = new Operation[] { op0, op1 };

                    db.BulkOperations(ops);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);
                    CheckEqual(r1, ops[1].Record);

                    ///////// Partial lookups
                    k[0] = 2;
                    ops = new Operation[] { new Operation { OperationType = OperationType.Find, Key = k, Flags = UpsConst.UPS_FIND_LT_MATCH } };
                    db.BulkOperations(ops);
                    CheckEqual(r1, ops[0].Key); // since inserted key and record are identical
                    CheckEqual(r1, ops[0].Record);

                    k[0] = 0;
                    ops = new Operation[] { new Operation { OperationType = OperationType.Find, Key = k, Flags = UpsConst.UPS_FIND_GT_MATCH } };
                    db.BulkOperations(ops);
                    CheckEqual(r1, ops[0].Key); // since inserted key and record are identical
                    CheckEqual(r1, ops[0].Record);

                    ///////// Erase
                    // first verify lookup
                    k[0] = 1;
                    using (var c = new Cursor(db))
                    {
                        r2 = c.TryFind(k);
                        CheckEqual(r1, r2);

                        // erase
                        ops = new Operation[] { new Operation { OperationType = OperationType.Erase, Key = k } };
                        db.BulkOperations(ops);

                        // check is erased
                        r2 = c.TryFind(k);
                        Assert.Null(r2);
                    }
                }
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void InsertRecNo()
        {
            byte[] r1 = new byte[5];
            byte[] r2;
            try
            {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1, UpsConst.UPS_RECORD_NUMBER))
                {
                    r1[0] = 1;
                    var k = db.InsertRecNo(r1);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);

                    r1[0] = 2;
                    k = db.InsertRecNo(r1);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);

                    r1[0] = 3;
                    k = db.InsertRecNo(r1);
                    r2 = db.Find(k);
                    CheckEqual(r1, r2);
                }
            }
            catch (DatabaseException)
            {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void InsertKeyInvalidParam() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            env.Create("ntest.db");
            using (var db = env.CreateDatabase(1))
            {
                try
                {
                    db.Insert(null, r);
                }
                catch (NullReferenceException)
                {
                }
                try
                {
                    db.Insert(k, null);
                }
                catch (NullReferenceException)
                {
                }
                try
                {
                    db.Insert(k, r, 9999);
                }
                catch (DatabaseException e)
                {
                    Assert.Equal(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
                }
            }
        }

        [Fact]
        public void InsertKeyNegative()
        {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1))
                {
                    db.Insert(k, r);
                    db.Insert(k, r);
                }
            }
            catch (DatabaseException e) {
                Assert.Equal(UpsConst.UPS_DUPLICATE_KEY, e.ErrorCode);
            }
        }

        [Fact]
        public void InsertKeyOverwrite() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase(1))
                {
                    db.Insert(k, r);
                    r[0] = 1;
                    db.Insert(k, r, UpsConst.UPS_OVERWRITE);
                    byte[] r2 = db.Find(k);
                    CheckEqual(r, r2);
                }
            }
            catch (DatabaseException) {
                // unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void EraseKey() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            env.Create("ntest.db");
            using (var db = env.CreateDatabase(1))
            {
                db.Insert(k, r);
                byte[] r2 = db.Find(k);
                CheckEqual(r, r2);
                db.Erase(k);

                try
                {
                    r2 = db.Find(k);
                }
                catch (DatabaseException e)
                {
                    Assert.Equal(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
                }
            }
        }

        [Fact]
        public void EraseKeyNegative() {
            byte[] k = new byte[5];
            env.Create("ntest.db");
            using (var db = env.CreateDatabase(1))
            {
                try
                {
                    db.Erase(null);
                }
                catch (NullReferenceException)
                {
                }
            }
        }

        [Fact]
        public void EraseUnknownKey() {
            byte[] k = new byte[5];
            env.Create("ntest.db");
            using (var db = env.CreateDatabase(1))
            {
                try
                {
                    db.Erase(k);
                }
                catch (DatabaseException e)
                {
                    Assert.Equal(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
                }
            }
        }

        [Fact]
        public void EraseKeyTwice() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            env.Create("ntest.db");
            using (var db = env.CreateDatabase(1))
            {
                db.Insert(k, r);
                byte[] r2 = db.Find(k);
                CheckEqual(r, r2);
                db.Erase(k);

                try
                {
                    db.Erase(k);
                }
                catch (DatabaseException e)
                {
                    Assert.Equal(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
                }
            }
        }

        [Fact]
        public void Transactions() {
            env.Create("ntest.db", UpsConst.UPS_ENABLE_TRANSACTIONS);
            using (var db = env.CreateDatabase(1))
            {

                byte[] k = new byte[5];
                byte[] r = new byte[5];
                db.Insert(k, r);
            }
        }

        [Fact]
        public void GetKeyCount()
        {
            env.Create("ntest.db");
            using (var db = env.CreateDatabase(1))
            {
                byte[] k = new byte[5];
                byte[] r = new byte[5];
                Assert.Equal(0, db.GetCount());
                db.Insert(k, r);
                Assert.Equal(1, db.GetCount());
                k[0] = 1;
                db.Insert(k, r);
                Assert.Equal(2, db.GetCount());
            }
        }

        private int NumericalCompareFunc(IntPtr handle,
                    IntPtr lhs, int lhsLength,
                    IntPtr rhs, int rhsLength)
        {
            byte[] alhs = new byte[lhsLength];
            byte[] arhs = new byte[rhsLength];
            Marshal.Copy(lhs, alhs, 0, lhsLength);
            Marshal.Copy(rhs, arhs, 0, rhsLength);
            // translate buffers to two numbers and compare them
            ulong ulhs = BitConverter.ToUInt64(alhs, 0);
            ulong urhs = BitConverter.ToUInt64(arhs, 0);
            if (ulhs < urhs) return -1;
            if (ulhs > urhs) return +1;
            return 0;
        }


        private Database CreateDatabase(string file)
        {
            List<Parameter> list = new List<Parameter>();

            Parameter param1 = new Parameter
            {
                name = UpsConst.UPS_PARAM_CACHESIZE,
                value = 768 * 1024 * 1024
            };
            list.Add(param1);

            Parameter param2 = new Parameter
            {
                name = UpsConst.UPS_PARAM_KEYSIZE,
                value = 8 // sizeof(ulong);
            };
            list.Add(param2);

            env.Create(file, 0, 0, list.ToArray());
            var db = env.CreateDatabase(1);
            db.SetCompareFunc(new Upscaledb.CompareFunc(NumericalCompareFunc));
            return db;
        }

        private Database OpenDatabase(string file)
        {
            List<Parameter> list = new List<Parameter>();

            Parameter param1 = new Parameter
            {
                name = UpsConst.UPS_PARAM_CACHESIZE,
                value = 768 * 1024 * 1024
            };
            list.Add(param1);

            Upscaledb.Environment env = new Upscaledb.Environment(); 
            Database db = new Database();
            env.Open(file, 0, list.ToArray());
            db = env.OpenDatabase(1);
            db.SetCompareFunc(new Upscaledb.CompareFunc(NumericalCompareFunc));
            return db;
        }

        [Fact]
        public void Cursor10000Test()
        {
            //create database
            env.Create("ntest.db");

            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_KEY_TYPE,
                value = UpsConst.UPS_TYPE_UINT64
            };
            using (var db = env.CreateDatabase(1, 0, param))
            {

                //insert records
                for (ulong i = 0; i < 10000; i++)
                {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] record = new byte[20];
                    db.Insert(key, record);
                }
            }

            //reopen again
            using (var db = env.OpenDatabase(1))
            {
                using (var cursor = new Cursor(db))
                {
                    cursor.MoveFirst();
                    ulong firstKey = BitConverter.ToUInt64(cursor.GetKey(), 0);
                    Assert.Equal((ulong)0, firstKey);

                    cursor.MoveLast();
                    ulong lastKey = BitConverter.ToUInt64(cursor.GetKey(), 0);
                    Assert.Equal((ulong)9999, lastKey);
                }
            }
        }

        [Fact]
        public void AutoCleanupCursors()
        {
            env.Create("ntest.db");
            var db = env.CreateDatabase(1);
            var cursor = new Cursor(db);
            // let gc do the cleanup
            env.Close();
        }

        [Fact]
        public void AutoCleanupCursors2()
        {
            env.Create("ntest.db");
            var db = env.CreateDatabase(1);
            var cursor1 = new Cursor(db);
            var cursor2 = new Cursor(db);
            var cursor3 = new Cursor(db);
            var cursor4 = new Cursor(db);
            var cursor5 = new Cursor(db);
            // let gc do the cleanup
            env.Close();
        }

        [Fact]
        public void AutoCleanupCursors3()
        {
            env.Create("ntest.db");
            var db = env.CreateDatabase(1);
            var cursor1 = new Cursor(db);
            var cursor2 = new Cursor(db);
            var cursor3 = new Cursor(db);
            var cursor4 = new Cursor(db);
            var cursor5 = new Cursor(db);
            cursor3.Close();
            cursor5.Close();
            // let gc do the cleanup
            env.Close();
        }

        [Fact]
        public void AutoCleanupCursors4()
        {
            env.Create("ntest.db");
            var db = env.CreateDatabase(1);
            var cursor1 = new Cursor(db);
            var cursor2 = cursor1.Clone();
            var cursor3 = cursor1.Clone();
            var cursor4 = cursor1.Clone();
            var cursor5 = cursor1.Clone();
            cursor3.Close();
            cursor5.Close();
            // let gc do the cleanup
            env.Close();
        }

        [Fact]
        public void ApproxMatching()
        {
            using (var env = new Upscaledb.Environment())
            {
                Database db = new Database();
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
                    db = env.CreateDatabase(1);
                    db.Insert(k1, r1);
                    db.Insert(k2, r2);
                    db.Insert(k3, r3);
                    byte[] r = db.Find(null, ref k2, UpsConst.UPS_FIND_GT_MATCH);
                    CheckEqual(r, r3);
                    CheckEqual(k2, k3);
                    k2[0] = 2;
                    r = db.Find(null, ref k2, UpsConst.UPS_FIND_LT_MATCH);
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
}
