/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Hamster;

namespace Unittests
{
    public class DatabaseTest
    {
        private static int errorCounter;

        static void MyErrorHandler(int level, String message) {
            Console.WriteLine("ErrorHandler: Level " + level + ", msg: " + message);
            errorCounter++;
        }

        private void SetErrorHandler() {
            Hamster.Environment env = new Hamster.Environment();
            ErrorHandler eh = new ErrorHandler(MyErrorHandler);
            try {
                Database.SetErrorHandler(eh);
                env.Create(null);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
                Assert.AreEqual(1, errorCounter);
            }
            Database.SetErrorHandler(null);
        }

        private void CreateWithParameters()
        {
            using (Hamster.Environment env = new Hamster.Environment())
            {
                env.Create("ntest.db");

                Parameter[] param = new Parameter[] {
                    new Parameter {
                        name = HamConst.HAM_PARAM_KEYSIZE, value = 32
                    }
                };
                using (Database db = env.CreateDatabase(13, 0, param)) { }
            }
        }

        private void CreateWithParameters2()
        {
            using (Hamster.Environment env = new Hamster.Environment())
            {
                env.Create("ntest.db");
                using (Database db = env.CreateDatabase(13, 0,
                           new Parameter[0])) { }
            }
        }

        private void GetVersion() {
            Hamster.Version v = Database.GetVersion();
            Assert.AreEqual(2, v.major);
            Assert.AreEqual(1, v.minor);
        }

        private void DatabaseClose() {
            Database db = new Database();
            try {
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("Unexpected exception " + e);
            }
        }

        private void CreateString() {
            Database db = new Database();
            Hamster.Environment env = new Hamster.Environment();
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                db.Close();
                env.Close();
                env.Open("ntest.db");
                db = env.OpenDatabase(1);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("Unexpected exception " + e);
            }
        }

        private void CreateInvalidParameter() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            Parameter[] param = new Parameter[3];
            param[1] = new Parameter();
            param[2] = new Parameter();
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1, 0, param);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
        }

        private void CreateStringIntIntParameter() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = HamConst.HAM_PARAM_CACHESIZE;
            param[0].value = 1024;
            try {
                env.Create("ntest.db", 0, 0644, param);
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("Unexpected exception " + e);
            }
        }

        private void CreateStringIntIntParameterNeg() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = HamConst.HAM_PARAM_CACHESIZE;
            param[0].value = 1024;
            try {
                env.Create("ntest.db", HamConst.HAM_IN_MEMORY, 0644, param);
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
        }

        private int compareCounter;

        private int MyCompareFunc(byte[] lhs, byte[] rhs) {
            // always return a different value or hamsterdb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        private void SetComparator() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = HamConst.HAM_PARAM_KEY_TYPE;
            param[0].value = HamConst.HAM_TYPE_CUSTOM;

            compareCounter = 0;
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1, 0, param);
                db.SetCompareFunc(new CompareFunc(MyCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
            Assert.AreEqual(1, compareCounter);
        }

        void checkEqual(byte[] lhs, byte[] rhs)
        {
            Assert.AreEqual(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.AreEqual(lhs[i], rhs[i]);
        }

        private void FindKey() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            r1[0] = 1;
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                db.Insert(k, r1);
                byte[] r2 = db.Find(k);
                checkEqual(r1, r2);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
        }

        private void FindKeyNull() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                byte[] r = db.Find(null);
            }
            catch (NullReferenceException) {
            }
            db.Close();
            env.Close();
        }

        private void FindUnknownKey() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                byte[] r = db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void InsertKey() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            byte[] r2;
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                k[0] = 1;
                r1[0] = 1;
                db.Insert(k, r1);
                r2 = db.Find(k);
                checkEqual(r1, r2);

                k[0] = 2;
                r1[0] = 2;
                db.Insert(k, r1);
                r2 = db.Find(k);
                checkEqual(r1, r2);

                k[0] = 3;
                r1[0] = 3;
                db.Insert(k, r1);
                r2 = db.Find(k);
                checkEqual(r1, r2);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
        }

        private void InsertRecNo()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] r1 = new byte[5];
            byte[] r2;
            try
            {
                env.Create("ntest.db");
                db = env.CreateDatabase(1, HamConst.HAM_RECORD_NUMBER);
                r1[0] = 1;
                var k = db.InsertRecNo(r1);
                r2 = db.Find(k);
                checkEqual(r1, r2);

                r1[0] = 2;
                k = db.InsertRecNo(r1);
                r2 = db.Find(k);
                checkEqual(r1, r2);

                r1[0] = 3;
                k = db.InsertRecNo(r1);
                r2 = db.Find(k);
                checkEqual(r1, r2);
            }
            catch (DatabaseException e)
            {
                Assert.Fail("unexpected exception " + e);
            }
            db.Close();
            env.Close();
        }

        private void InsertKeyInvalidParam() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            try {
                db.Insert(null, r);
            }
            catch (NullReferenceException) {
            }
            try {
                db.Insert(k, null);
            }
            catch (NullReferenceException) {
            }
            try {
                db.Insert(k, r, 9999);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void InsertKeyNegative() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                db.Insert(k, r);
                db.Insert(k, r);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_DUPLICATE_KEY, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void InsertKeyOverwrite() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                db.Insert(k, r);
                r[0] = 1;
                db.Insert(k, r, HamConst.HAM_OVERWRITE);
                byte[] r2 = db.Find(k);
                checkEqual(r, r2);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
        }

        private void EraseKey() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            db.Insert(k, r);
            byte[] r2 = db.Find(k);
            checkEqual(r, r2);
            db.Erase(k);

            try {
                r2 = db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void EraseKeyNegative() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            try {
                db.Erase(null);
            }
            catch (NullReferenceException) {
            }
            db.Close();
            env.Close();
        }

        private void EraseUnknownKey() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            try {
                db.Erase(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void EraseKeyTwice() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            db.Insert(k, r);
            byte[] r2 = db.Find(k);
            checkEqual(r, r2);
            db.Erase(k);

            try {
                db.Erase(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void Recovery() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db", HamConst.HAM_ENABLE_RECOVERY);
            db = env.CreateDatabase(1);

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            db.Close();
            env.Close();
        }

        private void GetKeyCount()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Assert.AreEqual(0, db.GetKeyCount());
            db.Insert(k, r);
            Assert.AreEqual(1, db.GetKeyCount());
            k[0] = 1;
            db.Insert(k, r);
            Assert.AreEqual(2, db.GetKeyCount());
            db.Close();
            env.Close();
        }

        private int NumericalCompareFunc(byte[] lhs, byte[] rhs)
        {
            // translate buffers to two numbers and compare them
            ulong ulhs = BitConverter.ToUInt64(lhs, 0);
            ulong urhs = BitConverter.ToUInt64(rhs, 0);
            if (ulhs < urhs) return -1;
            if (ulhs > urhs) return +1;
            return 0;
        }

        private Database CreateDatabase(string file)
        {
            List<Parameter> list = new List<Parameter>();

            Parameter param1 = new Parameter();
            param1.name = HamConst.HAM_PARAM_CACHESIZE;
            param1.value = 768 * 1024 * 1024;
            list.Add(param1);

            Parameter param2 = new Parameter();
            param2.name = HamConst.HAM_PARAM_KEYSIZE;
            param2.value = 8; // sizeof(ulong);
            list.Add(param2);

            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create(file, 0, 0, list.ToArray());
            db = env.CreateDatabase(1);
            db.SetCompareFunc(new CompareFunc(NumericalCompareFunc));
            return db;
        }

        private Database OpenDatabase(string file)
        {
            List<Parameter> list = new List<Parameter>();

            Parameter param1 = new Parameter();
            param1.name = HamConst.HAM_PARAM_CACHESIZE;
            param1.value = 768 * 1024 * 1024;
            list.Add(param1);

            Hamster.Environment env = new Hamster.Environment(); 
            Database db = new Database();
            env.Open(file, 0, list.ToArray());
            db = env.OpenDatabase(1);
            db.SetCompareFunc(new CompareFunc(NumericalCompareFunc));
            return db;
        }

        private void Cursor10000Test()
        {
            //create database
            Hamster.Environment env = new Hamster.Environment();
            env.Create("ntest.db");

            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = HamConst.HAM_PARAM_KEY_TYPE;
            param[0].value = HamConst.HAM_TYPE_UINT64;
            Database db = env.CreateDatabase(1, 0, param);

            //insert records
            for (ulong i = 0; i < 10000; i++)
            {
                byte[] key = BitConverter.GetBytes(i);
                byte[] record = new byte[20];
                db.Insert(key, record);
            }

            //close database
            db.Close();

            //reopen again
            db = env.OpenDatabase(1);
            Cursor cursor = new Cursor(db);

            cursor.MoveFirst();
            ulong firstKey = BitConverter.ToUInt64(cursor.GetKey(), 0);
            Assert.AreEqual((ulong)0, firstKey);

            cursor.MoveLast();
            ulong lastKey = BitConverter.ToUInt64(cursor.GetKey(), 0);
            Assert.AreEqual((ulong)9999, lastKey);

            //close database
            cursor.Close();
            db.Close();
            env.Close();
        }

        private void AutoCleanupCursors()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            Cursor cursor = new Cursor(db);
            // let gc do the cleanup
            env.Close();
        }

        private void AutoCleanupCursors2()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            Cursor cursor1 = new Cursor(db);
            Cursor cursor2 = new Cursor(db);
            Cursor cursor3 = new Cursor(db);
            Cursor cursor4 = new Cursor(db);
            Cursor cursor5 = new Cursor(db);
            // let gc do the cleanup
            env.Close();
        }

        private void AutoCleanupCursors3()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            Cursor cursor1 = new Cursor(db);
            Cursor cursor2 = new Cursor(db);
            Cursor cursor3 = new Cursor(db);
            Cursor cursor4 = new Cursor(db);
            Cursor cursor5 = new Cursor(db);
            cursor3.Close();
            cursor5.Close();
            // let gc do the cleanup
            env.Close();
        }

        private void AutoCleanupCursors4()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            Cursor cursor1 = new Cursor(db);
            Cursor cursor2 = cursor1.Clone();
            Cursor cursor3 = cursor1.Clone();
            Cursor cursor4 = cursor1.Clone();
            Cursor cursor5 = cursor1.Clone();
            cursor3.Close();
            cursor5.Close();
            // let gc do the cleanup
            env.Close();
        }

        private void ApproxMatching()
        {
            Hamster.Environment env = new Hamster.Environment();
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
                byte[] r = db.Find(null, ref k2, HamConst.HAM_FIND_GT_MATCH);
                checkEqual(r, r3);
                checkEqual(k2, k3);
                k2[0] = 2;
                r = db.Find(null, ref k2, HamConst.HAM_FIND_LT_MATCH);
                checkEqual(r, r1);
                checkEqual(k2, k1);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e)
            {
                Assert.Fail("unexpected exception " + e);
            }
        }

        public void Run()
        {
            Console.WriteLine("DatabaseTest.InsertRecNo");
            InsertRecNo();

            Console.WriteLine("DatabaseTest.SetErrorHandler");
            SetErrorHandler();

            Console.WriteLine("DatabaseTest.CreateWithParameters");
            CreateWithParameters();

            Console.WriteLine("DatabaseTest.CreateWithParameters2");
            CreateWithParameters2();

            Console.WriteLine("DatabaseTest.GetVersion");
            GetVersion();

            Console.WriteLine("DatabaseTest.DatabaseClose");
            DatabaseClose();

            Console.WriteLine("DatabaseTest.CreateInvalidParameter");
            CreateInvalidParameter();

            Console.WriteLine("DatabaseTest.CreateString");
            CreateString();

            Console.WriteLine("DatabaseTest.CreateStringIntIntParameter");
            CreateStringIntIntParameter();

            Console.WriteLine("DatabaseTest.CreateStringIntIntParameterNeg");
            CreateStringIntIntParameterNeg();

            Console.WriteLine("DatabaseTest.CreateWithParameters");
            CreateWithParameters();

            Console.WriteLine("DatabaseTest.CreateWithParameters2");
            CreateWithParameters2();

            Console.WriteLine("DatabaseTest.SetComparator");
            SetComparator();

            Console.WriteLine("DatabaseTest.FindKey"); 
            FindKey();

            Console.WriteLine("DatabaseTest.FindKeyNull"); 
            FindKeyNull();

            Console.WriteLine("DatabaseTest.FindUnknownKey"); 
            FindUnknownKey();

            Console.WriteLine("DatabaseTest.InsertKey"); 
            InsertKey();

            Console.WriteLine("DatabaseTest.InsertKeyInvalidParam"); 
            InsertKeyInvalidParam();

            Console.WriteLine("DatabaseTest.InsertKeyNegative");
            InsertKeyNegative();

            Console.WriteLine("DatabaseTest.InsertKeyOverwrite");
            InsertKeyOverwrite();

            Console.WriteLine("DatabaseTest.EraseKey");
            EraseKey();

            Console.WriteLine("DatabaseTest.EraseKeyNegative");
            EraseKeyNegative();

            Console.WriteLine("DatabaseTest.EraseKeyTwice");
            EraseKeyTwice();

            Console.WriteLine("DatabaseTest.EraseUnknownKey");
            EraseUnknownKey();

            Console.WriteLine("DatabaseTest.GetKeyCount");
            GetKeyCount();

            Console.WriteLine("DatabaseTest.Cursor10000Test");
            Cursor10000Test();

            Console.WriteLine("DatabaseTest.AutoCleanupCursors");
            AutoCleanupCursors();

            Console.WriteLine("DatabaseTest.AutoCleanupCursors2");
            AutoCleanupCursors2();

            Console.WriteLine("DatabaseTest.AutoCleanupCursors3");
            AutoCleanupCursors3();

            Console.WriteLine("DatabaseTest.AutoCleanupCursors4");
            AutoCleanupCursors4();

            Console.WriteLine("DatabaseTest.ApproxMatching");
            ApproxMatching();
        }
    }
}
