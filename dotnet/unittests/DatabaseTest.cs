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
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Hamster;

namespace Unittests
{
    [TestClass()]
    [DeploymentItem("..\\..\\..\\..\\win32\\msvc2013\\out\\dll_debug\\hamsterdb-2.1.9.dll")]
    public class DatabaseTest
    {
        private static int errorCounter;

        static void MyErrorHandler(int level, String message) {
            Console.WriteLine("ErrorHandler: Level " + level + ", msg: " + message);
            errorCounter++;
        }

        [TestMethod()]
        public void SetErrorHandler() {
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

        [TestMethod()]
        public void CreateWithParameters()
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

        [TestMethod()]
        public void CreateWithParameters2()
        {
            using (Hamster.Environment env = new Hamster.Environment())
            {
                env.Create("ntest.db");
                using (Database db = env.CreateDatabase(13, 0,
                           new Parameter[0])) { }
            }
        }

        [TestMethod()]
        public void GetVersion() {
            Hamster.Version v = Database.GetVersion();
            Assert.AreEqual(2, v.major);
            Assert.AreEqual(0, v.minor);
        }

        [TestMethod()]
        public void DatabaseClose() {
            Database db = new Database();
            try {
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("Unexpected exception " + e);
            }
        }

        [TestMethod()]
        public void CreateString() {
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

        [TestMethod()]
        public void CreateInvalidParameter() {
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

        [TestMethod()]
        public void CreateStringIntIntParameter() {
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

        [TestMethod()]
        public void CreateStringIntIntParameterNeg() {
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

        [TestMethod()]
        public void GetError() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                db.Insert(null, null, HamConst.HAM_OVERWRITE | HamConst.HAM_DUPLICATE);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
            Assert.AreEqual(HamConst.HAM_INV_PARAMETER, db.GetLastError());
            db.Close();
            env.Close();
        }

        private int compareCounter;

        private int MyCompareFunc(byte[] lhs, byte[] rhs) {
            // always return a different value or hamsterdb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        [TestMethod()]
        public void SetComparator() {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            compareCounter = 0;
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
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
            Assert.AreEqual(3, compareCounter);
        }

        private int MyDupeCompareFunc(byte[] lhs, byte[] rhs)
        {
            // always return a different value or hamsterdb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        void checkEqual(byte[] lhs, byte[] rhs)
        {
            Assert.AreEqual(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.AreEqual(lhs[i], rhs[i]);
        }

        [TestMethod()]
        public void FindKey() {
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

        [TestMethod()]
        public void FindKeyNull() {
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

        [TestMethod()]
        public void FindUnknownKey() {
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

        [TestMethod()]
        public void InsertKey() {
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

        [TestMethod()]
        public void InsertKeyInvalidParam() {
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

        [TestMethod()]
        public void InsertKeyNegative() {
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

        [TestMethod()]
        public void InsertKeyOverwrite() {
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

        [TestMethod()]
        public void EraseKey() {
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

        [TestMethod()]
        public void EraseKeyNegative() {
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

        [TestMethod()]
        public void EraseUnknownKey() {
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

        [TestMethod()]
        public void EraseKeyTwice() {
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

        [TestMethod()]
        public void Recovery() {
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

        [TestMethod()]
        public void GetKeyCount()
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

        [TestMethod()]
        public void Cursor10000Test()
        {
            //create database
            Hamster.Environment env = new Hamster.Environment();
            env.Create("ntest.db");
            Database hamster = env.CreateDatabase(1);

            //insert records
            for (ulong i = 0; i < 10000; i++)
            {
                byte[] key = BitConverter.GetBytes(i);
                byte[] record = new byte[20];
                hamster.Insert(key, record);
            }

            //close database
            hamster.Close();

            //reopen again
            hamster = env.OpenDatabase(1);
            Cursor cursor = new Cursor(hamster);

            cursor.MoveFirst();
            ulong firstKey = BitConverter.ToUInt64(cursor.GetKey(), 0);
            Assert.AreEqual((ulong)0, firstKey);

            cursor.MoveLast();
            ulong lastKey = BitConverter.ToUInt64(cursor.GetKey(), 0);
            Assert.AreEqual((ulong)9999, lastKey);

            //close database
            cursor.Close();
            hamster.Close();
        }

        [TestMethod()]
        public void AutoCleanupCursors()
        {
            Hamster.Environment env = new Hamster.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            Cursor cursor = new Cursor(db);
            // let gc do the cleanup
            env.Close(HamConst.HAM_AUTO_CLEANUP);
        }


        [TestMethod()]
        public void AutoCleanupCursors2()
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
            env.Close(HamConst.HAM_AUTO_CLEANUP);
        }

        [TestMethod()]
        public void AutoCleanupCursors3()
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
            env.Close(HamConst.HAM_AUTO_CLEANUP);
        }

        [TestMethod()]
        public void AutoCleanupCursors4()
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
            env.Close(HamConst.HAM_AUTO_CLEANUP);
        }
    }
}
