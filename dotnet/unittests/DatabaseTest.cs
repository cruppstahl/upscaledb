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
    [DeploymentItem("..\\win32\\msvc2008\\out\\dll_debug\\hamsterdb-2.0.5.dll")]
    public class DatabaseTest
    {
        private static int errorCounter;

        static void MyErrorHandler(int level, String message) {
            Console.WriteLine("ErrorHandler: Level " + level + ", msg: " + message);
            errorCounter++;
        }

        [TestMethod()]
        public void SetErrorHandler() {
            Database db = new Database();
            ErrorHandler eh = new ErrorHandler(MyErrorHandler);
            try {
                Database.SetErrorHandler(eh);
                db.Create(null);
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
                using (Database db = env.CreateDatabase(13, HamConst.HAM_DISABLE_VAR_KEYLEN, param)) { }
            }
        }

        [TestMethod()]
        public void CreateWithParameters2()
        {
            using (Hamster.Environment env = new Hamster.Environment())
            {
                env.Create("ntest.db");
                using (Database db = env.CreateDatabase(13, HamConst.HAM_DISABLE_VAR_KEYLEN, new Parameter[0])) { }
            }
        }

        [TestMethod()]
        public void GetVersion() {
            Hamster.Version v = Database.GetVersion();
            Assert.AreEqual(2, v.major);
            Assert.AreEqual(0, v.minor);
        }

        [TestMethod()]
        public void GetLicense() {
            Hamster.License l = Database.GetLicense();
            Assert.AreEqual("", l.licensee); // this fails if you have a licensed version
            Assert.AreEqual("hamsterdb embedded storage", l.product);
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
            try {
                db.Create("ntest.db");
                db.Close();
                db.Open("ntest.db");
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("Unexpected exception " + e);
            }
        }

        [TestMethod()]
        public void CreateInvalidParameter() {
            Database db = new Database();
            Parameter[] param = new Parameter[3];
            param[1] = new Parameter();
            param[2] = new Parameter();
            try {
                db.Create("ntest.db", 0, 0, param);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
        }

        [TestMethod()]
        public void CreateStringIntIntParameter() {
            Database db = new Database();
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = HamConst.HAM_PARAM_CACHESIZE;
            param[0].value = 1024;
            try {
                db.Create("ntest.db", 0, 0644, param);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("Unexpected exception " + e);
            }
        }

        [TestMethod()]
        public void CreateStringIntIntParameterNeg() {
            Database db = new Database();
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = HamConst.HAM_PARAM_CACHESIZE;
            param[0].value = 1024;
            try {
                db.Create("ntest.db", HamConst.HAM_IN_MEMORY_DB, 0644, param);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
        }

        [TestMethod()]
        public void GetError() {
            Database db = new Database();
            try {
                db.Create(null);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
            Assert.AreEqual(HamConst.HAM_INV_PARAMETER, db.GetLastError());
            db.Close();
        }

        private int compareCounter;

        private int MyCompareFunc(byte[] lhs, byte[] rhs) {
            // always return a different value or hamsterdb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        [TestMethod()]
        public void SetComparator() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            compareCounter = 0;
            try {
                db.Create("ntest.db");
                db.SetCompareFunc(new CompareFunc(MyCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
            Assert.AreEqual(3, compareCounter);
        }

        private int MyPrefixCompareFunc(byte[] lhs, int lhsRealLength,
                byte[] rhs, int rhsRealLength) {
            // always return a different value or hamsterdb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        [TestMethod()]
        public void SetPrefixComparator() {
            Database db = new Database();
            byte[] k = new byte[25];
            byte[] r = new byte[25];
            compareCounter = 0;
            try {
                db.Create("ntest.db");
                db.SetPrefixCompareFunc(new
                    PrefixCompareFunc(MyPrefixCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
            Assert.AreEqual(1, compareCounter);
        }

        private int MyDupeCompareFunc(byte[] lhs, byte[] rhs)
        {
            // always return a different value or hamsterdb thinks
            // we're inserting duplicates
            return ++compareCounter;
        }

        [TestMethod()]
        public void EnableCompression() {
            Database db = new Database();
            db.Create("ntest.db");
            db.EnableCompression();
            db.Close();
        }

        [TestMethod()]
        public void EnableCompressionInt() {
            Database db = new Database();
            try {
                db.Create("ntest.db");
                db.EnableCompression(999);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_INV_PARAMETER, e.ErrorCode);
            }
            db.Close();
        }

        void checkEqual(byte[] lhs, byte[] rhs)
        {
            Assert.AreEqual(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.AreEqual(lhs[i], rhs[i]);
        }

        [TestMethod()]
        public void FindKey() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            r1[0] = 1;
            try {
                db.Create("ntest.db");
                db.Insert(k, r1);
                byte[] r2 = db.Find(k);
                checkEqual(r1, r2);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
        }

        [TestMethod()]
        public void FindKeyNull() {
            Database db = new Database();
            try {
                db.Create("ntest.db");
                byte[] r = db.Find(null);
            }
            catch (NullReferenceException) {
            }
            db.Close();
        }

        [TestMethod()]
        public void FindUnknownKey() {
            Database db = new Database();
            byte[] k = new byte[5];
            try {
                db.Create("ntest.db");
                byte[] r = db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
        }

        [TestMethod()]
        public void InsertKey() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r1 = new byte[5];
            byte[] r2;
            try {
                db.Create("ntest.db");
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
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
        }

        [TestMethod()]
        public void InsertKeyInvalidParam() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Create("ntest.db");
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
        }

        [TestMethod()]
        public void InsertKeyNegative() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                db.Create("ntest.db");
                db.Insert(k, r);
                db.Insert(k, r);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_DUPLICATE_KEY, e.ErrorCode);
            }
            db.Close();
        }

        [TestMethod()]
        public void InsertKeyOverwrite() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                db.Create("ntest.db");
                db.Insert(k, r);
                r[0] = 1;
                db.Insert(k, r, HamConst.HAM_OVERWRITE);
                byte[] r2 = db.Find(k);
                checkEqual(r, r2);
                db.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
        }

        [TestMethod()]
        public void EraseKey() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            db.Create("ntest.db");
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
        }

        [TestMethod()]
        public void EraseKeyNegative() {
            Database db = new Database();
            byte[] k = new byte[5];
            db.Create("ntest.db");
            try {
                db.Erase(null);
            }
            catch (NullReferenceException) {
            }
            db.Close();
        }

        [TestMethod()]
        public void EraseUnknownKey() {
            Database db = new Database();
            byte[] k = new byte[5];
            db.Create("ntest.db");
            try {
                db.Erase(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(HamConst.HAM_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
        }

        [TestMethod()]
        public void EraseKeyTwice() {
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            db.Create("ntest.db");
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
        }

        [TestMethod()]
        public void Flush() {
            Database db = new Database();
            db.Create("ntest.db");
            db.Flush();
            db.Close();
        }

        [TestMethod()]
        public void Recovery() {
            Database db = new Database();
            db.Create("ntest.db", HamConst.HAM_ENABLE_RECOVERY);

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            db.Close();
        }

        [TestMethod()]
        public void GetEnvironment()
        {
            Database db = new Database();
            db.Create("ntest.db");
            Hamster.Environment env = db.GetEnvironment();
            Assert.AreNotEqual(0, env.GetHandle());
            db.Close();
        }

        [TestMethod()]
        public void GetKeyCount()
        {
            Database db = new Database();
            db.Create("ntest.db");

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Assert.AreEqual(0, db.GetKeyCount());
            db.Insert(k, r);
            Assert.AreEqual(1, db.GetKeyCount());
            k[0] = 1;
            db.Insert(k, r);
            Assert.AreEqual(2, db.GetKeyCount());
            db.Close();
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

            Database db = new Database();
            db.Create(file, 0, 0, list.ToArray());
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

            Database db = new Database();
            db.Open(file, 0, list.ToArray());
            db.SetCompareFunc(new CompareFunc(NumericalCompareFunc));
            return db;
        }

        [TestMethod()]
        public void Cursor10000Test()
        {
            //create database
            Database hamster = CreateDatabase("test.db");

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
            hamster = OpenDatabase("test.db");
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
            Database db = new Database();
            db.Create("ntest.db");
            Cursor cursor = new Cursor(db);
            // let gc do the cleanup
            db.Close(HamConst.HAM_AUTO_CLEANUP);
        }


        [TestMethod()]
        public void AutoCleanupCursors2()
        {
            Database db = new Database();
            db.Create("ntest.db");
            Cursor cursor1 = new Cursor(db);
            Cursor cursor2 = new Cursor(db);
            Cursor cursor3 = new Cursor(db);
            Cursor cursor4 = new Cursor(db);
            Cursor cursor5 = new Cursor(db);
            // let gc do the cleanup
            db.Close(HamConst.HAM_AUTO_CLEANUP);
        }

        [TestMethod()]
        public void AutoCleanupCursors3()
        {
            Database db = new Database();
            db.Create("ntest.db");
            Cursor cursor1 = new Cursor(db);
            Cursor cursor2 = new Cursor(db);
            Cursor cursor3 = new Cursor(db);
            Cursor cursor4 = new Cursor(db);
            Cursor cursor5 = new Cursor(db);
            cursor3.Close();
            cursor5.Close();
            // let gc do the cleanup
            db.Close(HamConst.HAM_AUTO_CLEANUP);
        }

        [TestMethod()]
        public void AutoCleanupCursors4()
        {
            Database db = new Database();
            db.Create("ntest.db");
            Cursor cursor1 = new Cursor(db);
            Cursor cursor2 = cursor1.Clone();
            Cursor cursor3 = cursor1.Clone();
            Cursor cursor4 = cursor1.Clone();
            Cursor cursor5 = cursor1.Clone();
            cursor3.Close();
            cursor5.Close();
            // let gc do the cleanup
            db.Close(HamConst.HAM_AUTO_CLEANUP);
        }
    }
}
