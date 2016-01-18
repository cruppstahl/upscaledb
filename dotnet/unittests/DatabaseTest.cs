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
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Upscaledb;

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
            Upscaledb.Environment env = new Upscaledb.Environment();
            ErrorHandler eh = new ErrorHandler(MyErrorHandler);
            try {
                Database.SetErrorHandler(eh);
                env.Create(null);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
                Assert.AreEqual(1, errorCounter);
            }
            Database.SetErrorHandler(null);
        }

        private void CreateWithParameters()
        {
            using (Upscaledb.Environment env = new Upscaledb.Environment())
            {
                env.Create("ntest.db");

                Parameter[] param = new Parameter[] {
                    new Parameter {
                        name = UpsConst.UPS_PARAM_KEYSIZE, value = 32
                    }
                };
                using (Database db = env.CreateDatabase(13, 0, param)) { }
            }
        }

        private void CreateWithParameters2()
        {
            using (Upscaledb.Environment env = new Upscaledb.Environment())
            {
                env.Create("ntest.db");
                using (Database db = env.CreateDatabase(13, 0,
                           new Parameter[0])) { }
            }
        }

        private void GetVersion() {
            Upscaledb.Version v = Database.GetVersion();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
                Assert.AreEqual(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
            }
        }

        private void CreateStringIntIntParameter() {
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = UpsConst.UPS_PARAM_CACHESIZE;
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
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = UpsConst.UPS_PARAM_CACHESIZE;
            param[0].value = 1024;
            try {
                env.Create("ntest.db", UpsConst.UPS_IN_MEMORY, 0644, param);
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
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

        private void SetComparator1() {
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = UpsConst.UPS_PARAM_KEY_TYPE;
            param[0].value = UpsConst.UPS_TYPE_CUSTOM;

            compareCounter = 0;
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1, 0, param);
                db.SetCompareFunc(new Upscaledb.CompareFunc(MyCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                k[0] = 2;
                db.Insert(k, r);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e) {
                Assert.Fail("unexpected exception " + e);
            }
            Assert.AreEqual(2, compareCounter);
        }

        private void SetComparator2()
        {
            Upscaledb.Database.RegisterCompare("cmp", MyCompareFunc);
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = UpsConst.UPS_PARAM_KEY_TYPE;
            param[0].value = UpsConst.UPS_TYPE_CUSTOM;

            compareCounter = 0;
            try
            {
                env.Create("ntest.db");
                db = env.CreateDatabase(1, 0, param);
                db.SetCompareFunc(new CompareFunc(MyCompareFunc));
                db.Insert(k, r);
                k[0] = 1;
                db.Insert(k, r);
                k[0] = 2;
                db.Insert(k, r);
                db.Close();
                env.Close();
            }
            catch (DatabaseException e)
            {
                Assert.Fail("unexpected exception " + e);
            }
            Assert.AreEqual(2, compareCounter);
        }

        void checkEqual(byte[] lhs, byte[] rhs)
        {
            Assert.AreEqual(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.AreEqual(lhs[i], rhs[i]);
        }

        private void FindKey() {
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                byte[] r = db.Find(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void InsertKey() {
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            byte[] r1 = new byte[5];
            byte[] r2;
            try
            {
                env.Create("ntest.db");
                db = env.CreateDatabase(1, UpsConst.UPS_RECORD_NUMBER);
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
                Assert.AreEqual(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void InsertKeyNegative() {
            Upscaledb.Environment env = new Upscaledb.Environment();
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
                Assert.AreEqual(UpsConst.UPS_DUPLICATE_KEY, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void InsertKeyOverwrite() {
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try {
                env.Create("ntest.db");
                db = env.CreateDatabase(1);
                db.Insert(k, r);
                r[0] = 1;
                db.Insert(k, r, UpsConst.UPS_OVERWRITE);
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
                Assert.AreEqual(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void EraseKeyNegative() {
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            byte[] k = new byte[5];
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            try {
                db.Erase(k);
            }
            catch (DatabaseException e) {
                Assert.AreEqual(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void EraseKeyTwice() {
            Upscaledb.Environment env = new Upscaledb.Environment();
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
                Assert.AreEqual(UpsConst.UPS_KEY_NOT_FOUND, e.ErrorCode);
            }
            db.Close();
            env.Close();
        }

        private void Transactions() {
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            env.Create("ntest.db", UpsConst.UPS_ENABLE_TRANSACTIONS);
            db = env.CreateDatabase(1);

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            db.Insert(k, r);
            db.Close();
            env.Close();
        }

        private void GetKeyCount()
        {
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);

            byte[] k = new byte[5];
            byte[] r = new byte[5];
            Assert.AreEqual(0, db.GetCount());
            db.Insert(k, r);
            Assert.AreEqual(1, db.GetCount());
            k[0] = 1;
            db.Insert(k, r);
            Assert.AreEqual(2, db.GetCount());
            db.Close();
            env.Close();
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

            Parameter param1 = new Parameter();
            param1.name = UpsConst.UPS_PARAM_CACHESIZE;
            param1.value = 768 * 1024 * 1024;
            list.Add(param1);

            Parameter param2 = new Parameter();
            param2.name = UpsConst.UPS_PARAM_KEYSIZE;
            param2.value = 8; // sizeof(ulong);
            list.Add(param2);

            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            env.Create(file, 0, 0, list.ToArray());
            db = env.CreateDatabase(1);
            db.SetCompareFunc(new Upscaledb.CompareFunc(NumericalCompareFunc));
            return db;
        }

        private Database OpenDatabase(string file)
        {
            List<Parameter> list = new List<Parameter>();

            Parameter param1 = new Parameter();
            param1.name = UpsConst.UPS_PARAM_CACHESIZE;
            param1.value = 768 * 1024 * 1024;
            list.Add(param1);

            Upscaledb.Environment env = new Upscaledb.Environment(); 
            Database db = new Database();
            env.Open(file, 0, list.ToArray());
            db = env.OpenDatabase(1);
            db.SetCompareFunc(new Upscaledb.CompareFunc(NumericalCompareFunc));
            return db;
        }

        private void Cursor10000Test()
        {
            //create database
            Upscaledb.Environment env = new Upscaledb.Environment();
            env.Create("ntest.db");

            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = UpsConst.UPS_PARAM_KEY_TYPE;
            param[0].value = UpsConst.UPS_TYPE_UINT64;
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
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();
            env.Create("ntest.db");
            db = env.CreateDatabase(1);
            Cursor cursor = new Cursor(db);
            // let gc do the cleanup
            env.Close();
        }

        private void AutoCleanupCursors2()
        {
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
            Upscaledb.Environment env = new Upscaledb.Environment();
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
                checkEqual(r, r3);
                checkEqual(k2, k3);
                k2[0] = 2;
                r = db.Find(null, ref k2, UpsConst.UPS_FIND_LT_MATCH);
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

            Console.WriteLine("DatabaseTest.SetComparator1");
            SetComparator1();

            Console.WriteLine("DatabaseTest.SetComparator2");
            SetComparator2();

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
