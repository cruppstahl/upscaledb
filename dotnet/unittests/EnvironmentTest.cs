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
    public class EnvironmentTest : IDisposable
    {
        private readonly Upscaledb.Environment env;

        public EnvironmentTest()
        {
            env = new Upscaledb.Environment();
        }
        public void Dispose()
        {
            env.Dispose();
        }

        private static void CheckEqual<X>(X[] lhs, X[] rhs)
        {
            Assert.Equal(lhs.Length, rhs.Length);
            for (int i = 0; i < lhs.Length; i++)
                Assert.Equal(lhs[i], rhs[i]);
        }

        [Fact]
        public void CreateString() {
            try
            {
                env.Create("ntest.db");
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateStringNull()
        {
            try
            {
                env.Create(null);
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
            }
        }

        [Fact]
        public void CreateStringInt() {
            try
            {
                env.Create(null, UpsConst.UPS_IN_MEMORY);
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateStringIntInt() {
            try
            {
                env.Create("ntest.db", 0, 0644);
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateStringIntIntParameter()
        {
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter();
            param[0].name = UpsConst.UPS_PARAM_CACHE_SIZE;
            param[0].value = 1000;
            try
            {
                env.Create("ntest.db", 0, 0644, param);
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateStringIntIntParameterNeg() {
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_PAGESIZE,
                value = 777
            };
            try
            {
                env.Create("ntest.db", 0, 0644, param);
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_INV_PAGESIZE, e.ErrorCode);
            }
        }

        private void OpenString() {
            try
            {
                env.Create("ntest.db");
                env.Close();
                env.Open("ntest.db");
                env.Close();
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        private void OpenStringNegative() {
            try
            {
                env.Open("ntestxxxxx.db");
                env.Close();
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_FILE_NOT_FOUND, e.ErrorCode);
            }
        }

        private void OpenStringIntIntParameter() {
            Parameter[] param = new Parameter[1];
            param[0] = new Parameter
            {
                name = UpsConst.UPS_PARAM_CACHESIZE,
                value = 1024
            };
            try
            {
                env.Create("ntest.db", 0, 0644, param);
                env.Close();
                env.Open("ntest.db", 0, param);
                env.Close();
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateDatabaseShort() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try
            {
                env.Create("ntest.db");
                Database db = env.CreateDatabase((short)13);
                db.Insert(k, r);
                db.Close();
                db = env.OpenDatabase((short)13);
                byte[] f = db.Find(k);
                CheckEqual(r, f);
                // db.Close();
                env.Close();
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void CreateDatabaseNegative() {
            try
            {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase((short)0))
                { }
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_INV_PARAMETER, e.ErrorCode);
            }
        }

        [Fact]
        public void OpenDatabaseNegative() {
            try
            {
                env.Create("ntest.db");
                using (var db = env.OpenDatabase((short)99)) { }
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_DATABASE_NOT_FOUND, e.ErrorCode);
            }
        }

        [Fact]
        public void RenameDatabase() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];
            try
            {
                env.Create("ntest.db");
                using (var db = env.CreateDatabase((short)13))
                    db.Insert(k, r);
                env.RenameDatabase((short)13, (short)15);
                using (var db = env.OpenDatabase((short)15))
                {
                    byte[] f = db.Find(k);
                    CheckEqual(r, f);
                }
            }
            catch (DatabaseException)
            {
                //Unexpected exception
                Assert.False(true);
            }
        }

        [Fact]
        public void EraseDatabase() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            env.Create("ntest.db");
            using (var db = env.CreateDatabase((short)13))
                db.Insert(k, r);
            env.EraseDatabase((short)13);
            try
            {
                using (var db = env.OpenDatabase((short)15)) { }
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_DATABASE_NOT_FOUND, e.ErrorCode);
            }
        }


        [Fact]
        public void EraseUnknownDatabase() {
            byte[] k = new byte[5];
            byte[] r = new byte[5];

            env.Create("ntest.db");
            using (var db = env.CreateDatabase((short)13))
                db.Insert(k, r);
            try
            {
                env.EraseDatabase((short)99);
            }
            catch (DatabaseException e)
            {
                Assert.Equal(UpsConst.UPS_DATABASE_NOT_FOUND, e.ErrorCode);
            }
        }

        [Fact]
        public void Flush()
        {
            env.Create("ntest.db");
            env.Flush();
        }

        [Fact]
        public void GetDatabaseNames()
        {
            short[] names;
            short[] s1 = { 13 };
            short[] s2 = { 13, 14 };
            short[] s3 = { 13, 14, 15 };

            env.Create("ntest.db");
            using (var db = env.CreateDatabase(13))
                names = env.GetDatabaseNames();
            CheckEqual(s1, names);

            using (var db = env.CreateDatabase(14))
                names = env.GetDatabaseNames();
            CheckEqual(s2, names);

            using (var db = env.CreateDatabase(15))
                names = env.GetDatabaseNames();
            CheckEqual(s3, names);
        }
    }
}
