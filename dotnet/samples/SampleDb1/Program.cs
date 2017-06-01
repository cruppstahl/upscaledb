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
using System.Text;
using Upscaledb;

namespace SampleDb1
{
    class Program
    {
        const int LOOP = 10;

        static void Main(string[] args) {
            byte[] key = new byte[5];
            byte[] record = new byte[5];
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database db = new Database();

            /*
             * first, create a new Database
             */
            env.Create("test.db");
            db = env.CreateDatabase(1);

            /*
             * now we can insert, delete or lookup values in the Database
             *
             * for our test program, we just insert a few values, then look them
             * up, then delete them and try to look them up again (which will fail).
             */
            for (int i = 0; i < LOOP; i++) {
                key[0] = (byte)i;
                record[0] = (byte)i;
                db.Insert(key, record);
            }

            /*
             * now look up all values
             */
            for (int i = 0; i < LOOP; i++) {
                key[0] = (byte)i;
                byte[] r = db.Find(key);

                /*
                 * check if the value is ok
                 */
                if (r[0] != (byte)i) {
                    Console.Out.WriteLine("db.Find() returned bad value");
                    return;
                }
            }

            /*
             * close the Database handle, then re-open it (just to demonstrate how
             * to open a Database file)
             */
            db.Close();
            env.Close();
            env.Open("test.db");
            db = env.OpenDatabase(1);

            /*
             * now erase all values
             */
            for (int i = 0; i < LOOP; i++) {
                key[0] = (byte)i;
                db.Erase(key);
            }

            /*
             * once more we try to find all values... every db.Find() call must
             * now fail with UPS_KEY_NOT_FOUND
             */
            for (int i = 0; i < LOOP; i++) {
                key[0] = (byte)i;

                try {
                    byte[] r = db.Find(key);
                }
                catch (DatabaseException e) {
                    if (e.ErrorCode != UpsConst.UPS_KEY_NOT_FOUND) {
                        Console.Out.WriteLine("db.Find() returned error " + e);
                        return;
                    }
                }
            }

            /*
             * We're done! No need to close the Database handle - it's closed automatically
             */
            Console.Out.WriteLine("Success!");
        }
    }
}
