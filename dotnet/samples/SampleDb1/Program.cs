/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Hamster;

namespace SampleDb1
{
    class Program
    {
        const int LOOP = 10;

        static void Main(string[] args) {
            byte[] key = new byte[5];
            byte[] record = new byte[5];
            Hamster.Environment env = new Hamster.Environment();
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
             * now fail with HAM_KEY_NOT_FOUND
             */
            for (int i = 0; i < LOOP; i++) {
                key[0] = (byte)i;

                try {
                    byte[] r = db.Find(key);
                }
                catch (DatabaseException e) {
                    if (e.ErrorCode != HamConst.HAM_KEY_NOT_FOUND) {
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
