/**
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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
