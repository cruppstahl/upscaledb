/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

import de.crupp.hamsterdb.*;

public class Db1 {

    public static final int LOOP=10;

    public void run() 
            throws de.crupp.hamsterdb.Error {
        byte[] key=new byte[5];
        byte[] record=new byte[5];
        Database db=new Database();

        /*
         * first, create a new database file
         */
        db.create("test.db");

        /*
         * now we can insert, delete or lookup values in the database
         *
         * for our test program, we just insert a few values, then look them 
         * up, then delete them and try to look them up again (which will fail).
         */
        for (int i=0; i<LOOP; i++) {
            key[0]=(byte)i;
            record[0]=(byte)i;
    
            db.insert(key, record);
        }

        /*
         * now lookup all values
         *
         * for db::find(), we could use the flag HAM_RECORD_USER_ALLOC, if WE
         * allocate record.data (otherwise the memory is automatically allocated
         * by hamsterdb)
         */
        for (int i=0; i<LOOP; i++) {
            key[0]=(byte)i;

            record=db.find(key);
    
            /*
             * check if the value is ok
             */
            if (record[0]!=(byte)i) {
                System.out.println("db.find() ok, but returned bad value");
                return;
            }
        }

        /*
         * close the database handle, then re-open it (just to demonstrate how
         * to open a database file)
         */
        db.close();
        db.open("test.db");

        /*
         * now erase all values
         */
        for (int i=0; i<LOOP; i++) {
            key[0]=(byte)i;
            db.erase(key);
        }

        /*
         * once more we try to find all values... every db.find() call must
         * now fail with HAM_KEY_NOT_FOUND
         */
        for (int i=0; i<LOOP; i++) {
            key[0]=(byte)i;

            try {
                record=db.find(key);
            }
            catch (de.crupp.hamsterdb.Error e) {
                if (e.getErrno()!=Const.HAM_KEY_NOT_FOUND) {
                    System.out.println("db.find() returned error "+e);
                    return;
                }
            }
        }

        /*
         * we're done! no need to close the database handle, it's done 
         * automatically
         */
        System.out.println("Success!");
    }

	public static void main(String args[]) {
		try {
            Db1 db1=new Db1();
            db1.run();
        }
		catch (de.crupp.hamsterdb.Error err) {
			System.out.println("Exception "+err);
		}
	}
}

