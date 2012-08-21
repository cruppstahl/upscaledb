/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * A simple example, which creates a database, inserts some values,
 * looks them up and erases them. Uses the C++ api.
 */

#include <iostream>
#include <ham/hamsterdb.hpp>

#define LOOP 10

int
run_demo(void)
{
    int i;
    ham::db db;            /* hamsterdb database object */
    ham::key key;          /* a key */
    ham::record record;    /* a record */

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
    for (i=0; i<LOOP; i++) {
        key.set_size(sizeof(i));
        key.set_data(&i);

        record.set_size(sizeof(i));
        record.set_data(&i);

        db.insert(&key, &record);
    }

    /*
     * now lookup all values
     *
     * for db::find(), we could use the flag HAM_RECORD_USER_ALLOC, if WE
     * allocate record.data (otherwise the memory is automatically allocated
     * by hamsterdb)
     */
    for (i=0; i<LOOP; i++) {
        key.set_size(sizeof(i));
        key.set_data(&i);

        record=db.find(&key);

        /*
         * check if the value is ok
         */
        if (*(int *)record.get_data()!=i) {
            std::cerr << "db::find() ok, but returned bad value" << std::endl;
            return (-1);
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
    for (i=0; i<LOOP; i++) {
        key.set_size(sizeof(i));
        key.set_data(&i);

        db.erase(&key);
    }

    /*
     * once more we try to find all values... every db::find() call must
     * now fail with HAM_KEY_NOT_FOUND
     */
    for (i=0; i<LOOP; i++) {
        key.set_size(sizeof(i));
        key.set_data(&i);

        try {
            record=db.find(&key);
        }
        catch (ham::error &e) {
            if (e.get_errno()!=HAM_KEY_NOT_FOUND) {
                std::cerr << "db::find() returned error " << e.get_string()
                          << std::endl;
                return (-1);
            }
        }
    }

    /*
     * we're done! no need to close the database handle, it's done
     * automatically
     */

    std::cout << "success!" << std::endl;
    return (0);
}

int
main(int argc, char **argv)
{
    try {
        return (run_demo());
    }
    catch (ham::error &e) {
        std::cerr << "run_demo() failed with unexpected error "
                  << e.get_errno() << " ('"
                  << e.get_string() << "')" << std::endl;
        return (-1);
    }
}
