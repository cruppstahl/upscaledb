/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

/**
 * A simple example, which creates a database, inserts some values,
 * looks them up and erases them. Uses the C++ api.
 */

#include <iostream>
#include <set>
#include <ups/upscaledb.hpp>

void fillWithRandom(ups_db_t* db, size_t item_count)
{
    srand(0);

    std::set<unsigned int> unique_keys;

    while(unique_keys.size() != item_count)
    {
        unsigned int v = rand() % 100000;
        if(unique_keys.insert(v).second)
        {
            ups_key_t key = ups_make_key(&v, sizeof(v));
            ups_record_t record = {0};

            ups_db_insert(db, 0, &key, &record, 0);
        }
    }
}

const unsigned int bulk_size = 100;

int main()
{
    ups_env_t* env;
    ups_env_create(&env, "test.db", UPS_ENABLE_TRANSACTIONS, 0664, 0);

    ups_parameter_t params[] = {
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {0, }
    };

    ups_db_t* db_1;
    ups_env_create_db(env, &db_1, 1, 0, &params[0]);
    ups_db_t* db_2;
    ups_env_create_db(env, &db_2, 2, 0, &params[0]);

    size_t db_size = bulk_size * 2;

    fillWithRandom(db_1, db_size);

    ups_status_t st;

    do
    {
        ups_cursor_t* cur;
        if(UPS_SUCCESS != (st =ups_cursor_create(&cur, db_1, 0, 0)))
        {
            std::cout << "Cursor create error " << st;
        }
        unsigned int query = 0;

        ups_key_t key_find = ups_make_key(&query, sizeof(query));

        if((st = ups_cursor_find(cur, &key_find, 0, UPS_FIND_GEQ_MATCH)) != UPS_SUCCESS) // won't return in second iteration
        {
            std::cout << "Find error " << st;
        }

        ups_key_t key_move = {0};

        size_t insertions = 0;

        while(UPS_SUCCESS == (st = ups_cursor_move(cur, &key_move, 0, UPS_CURSOR_NEXT)) && (insertions++ <= bulk_size))
        {
            ups_record_t record = {0};

            if(UPS_SUCCESS != (st = ups_db_insert(db_2, 0, &key_move, &record, 0)))
            {
                std::cout << "Insert error " << st;
            }

            if(UPS_SUCCESS != (st = ups_db_erase(db_1, 0, &key_move, 0)))
            {
                std::cout << "Erase error " << st;
            }
        }

        if(UPS_SUCCESS != (st = ups_cursor_close(cur)))
        {
            std::cout << "Cursor close error " << st;
        }

    }while((db_size -= bulk_size) > 0);

    return 0;
}
