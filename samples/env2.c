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
 * Similar to env1, an Environment with a customer- and an order-Database
 * is created; a third Database is created which manages the 1:n relationship
 * between the other two.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ups/upscaledb.h>

void
error(const char *foo, ups_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

#define MAX_DBS           3

#define DBNAME_CUSTOMER   1
#define DBNAME_ORDER      2
#define DBNAME_C2O        3   /* C2O: Customer To Order */

#define DBIDX_CUSTOMER    0
#define DBIDX_ORDER       1
#define DBIDX_C2O         2

#define MAX_CUSTOMERS     4
#define MAX_ORDERS        8

/* A structure for the "customer" database */
typedef struct {
  uint32_t id;         /* customer id; will be the key of the customer table */
  char name[32];        /* customer name */
  /* ... additional information could follow here */
} customer_t;

/* A structure for the "orders" database */
typedef struct {
  uint32_t id;         /* order id; will be the key of the order table */
  uint32_t customer_id;/* customer id */
  char assignee[32];    /* assigned to whom? */
  /* ... additional information could follow here */
} order_t;

int
main(int argc, char **argv) {
  int i;
  ups_status_t st;               /* status variable */
  ups_db_t *db[MAX_DBS];         /* upscaledb database objects */
  ups_env_t *env;                /* upscaledb environment */
  ups_cursor_t *cursor[MAX_DBS]; /* a cursor for each database */
  ups_key_t key = {0};
  ups_key_t cust_key = {0};
  ups_key_t ord_key = {0};
  ups_key_t c2o_key = {0};
  ups_record_t record = {0};
  ups_record_t cust_record = {0};
  ups_record_t ord_record = {0};
  ups_record_t c2o_record = {0};

  ups_parameter_t params[] = {
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {0, }
  };

  ups_parameter_t c2o_params[] = {
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {UPS_PARAM_RECORD_SIZE, sizeof(uint32_t)},
    {0, }
  };

  customer_t customers[MAX_CUSTOMERS] = {
    { 1, "Alan Antonov Corp." },
    { 2, "Barry Broke Inc." },
    { 3, "Carl Caesar Lat." },
    { 4, "Doris Dove Brd." }
  };

  order_t orders[MAX_ORDERS] = {
    { 1, 1, "Joe" },
    { 2, 1, "Tom" },
    { 3, 3, "Joe" },
    { 4, 4, "Tom" },
    { 5, 3, "Ben" },
    { 6, 3, "Ben" },
    { 7, 4, "Chris" },
    { 8, 1, "Ben" }
  };

  /* Now create a new database file for the Environment */
  st = ups_env_create(&env, "test.db", 0, 0664, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_create", st);

  /*
   * Then create the two Databases in this Environment; each Database
   * has a name - the first is our "customer" Database, the second
   * is for the "orders"; the third manages our 1:n relation and
   * therefore needs to enable duplicate keys
   */
  st = ups_env_create_db(env, &db[DBIDX_CUSTOMER], DBNAME_CUSTOMER,
                  0, &params[0]);
  if (st != UPS_SUCCESS)
    error("ups_env_create_db(customer)", st);
  st = ups_env_create_db(env, &db[DBIDX_ORDER], DBNAME_ORDER, 0, &params[0]);
  if (st != UPS_SUCCESS)
    error("ups_env_create_db(order)", st);
  st = ups_env_create_db(env, &db[DBIDX_C2O], DBNAME_C2O,
                  UPS_ENABLE_DUPLICATE_KEYS, &c2o_params[0]);
  if (st != UPS_SUCCESS)
    error("ups_env_create_db(c2o)", st);

  /* Create a Cursor for each Database */
  for (i = 0; i < MAX_DBS; i++) {
    st = ups_cursor_create(&cursor[i], db[i], 0, 0);
    if (st != UPS_SUCCESS)
      error("ups_cursor_create" , st);
  }

  /*
   * Insert the customers in the customer table
   *
   * INSERT INTO customers VALUES (1, "Alan Antonov Corp.");
   * INSERT INTO customers VALUES (2, "Barry Broke Inc.");
   * etc
   */
  for (i = 0; i < MAX_CUSTOMERS; i++) {
    key.size = sizeof(int);
    key.data = &customers[i].id;

    record.size = sizeof(customer_t);
    record.data = &customers[i];

    st = ups_db_insert(db[0], 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_insert (customer)", st);
  }

  /*
   * And now the orders in the second Database; contrary to env1,
   * we only store the assignee, not the whole structure
   *
   * INSERT INTO orders VALUES (1, "Joe");
   * INSERT INTO orders VALUES (2, "Tom");
   */
  for (i = 0; i < MAX_ORDERS; i++) {
    key.size = sizeof(int);
    key.data = &orders[i].id;

    record.size = sizeof(orders[i].assignee);
    record.data = orders[i].assignee;

    st = ups_db_insert(db[1], 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_insert (order)", st);
  }

  /*
   * And now the 1:n relationships; the flag UPS_DUPLICATE creates
   * a duplicate key, if the key already exists
   *
   * INSERT INTO c2o VALUES (1, 1);
   * INSERT INTO c2o VALUES (2, 1);
   * etc
   */
  for (i = 0; i < MAX_ORDERS; i++) {
    key.size = sizeof(int);
    key.data = &orders[i].customer_id;

    record.size = sizeof(int);
    record.data = &orders[i].id;

    st = ups_db_insert(db[2], 0, &key, &record, UPS_DUPLICATE);
    if (st != UPS_SUCCESS)
      error("ups_db_insert(c2o)", st);
  }

  /*
   * Now start the query - we want to dump each customer with his
   * orders
   *
   * loop over the customer; for each customer, loop over the 1:n table
   * and pick those orders with the customer id. then load the order
   * and print it
   *
   * the outer loop is similar to:
   *  SELECT * FROM customers WHERE 1;
   */
  while (1) {
    customer_t *customer;

    st = ups_cursor_move(cursor[0], &cust_key, &cust_record, UPS_CURSOR_NEXT);
    if (st != UPS_SUCCESS) {
      /* reached end of the database? */
      if (st == UPS_KEY_NOT_FOUND)
        break;
      else
        error("ups_cursor_next(customer)", st);
    }

    customer = (customer_t *)cust_record.data;

    /* print the customer id and name */
    printf("customer %d ('%s')\n", customer->id, customer->name);

    /*
     * loop over the 1:n table
     *
     * before we start the loop, we move the cursor to the
     * first duplicate key
     *
     * SELECT * FROM customers, orders, c2o
     *   WHERE c2o.customer_id=customers.id AND
     *    c2o.order_id=orders.id;
     */
    c2o_key.data = &customer->id;
    c2o_key.size = sizeof(int);
    st = ups_cursor_find(cursor[2], &c2o_key, 0, 0);
    if (st != UPS_SUCCESS) {
      if (st == UPS_KEY_NOT_FOUND)
        continue;
      error("ups_cursor_find(c2o)", st);
    }
    st = ups_cursor_move(cursor[2], 0, &c2o_record, 0);
    if (st != UPS_SUCCESS)
      error("ups_cursor_move(c2o)", st);

    do {
      int order_id;

      order_id = *(int *)c2o_record.data;
      ord_key.data = &order_id;
      ord_key.size = sizeof(int);

      /*
       * load the order
       * SELECT * FROM orders WHERE id = order_id;
       */
      st = ups_db_find(db[1], 0, &ord_key, &ord_record, 0);
      if (st != UPS_SUCCESS)
        error("ups_db_find(order)", st);

      printf("  order: %d (assigned to %s)\n",
          order_id, (char *)ord_record.data);

      /*
       * The flag UPS_ONLY_DUPLICATES restricts the cursor
       * movement to the duplicate list.
       */
      st = ups_cursor_move(cursor[2], &c2o_key,
          &c2o_record, UPS_CURSOR_NEXT|UPS_ONLY_DUPLICATES);
      if (st != UPS_SUCCESS) {
        /* reached end of the database? */
        if (st == UPS_KEY_NOT_FOUND)
          break;
        else
          error("ups_cursor_next(c2o)", st);
      }
    } while (1);
  }

  /*
   * Now close the Environment handle; the flag
   * UPS_AUTO_CLEANUP will automatically close all Databases and
   * Cursors
   */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  printf("success!\n");
  return (0);
}
