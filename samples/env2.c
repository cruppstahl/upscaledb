/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * Similar to env1, an Environment with a customer- and an order-Database
 * is created; a third Database is created which manages the 1:n relationship
 * between the other two.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ham/hamsterdb.h>

void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

#define MAX_DBS       3

#define DBNAME_CUSTOMER   1
#define DBNAME_ORDER    2
#define DBNAME_C2O      3   /* C2O: Customer To Order */

#define DBIDX_CUSTOMER    0
#define DBIDX_ORDER     1
#define DBIDX_C2O       2

#define MAX_CUSTOMERS     4
#define MAX_ORDERS      8

/* A structure for the "customer" database */
typedef struct {
  int id;         /* customer id - will be the key of the
               * customer table */
  char name[32];      /* customer name */
  /* ... additional information could follow here */
} customer_t;

/* A structure for the "orders" database */
typedef struct {
  int id;         /* order id - will be the key of the
                 order table */
  int customer_id;    /* customer id */
  char assignee[32];    /* assigned to whom? */
  /* ... additional information could follow here */
} order_t;

int
main(int argc, char **argv) {
  int i;
  ham_status_t st;    /* status variable */
  ham_db_t *db[MAX_DBS];  /* hamsterdb database objects */
  ham_env_t *env;     /* hamsterdb environment */
  ham_cursor_t *cursor[MAX_DBS]; /* a cursor for each database */
  ham_key_t key, cust_key, ord_key, c2o_key;
  ham_record_t record, cust_record, ord_record, c2o_record;

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

  memset(&key, 0, sizeof(key));
  memset(&record, 0, sizeof(record));
  memset(&cust_key, 0, sizeof(cust_key));
  memset(&cust_record, 0, sizeof(cust_record));
  memset(&ord_key, 0, sizeof(ord_key));
  memset(&ord_record, 0, sizeof(ord_record));
  memset(&c2o_key, 0, sizeof(c2o_key));
  memset(&c2o_record, 0, sizeof(c2o_record));

  /* Now create a new database file for the Environment */
  st = ham_env_create(&env, "test.db", 0, 0664, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create", st);

  /*
   * Then create the two Databases in this Environment; each Database
   * has a name - the first is our "customer" Database, the second
   * is for the "orders"; the third manages our 1:n relation and
   * therefore needs to enable duplicate keys
   */
  st = ham_env_create_db(env, &db[DBIDX_CUSTOMER], DBNAME_CUSTOMER, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create_db(customer)", st);
  st = ham_env_create_db(env, &db[DBIDX_ORDER], DBNAME_ORDER, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create_db(order)", st);
  st = ham_env_create_db(env, &db[DBIDX_C2O], DBNAME_C2O,
        HAM_ENABLE_DUPLICATES, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create_db(c2o)", st);

  /* Create a Cursor for each Database */
  for (i = 0; i < MAX_DBS; i++) {
    st = ham_cursor_create(&cursor[i], db[i], 0, 0);
    if (st != HAM_SUCCESS)
      error("ham_cursor_create" , st);
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

    st = ham_db_insert(db[0], 0, &key, &record, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_insert (customer)", st);
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

    st = ham_db_insert(db[1], 0, &key, &record, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_insert (order)", st);
  }

  /*
   * And now the 1:n relationships; the flag HAM_DUPLICATE creates
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

    st = ham_db_insert(db[2], 0, &key, &record, HAM_DUPLICATE);
    if (st != HAM_SUCCESS)
      error("ham_db_insert(c2o)", st);
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

    st = ham_cursor_move(cursor[0], &cust_key, &cust_record, HAM_CURSOR_NEXT);
    if (st != HAM_SUCCESS) {
      /* reached end of the database? */
      if (st == HAM_KEY_NOT_FOUND)
        break;
      else
        error("ham_cursor_next(customer)", st);
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
    st = ham_cursor_find(cursor[2], &c2o_key, 0, 0);
    if (st != HAM_SUCCESS) {
      if (st == HAM_KEY_NOT_FOUND)
        continue;
      error("ham_cursor_find(c2o)", st);
    }
    st = ham_cursor_move(cursor[2], 0, &c2o_record, 0);
    if (st != HAM_SUCCESS)
      error("ham_cursor_move(c2o)", st);

    do {
      int order_id;

      order_id = *(int *)c2o_record.data;
      ord_key.data = &order_id;
      ord_key.size = sizeof(int);

      /*
       * load the order
       * SELECT * FROM orders WHERE id = order_id;
       */
      st = ham_db_find(db[1], 0, &ord_key, &ord_record, 0);
      if (st != HAM_SUCCESS)
        error("ham_db_find(order)", st);

      printf("  order: %d (assigned to %s)\n",
          order_id, (char *)ord_record.data);

      /*
       * The flag HAM_ONLY_DUPLICATES restricts the cursor
       * movement to the duplicate list.
       */
      st = ham_cursor_move(cursor[2], &c2o_key,
          &c2o_record, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES);
      if (st != HAM_SUCCESS) {
        /* reached end of the database? */
        if (st == HAM_KEY_NOT_FOUND)
          break;
        else
          error("ham_cursor_next(c2o)", st);
      }
    } while (1);
  }

  /*
   * Now close the Environment handle; the flag
   * HAM_AUTO_CLEANUP will automatically close all Databases and
   * Cursors
   */
  st = ham_env_close(env, HAM_AUTO_CLEANUP);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  printf("success!\n");
  return (0);
}
