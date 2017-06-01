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

/*
 * This sample is the implementation of /samples/env3.cpp in C#.
 *
 * It creates an Environment with three Databases - one for the Customers,
 * one for Orders, and a third for managing the 1:n relationship between
 * the other two.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Upscaledb;

namespace SampleEnv3
{
    /*
     * a Customer class
     */
    public struct Customer
    {
        public Customer(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public Customer(byte[] id, byte[] name) {
            this.id = BitConverter.ToInt32(id, 0);
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            this.name = enc.GetString(name);
        }

        public int id;
        public string name;

        public byte[] GetKey() {
            return BitConverter.GetBytes(id);
        }

        public byte[] GetRecord() {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            return enc.GetBytes(name);
        }
    }

    /*
     * An Order class; it stores the ID of the Customer,
     * and the name of the employee who is assigned to this order
     */
    public class Order
    {
        public Order(int id, int customerId, string assignee) {
            this.id = id;
            this.customerId = customerId;
            this.assignee = assignee;
        }

        public int id;
        public int customerId;
        public String assignee;

        public byte[] GetKey() {
            return BitConverter.GetBytes(id);
        }

        public byte[] GetCustomerKey() {
            return BitConverter.GetBytes(customerId);
        }

        public byte[] GetRecord() {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            return enc.GetBytes(assignee);
        }
    }

    class Program
    {
        const int DBIDX_CUSTOMER = 0;
        const int DBIDX_ORDER    = 1;
        const int DBIDX_C2O      = 2;

        const short DBNAME_CUSTOMER = 1;
        const short DBNAME_ORDER    = 2;
        const short DBNAME_C2O      = 3;

        static void Main(string[] args) {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            Upscaledb.Environment env = new Upscaledb.Environment();
            Database[] db = new Database[3];
            Cursor[] cursor = new Cursor[3];

            /*
             * set up the customer and order data - these arrays will later
             * be inserted into the Databases
             */
            Customer[] customers = new Customer[4];
            customers[0] = new Customer(1, "Alan Antonov Corp.");
            customers[1] = new Customer(2, "Barry Broke Inc.");
            customers[2] = new Customer(3, "Carl Caesar Lat.");
            customers[3] = new Customer(4, "Doris Dove Brd.");

            Order[] orders = new Order[8];
            orders[0] = new Order(1, 1, "Joe");
            orders[1] = new Order(2, 1, "Tom");
            orders[2] = new Order(3, 3, "Joe");
            orders[3] = new Order(4, 4, "Tom");
            orders[4] = new Order(5, 3, "Ben");
            orders[5] = new Order(6, 3, "Ben");
            orders[6] = new Order(7, 4, "Chris");
            orders[7] = new Order(8, 1, "Ben");

            /*
             * Create a new Environment
             */
            env.Create("test.db");

            /*
             * then create the three Databases in this Environment; each Database
             * has a name - the first is our "customer" Database, the second
             * is for the "orders"; the third manages our 1:n relation and
             * therefore needs to enable duplicate keys
             */
            db[DBIDX_CUSTOMER] = env.CreateDatabase(DBNAME_CUSTOMER);
            db[DBIDX_ORDER]    = env.CreateDatabase(DBNAME_ORDER);
            db[DBIDX_C2O]      = env.CreateDatabase(DBNAME_C2O,
                    UpsConst.UPS_ENABLE_DUPLICATE_KEYS);

            /*
             * create a Cursor for each Database
             */
            cursor[DBIDX_CUSTOMER] = new Cursor(db[DBIDX_CUSTOMER]);
            cursor[DBIDX_ORDER] = new Cursor(db[DBIDX_ORDER]);
            cursor[DBIDX_C2O] = new Cursor(db[DBIDX_C2O]);

            /*
             * Insert the customers in the customer Database
             *
             * INSERT INTO customers VALUES (1, "Alan Antonov Corp.");
             * INSERT INTO customers VALUES (2, "Barry Broke Inc.");
             * etc.
             */
            for (int i = 0; i < customers.GetLength(0); i++) {
                byte[] key = customers[i].GetKey();
                byte[] rec = customers[i].GetRecord();

                db[DBIDX_CUSTOMER].Insert(key, rec);
            }

            /*
             * Insert the orders in the order Database
             *
             * INSERT INTO orders VALUES (1, "Joe");
             * INSERT INTO orders VALUES (2, "Tom");
             * etc.
             */
            for (int i = 0; i < orders.GetLength(0); i++) {
                byte[] key = orders[i].GetKey();
                byte[] rec = orders[i].GetRecord();

                db[DBIDX_ORDER].Insert(key, rec);
            }

            /*
             * and now the 1:n relationships; the flag UPS_DUPLICATE creates
             * a duplicate key, if the key already exists
             *
             * INSERT INTO c2o VALUES (1, 1);
             * INSERT INTO c2o VALUES (2, 1);
             * etc
             */
            for (int i = 0; i < orders.GetLength(0); i++) {
                byte[] key = orders[i].GetCustomerKey();
                byte[] rec = orders[i].GetKey();

                db[DBIDX_C2O].Insert(key, rec, UpsConst.UPS_DUPLICATE);
            }

            /*
             * now start the queries - we want to dump each customer and
             * his orders
             *
             * loop over the customer; for each customer, loop over the
             * 1:n table and pick those orders with the customer id.
             * then load the order and print it
             *
             * the outer loop is similar to
             * SELECT * FROM customers WHERE 1;
             */
            while (1 == 1) {
                Customer c;

                try {
                    cursor[DBIDX_CUSTOMER].MoveNext();
                }
                catch (DatabaseException e) {
                    // reached end of Database?
                    if (e.ErrorCode == UpsConst.UPS_KEY_NOT_FOUND)
                        break;
                    Console.Out.WriteLine("cursor.MoveNext failed: " + e);
                    return;
                }

                // load the customer
                c = new Customer(cursor[DBIDX_CUSTOMER].GetKey(),
                        cursor[DBIDX_CUSTOMER].GetRecord());

                // print information about this customer
                Console.Out.WriteLine("customer " + c.id + " ('" + c.name + "')");

                /*
                 * loop over the 1:n table
                 *
                 * SELECT * FROM customers, orders, c2o
                 *      WHERE c2o.customer_id=customers.id AND
                 *          c2o.order_id=orders.id;
                 */
                try {
                    cursor[DBIDX_C2O].Find(c.GetKey());
                }
                catch (DatabaseException e) {
                    // no order for this customer?
                    if (e.ErrorCode == UpsConst.UPS_KEY_NOT_FOUND)
                        continue;
                    Console.Out.WriteLine("cursor.Find failed: " + e);
                    return;
                }

                do {
                    /*
                     * load the order; orderId is a byteArray with the ID of the
                     * Order; the record of the item is a byteArray with the
                     * name of the assigned employee
                     *
                     * SELECT * FROM orders WHERE id = order_id;
                     */
                    byte[] orderId = cursor[DBIDX_C2O].GetRecord();
                    cursor[DBIDX_ORDER].Find(orderId);
                    String assignee = enc.GetString(cursor[DBIDX_ORDER].GetRecord());

                    Console.Out.WriteLine("  order: " + BitConverter.ToInt32(orderId, 0) +
                        " (assigned to " + assignee + ")");

                    /*
                     * move to the next order of this customer
                     *
                     * the flag UPS_ONLY_DUPLICATES restricts the cursor
                     * movement to the duplicates of the current key.
                     */
                    try {
                        cursor[DBIDX_C2O].MoveNext(UpsConst.UPS_ONLY_DUPLICATES);
                    }
                    catch (DatabaseException e) {
                        // no more orders for this customer?
                        if (e.ErrorCode == UpsConst.UPS_KEY_NOT_FOUND)
                            break;
                        Console.Out.WriteLine("cursor.MoveNext failed: " + e);
                        return;
                    }
                } while (1 == 1);
            }
        }
    }
}

