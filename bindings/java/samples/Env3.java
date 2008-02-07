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

/*
 * This sample is the implementation of /samples/env3.cpp in Java.
 *
 * It creates an Environment with three Databases - one for the Customers,
 * one for Orders, and a third for managing the 1:n relationship between
 * the other two.
 */

import de.crupp.hamsterdb.*;

public class Env3 {

    /*
     * a Customer class
     */
    private class Env3Customer {
        public Env3Customer(int id, String name) {
            this.id=id;
            this.name=name;
        }

        public Env3Customer(byte[] bid, byte[] bname) {
            this.id=Env3.btoi(bid);
            this.name=new String(bname);
        }
    
        public int id;              /*< the unique Customer ID */
        public String name;         /*< the Customer's name */
        // ...

        public byte[] getKey() {
            // return the id as a byte array
            return Env3.itob(id);
        }

        public byte[] getRecord() {
            // return the id and name as a byte array
            return name.getBytes();
        }
    }

    /*
     * an Order class; it stores the ID of the Customer,
     * and the name of the employee who is assigned to this order
     */
    private class Env3Order {
        public Env3Order(int id, int customer_id, String assignee) {
            this.id=id;
            this.customer_id=customer_id;
            this.assignee=assignee;
        }

        public int id;              /*< the unique Order ID */
        public int customer_id;     /*< the Customer of this Order */
        public String assignee;     /*< the Employee who is assigned to this 
                                      order */
        // ...

        public byte[] getKey() {
            // return the ID as a byte array
            return Env3.itob(id);
        }

        public byte[] getCustomerKey() {
            // return the customer ID as a byte array
            return Env3.itob(customer_id);
        }

        public byte[] getRecord() {
            // return the assignee as a byte array
            return assignee.getBytes();
        }
    }

    /*
     * store an integer in a byte array
     */
    public static byte[] itob(int i) {
        return new byte[] {
            (byte)(i >>> 24),
            (byte)(i >>> 16),
            (byte)(i >>> 8),
            (byte) i};
    }

    /*
     * read an integer from a byte array
     */
    public static int btoi(byte[] b) {
        return (b[0] << 24)
            + ((b[1] & 0xFF) << 16)
            + ((b[2] & 0xFF) << 8)
            + ( b[3] & 0xFF); 
    }

    static final int MAX_DBS           = 3;

    static final short DBNAME_CUSTOMER = 1;
    static final short DBNAME_ORDER    = 2;
    static final short DBNAME_C2O      = 3;  /* C2O: Customer To Order */

    static final int DBIDX_CUSTOMER    = 0;
    static final int DBIDX_ORDER       = 1;
    static final int DBIDX_C2O         = 2;

    private Environment m_env;
    private Database m_db[];
    private Cursor m_cursor[];

    public void init() 
            throws de.crupp.hamsterdb.Error {
        m_env=new Environment();
        m_db=new Database[MAX_DBS];
        m_cursor=new Cursor[MAX_DBS];

        /*
         * create a new Environment file
         */
        m_env.create("test.db");

        /*
         * then create the two Databases in this Environment; each Database
         * has a name - the first is our "customer" Database, the second 
         * is for the "orders"; the third manages our 1:n relation and
         * therefore needs to enable duplicate keys
         */
        m_db[DBIDX_CUSTOMER]=m_env.createDatabase(DBNAME_CUSTOMER);
        m_db[DBIDX_ORDER]   =m_env.createDatabase(DBNAME_ORDER);
        m_db[DBIDX_C2O]     =m_env.createDatabase(DBNAME_C2O, 
                Const.HAM_ENABLE_DUPLICATES);

        /* 
         * create a Cursor for each Database
         */
        for (int i=0; i<MAX_DBS; i++)
            m_cursor[i]=new Cursor(m_db[i]);
    }

    public void close() 
            throws de.crupp.hamsterdb.Error {
        /*
         * close all Cursors, Databases and the Environment
         */
        for (int i=0; i<MAX_DBS; i++)
            m_cursor[i].close();
        for (int i=0; i<MAX_DBS; i++)
            m_db[i].close();
        m_env.close();
    }

    public void run() 
            throws de.crupp.hamsterdb.Error {

        Env3Customer customers[]={
            new Env3Customer(1, "Alan Antonov Corp."),
            new Env3Customer(2, "Barry Broke Inc."),
            new Env3Customer(3, "Carl Caesar Lat."),
            new Env3Customer(4, "Doris Dove Brd.")
        };

        Env3Order orders[]={
            new Env3Order(1, 1, "Joe"),
            new Env3Order(2, 1, "Tom"),
            new Env3Order(3, 3, "Joe"),
            new Env3Order(4, 4, "Tom"),
            new Env3Order(5, 3, "Ben"),
            new Env3Order(6, 3, "Ben"),
            new Env3Order(7, 4, "Chris"),
            new Env3Order(8, 1, "Ben")
        };

        /*
         * insert the customers in the customer table
         *
         * INSERT INTO customers VALUES (1, "Alan Antonov Corp.");
         * INSERT INTO customers VALUES (2, "Barry Broke Inc.");
         * etc
         */
        for (int i=0; i<customers.length; i++) {
            byte[] key=customers[i].getKey();
            byte[] rec=customers[i].getRecord();

            m_db[DBIDX_CUSTOMER].insert(key, rec);
        }

        /*
         * and now the orders in the second Database; contrary to env1, 
         * we only store the assignee, not the whole structure
         *
         * INSERT INTO orders VALUES (1, "Joe");
         * INSERT INTO orders VALUES (2, "Tom");
         */
        for (int i=0; i<orders.length; i++) {
            byte[] key=orders[i].getKey();
            byte[] rec=orders[i].getRecord();

            m_db[DBIDX_ORDER].insert(key, rec);
        }

        /*
         * and now the 1:n relationships; the flag HAM_DUPLICATE creates
         * a duplicate key, if the key already exists
         *
         * INSERT INTO c2o VALUES (1, 1);
         * INSERT INTO c2o VALUES (2, 1);
         * etc
         */
        for (int i=0; i<orders.length; i++) {
            byte[] key=orders[i].getCustomerKey();
            byte[] rec=orders[i].getKey();

            m_db[DBIDX_C2O].insert(key, rec, Const.HAM_DUPLICATE);
        }

        /*
         * now start the query - we want to dump each customer with his
         * orders
         *
         * loop over the customer; for each customer, loop over the 1:n table
         * and pick those orders with the customer id. then load the order
         * and print it
         *
         * the outer loop is similar to 
         * SELECT * FROM customers WHERE 1;
         */
        while (1==1) {
            Env3Customer customer;

            try {
                m_cursor[DBIDX_CUSTOMER].moveNext();
            }
            catch (de.crupp.hamsterdb.Error e) {
                // reached end of Database? 
                if (e.getErrno()==Const.HAM_KEY_NOT_FOUND)
                    break;
                System.out.println("cursor.moveNext failed: "+e);
                return;
            }

            // load the Customer
            customer=new Env3Customer(m_cursor[DBIDX_CUSTOMER].getKey(),
                    m_cursor[DBIDX_CUSTOMER].getRecord());

            // print information about this Customer
            System.out.println("customer "+customer.id+" ('"+
                    customer.name+"')");

            /*
             * loop over the 1:n table
             *
             * before we start the loop, we move the cursor to the
             * first duplicate key
             *
             * SELECT * FROM customers, orders, c2o 
             *   WHERE c2o.customer_id=customers.id AND
             *      c2o.order_id=orders.id;
             */
            try {
                m_cursor[DBIDX_C2O].find(customer.getKey());
            }
            catch (de.crupp.hamsterdb.Error e) {
                // no order for this Customer?
                if (e.getErrno()==Const.HAM_KEY_NOT_FOUND)
                    continue;
                System.out.println("cursor.find failed: "+e);
                return;
            }

            do {
                /* 
                 * load the order; order_id is a byteArray with the ID of the
                 * Order; the record of the item is a byteArray with the
                 * name of the assigned employee
                 *
                 * SELECT * FROM orders WHERE id = order_id;
                 */
                byte[] order_id=m_cursor[DBIDX_C2O].getRecord();
                m_cursor[DBIDX_ORDER].find(order_id);
                String assignee=new String(m_cursor[DBIDX_ORDER].getRecord());

                System.out.println("  order: "+btoi(order_id)+" (assigned to "+
                        assignee+")");
                
                /*
                 * move to the next order which belongs to this customer
                 *
                 * the flag HAM_ONLY_DUPLICATES restricts the cursor 
                 * movement to the duplicate list.
                 */
                try {
                    m_cursor[DBIDX_C2O].move(Const.HAM_CURSOR_NEXT
                            |Const.HAM_ONLY_DUPLICATES);
                }
                catch (de.crupp.hamsterdb.Error e) {
                    // no more order for this customer?
                    if (e.getErrno()==Const.HAM_KEY_NOT_FOUND)
                        break;
                    System.out.println("cursor.moveNext failed: "+e);
                    return;
                }
            } while(1==1);
        }

        /* TODO ... */
        System.out.println("Success!");
    }

	public static void main(String args[]) {
		try {
            Env3 env3=new Env3();
            env3.init();
            env3.run();
            env3.close();
        }
		catch (de.crupp.hamsterdb.Error err) {
			System.out.println("Exception "+err);
		}
	}
}

