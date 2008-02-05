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
import java.io.*;

public class Env3 {

    /*
     * a Customer class
     */
    private class Env3Customer 
            implements Serializable {
        public Env3Customer(int id, String name) {
            this.id=id;
            this.name=name;
        }
    
        public int id;              /*< the unique Customer ID */
        public String name;         /*< the Customer's name */
        // ...

        public byte[] getKey() {
            // return the id as a byte array
            return Env3.itob(id);
        }

        public byte[] getRecord() {
            // TODO return the whole class as a byte array
            return Env3.itob(id);
        }
    }

    /*
     * an Order class; it stores the ID of the Customer,
     * and the name of the employee who is assigned to this order
     */
    private class Env3Order 
            implements Serializable {
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
            // TODO
            return new byte[15];
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

    static final int DBNAME_CUSTOMER   = 1;
    static final int DBNAME_ORDER      = 2;
    static final int DBNAME_C2O        = 3;  /* C2O: Customer To Order */

    static final int DBIDX_CUSTOMER    = 0;
    static final int DBIDX_ORDER       = 1;
    static final int DBIDX_C2O         = 2;

    public void run() 
            throws de.crupp.hamsterdb.Error {
        Environment env=new Environment();
        Database db[]=new Database[MAX_DBS];
        Cursor cursor[]=new Cursor[MAX_DBS];

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
         * create a new Environment file
         */
        env.create("test.db");

        /*
         * then create the two Databases in this Environment; each Database
         * has a name - the first is our "customer" Database, the second 
         * is for the "orders"; the third manages our 1:n relation and
         * therefore needs to enable duplicate keys
         */
        db[DBIDX_CUSTOMER]=env.createDatabase((short)DBNAME_CUSTOMER);
        db[DBIDX_ORDER]   =env.createDatabase((short)DBNAME_ORDER);
        db[DBIDX_C2O]     =env.createDatabase((short)DBNAME_C2O, 
                Const.HAM_ENABLE_DUPLICATES);

        /* 
         * create a Cursor for each Database
         */
        for (int i=0; i<MAX_DBS; i++)
            cursor[i]=new Cursor(db[i]);

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

            db[0].insert(key, rec);
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

            db[1].insert(key, rec);
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

            db[2].insert(key, rec, Const.HAM_DUPLICATE);
        }

        /* TODO ... */
        System.out.println("Success!");
    }

	public static void main(String args[]) {
		try {
            Env3 env3=new Env3();
            env3.run();
        }
		catch (de.crupp.hamsterdb.Error err) {
			System.out.println("Exception "+err);
		}
	}
}

