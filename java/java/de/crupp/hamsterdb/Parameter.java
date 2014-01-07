/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

package de.crupp.hamsterdb;

/**
 * A named Parameter
 * <p>
 * This Parameter class is used for functions like <code>Database.create</code>
 * or <code>Database.open</code> etc to pass variable length parameter lists.
 */
public class Parameter {

    /**
     * Constructor
     * <p>
     * Creates an empty Parameter
     */
    public Parameter() {
    }

    /**
     * Constructor
     * <p>
     * Creates a Parameter and assigns name and value
     */
    public Parameter(int name, long value) {
        this.name=name;
        this.value=value;
    }

    /**
     * Constructor
     * <p>
     * Same as above, but assigns a string value
     */
    public Parameter(int name, String value) {
        this.name=name;
        this.stringValue=value;
    }

    /** the name of the parameter; all names start with Const.HAM_PARAM_* */
    public int name;

    /** the value of the parameter */
    public long value;

    /** in some cases, we definitely need a string value (i.e. for
     * HAM_PARAM_GET_FILENAME. */
    public String stringValue;
}

