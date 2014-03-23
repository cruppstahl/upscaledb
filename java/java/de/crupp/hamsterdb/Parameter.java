/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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

