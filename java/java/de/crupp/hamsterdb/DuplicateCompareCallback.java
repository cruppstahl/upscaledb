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

public interface DuplicateCompareCallback {

    /**
     * The compare method compares two records -
     * "left-hand side" (lhs) and the "right-hand side (rhs).
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga7d8cd9266c8e802685ee467d3bb35b3b">C documentation</a>
     *
     * @param lhs The first record
     * @param rhs The second record
     * @return -1 if the first record is smaller, +1 if the first record
     *     is larger, 0 if both records are equal
     */
    public int compare(byte[] lhs, byte[] rhs);
}


