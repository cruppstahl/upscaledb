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

public interface CompareCallback {

    /**
     * The compare method compares two keys - the "left-hand side"
     * (lhs) and the "right-hand side" (rhs).
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gadb18cf3c921760a08081af2721860495">C documentation</a>
     *
     * @param lhs The first key
     * @param rhs The second key
     * @return -1 if the first key is smaller, +1 if the first key
     *     is larger, 0 if both keys are equal
     */
    public int compare(byte[] lhs, byte[] rhs);
}

