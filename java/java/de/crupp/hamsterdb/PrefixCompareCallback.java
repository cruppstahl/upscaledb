/**
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

public interface PrefixCompareCallback {

    /**
     * The compare method compares the prefixes of two keys - the
     * "left-hand side" (lhs) and the "right-hand side (rhs).
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga0c927202b76d129e6a7055c9f51436d9">C documentation</a>
     *
     * @param lhs The prefix of the first key
     * @param lhs_realsize The real size of the first key
     * @param rhs The prefix of the second key
     * @param rhs_realsize The real size of the second key
     *
     * @return -1 if the first key is smaller, +1 if the first key
     *     is larger, 0 if both keys are equal, or
     *     <code>Const.HAM_PREFIX_REQUEST_FULLKEY</code> if the prefixes are
     *     not sufficient for the comparison
     */
    public int compare(byte[] lhs, int lhs_realsize,
            byte[] rhs, int rhs_realsize);
}


