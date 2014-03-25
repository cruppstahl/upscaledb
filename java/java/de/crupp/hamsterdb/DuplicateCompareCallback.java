/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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


