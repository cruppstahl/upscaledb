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

