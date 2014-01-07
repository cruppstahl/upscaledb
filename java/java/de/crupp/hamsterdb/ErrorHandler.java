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

public interface ErrorHandler {

    /**
     * The handleMessage method is called whenever a message
     * is emitted.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__static.html#gad2927b8e80c7bddb0a34a876c413a3c3">C documentation</a>
     *
     * @param level the debug level (0 = Debug, 1 = Normal, 3 = Fatal)
     * @param message the message
     */
    public void handleMessage(int level, String message);
}

