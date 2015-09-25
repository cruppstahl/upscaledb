/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
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
    this.name = name;
    this.value = value;
  }

  /**
   * Constructor
   * <p>
   * Same as above, but assigns a string value
   */
  public Parameter(int name, String value) {
    this.name = name;
    this.stringValue = value;
  }

  /** the name of the parameter; all names start with Const.HAM_PARAM_* */
  public int name;

  /** the value of the parameter */
  public long value;

  /** in some cases, we definitely need a string value (i.e. for
   * HAM_PARAM_GET_FILENAME. */
  public String stringValue;
}

