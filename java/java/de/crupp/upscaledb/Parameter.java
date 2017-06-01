/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

package de.crupp.upscaledb;

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

  /** the name of the parameter; all names start with Const.UPS_PARAM_* */
  public int name;

  /** the value of the parameter */
  public long value;

  /** in some cases, we definitely need a string value (i.e. for
   * UPS_PARAM_GET_FILENAME. */
  public String stringValue;
}

