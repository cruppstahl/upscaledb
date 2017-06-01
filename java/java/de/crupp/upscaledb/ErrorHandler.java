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

public interface ErrorHandler {

  /**
   * The handleMessage method is called whenever a message
   * is emitted.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__static.html#ga7e9a7dfcb312d1407b69e3c1a1f0d71e">C documentation</a>
   *
   * @param level the debug level (0 = Debug, 1 = Normal, 3 = Fatal)
   * @param message the message
   */
  public void handleMessage(int level, String message);
}

