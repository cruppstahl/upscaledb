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

#ifndef HAM_SERIAL_H__
#define HAM_SERIAL_H__

// the serial number; for MIT versions, this is always
// 0x0; only non-MIT versions ("hamsterdb pro") get a serial number.
#define HAM_SERIALNO                  0x0

// the name of the licensee; for MIT, this string is empty ("")
#define HAM_LICENSEE                  ""

// same as above, but as a readable string
#define HAM_PRODUCT_NAME              "hamsterdb embedded storage"

#endif /* HAM_SERIAL_H__ */
