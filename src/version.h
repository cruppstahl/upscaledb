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

#ifndef HAM_VERSION_H__
#define HAM_VERSION_H__

/*
 * The version numbers
 *
 * @remark A change of the major revision means a significant update
 * with a lot of new features and API changes.
 * 
 * The minor version means a significant update without API changes, and the 
 * revision is incremented for each release with minor improvements only.
 *
 * The file version describes the version of the binary database format.
 * hamsterdb is neither backwards- nor forwards-compatible regarding file
 * format changes. 
 *
 * If a file was created with hamsterdb pro then the msb of the file version
 * is set. hamsterdb pro is able to open files created with hamsterdb (APL
 * version), but not vice versa.
 *
 * History of file versions:
 *   2.1.0: introduced the file version; version is 0
 *   2.1.3: new btree format, file format cleanups; version is 1
 *   2.1.4: new btree format for duplicate keys/var. length keys; version is 2
 *   2.1.5: new freelist; version is 3
 *   2.1.8: changes in btree node format; version is 4
 */
#define HAM_VERSION_MAJ     2
#define HAM_VERSION_MIN     1
#define HAM_VERSION_REV     7
#define HAM_FILE_VERSION    4
#define HAM_VERSION_STR     "2.1.7"

#endif /* HAM_VERSION_H__ */
