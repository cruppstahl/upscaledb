/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * Event logging
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */
 
#ifdef HAM_ENABLE_EVENT_LOGGING

#ifndef HAM_EVENTLOG_H
#define HAM_EVENTLOG_H

#include "0root/root.h"

#include <stdio.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

namespace EventLog {

// Creates an event log, overwriting any existing file. The filename
// will be <filename>.elog
extern void
create(const char *filename);

// Opens an existing event log. The filename will be <filename>.elog
extern void
open(const char *filename);

// Appends a printf-formatted string to the log
extern void
append(const char *tag, const char *format, ...);

// Converts a binary string to an escaped C literal
extern const char *
escape(const void *data, size_t size);

// A few helper macros
#  define EVENTLOG_CREATE    EventLog::create
#  define EVENTLOG_OPEN      EventLog::open
#  define EVENTLOG_APPEND    EventLog::append

} // namespace EventLog

} // namespace hamsterdb

#endif /* HAM_EVENTLOG_H */

#else /* !HAM_ENABLE_EVENT_LOGGING */

#  define EVENTLOG_CREATE    (void)
#  define EVENTLOG_OPEN      (void)
#  define EVENTLOG_APPEND    (void)

#endif /* HAM_ENABLE_EVENT_LOGGING */
