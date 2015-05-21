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

#ifdef HAM_ENABLE_EVENT_LOGGING

// Locks the EventLog; used by the helper macros below
extern void
lock();

// Unlocks the EventLog; used by the helper macros below
extern void
unlock();

// Creates an event log, overwriting any existing file. The filename
// will be <filename>.elog
extern void
create(const char *filename);

// Opens an existing event log. The filename will be <filename>.elog
extern void
open(const char *filename);

// Closes an existing event log.
extern void
close(const char *filename);

// Appends a printf-formatted string to the log
extern void
append(const char *filename, const char *tag, const char *format, ...);

// Converts a binary string to an escaped C literal
extern const char *
escape(const void *data, size_t size);

// A few helper macros
#  define EVENTLOG_CREATE(f)    do {                            \
                                  EventLog::lock();             \
                                  EventLog::create(f);          \
                                  EventLog::unlock();           \
                                } while (0)

#  define EVENTLOG_OPEN(f)      do {                            \
                                  EventLog::lock();             \
                                  EventLog::open(f);            \
                                  EventLog::unlock();           \
                                } while (0)

#  define EVENTLOG_CLOSE(f)     do {                            \
                                  EventLog::lock();             \
                                  EventLog::close(f);           \
                                  EventLog::unlock();           \
                                } while (0)

#  define EVENTLOG_APPEND(x)    do {                            \
                                  EventLog::lock();             \
                                  EventLog::append x;           \
                                  EventLog::unlock();           \
                                } while (0)

#else /* !HAM_ENABLE_EVENT_LOGGING */

#  define EVENTLOG_CREATE(x)    (void)0
#  define EVENTLOG_OPEN(x)      (void)0
#  define EVENTLOG_APPEND(x)    (void)0

inline const char *
escape(const void *data, size_t size)
{
  return (0);
}

#endif /* HAM_ENABLE_EVENT_LOGGING */

} // namespace EventLog

} // namespace hamsterdb

#endif /* HAM_EVENTLOG_H */

