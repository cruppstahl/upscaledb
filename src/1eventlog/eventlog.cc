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

#ifdef HAM_ENABLE_EVENT_LOGGING

#include "0root/root.h"

#include <stdio.h>
#include <stdarg.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/util.h"
#include "1base/spinlock.h"
#include "1eventlog/eventlog.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

namespace EventLog {

struct EventLogDesc {
  // Constructor
  EventLogDesc()
    : f(0) {
  }

  // Destructor
  ~EventLogDesc() {
    if (f)
      ::fclose(f);
    f = 0;
  }

  // For synchronization
  Spinlock mutex;

  // The file handle
  FILE *f;

  // temporary buffer for escaping incoming data
  char temp[1024 * 2];
};

static EventLogDesc event_log;

static void
open_or_create(const char *filename, const char *mode)
{
  ScopedSpinlock lock(event_log.mutex);
  if (event_log.f)
    ::fclose(event_log.f);

  std::string path(filename);
  path += ".elog";
  event_log.f = ::fopen(path.c_str(), mode);
  if (!event_log.f) {
    ham_trace(("failed to create event log: %s", ::strerror(errno)));
    throw Exception(HAM_IO_ERROR);
  }
}

void
create(const char *filename)
{
  open_or_create(filename, "w");
}

void
open(const char *filename)
{
  open_or_create(filename, "a+");
}

void
append(const char *tag, const char *format, ...)
{
  char buffer[1024 * 4];
  va_list ap;
  va_start(ap, format);
  util_vsnprintf(buffer, sizeof(buffer), format, ap);
  va_end(ap);

  ScopedSpinlock lock(event_log.mutex);
  ::fprintf(event_log.f, "%s(%s);\n", tag, buffer);
  ::fflush(event_log.f);
}

const char *
escape(const void *data, size_t size)
{
  ScopedSpinlock lock(event_log.mutex);

  const char *d = (const char *)data;
  char *out = &event_log.temp[0];
  *out = '\"';
  out++;
  for (size_t i = 0; i < size; i++) {
    if (isascii(d[i])) {
      *out = d[i];
      out++;
    }
    else {
      out += ::sprintf(out, "\\x%02x", (unsigned int)d[i]);
    }
  }
  *out = '\"';
  out++;
  *out = '\0';
  return (event_log.temp);
}

} // namespace EventLog

} // namespace hamsterdb

#endif /* HAM_ENABLE_EVENT_LOGGING */
