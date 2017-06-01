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

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "getopts.h"

namespace Impl {

static int cur = 0;
static int argc = 0;
static char **argv = 0;
static const char *program = 0;

static bool
ISSET(unsigned int base, unsigned int bit) {
  return ((base & bit) == bit);
}

static bool
is_empty(const char *p) {
  return (*p == '\0');
}

static bool
starts_with(const char *p, const char *needle) {
  return (p == ::strstr(p, needle));
}

static bool
equals(const char *p, const char *q) {
  return (0 == ::strcmp(p, q));
}

template<typename T>
static T * // can handle const and non-const pointers
consume(T *p, const char *token) {
  assert(starts_with(p, token));
  return (p + ::strlen(token));
}

static const char *
consume(const char *p, int size) {
  return (p + size);
}

static bool
consume_option_by_name(const char **pp, const char *name) {
  const char *p = *pp;

  // either the parameter is equal to the name
  if (equals(p, name)) {
    *pp = consume(p, name);
    return (true);
  }

  // OR it starts with the name and is followed by ':' or '='
  if (starts_with(p, name)) {
    const char *q = consume(p, name);
    if (starts_with(q, ":") || starts_with(q, "=")) {
      *pp = consume(q, 1);
      return (true);
    }
  }

  return (false);
}

static option_t *
consume_option_by_longname(const char **pp, option_t *options) {
  for (; options->longopt; options++) {
    if (consume_option_by_name(pp, options->longopt))
      return (options);
  }

  throw (GETOPTS_UNKNOWN);
}

static option_t *
consume_option_by_shortname(const char **pp, option_t *options) {
  for (; options->shortopt; options++) {
    if (consume_option_by_name(pp, options->shortopt))
      return (options);
  }

  throw (GETOPTS_UNKNOWN);
}

static unsigned int
parse_parameter(const char *p, option_t *options, const char **param)
{
  option_t *o = 0;

  *param = 0;

  // check for a long option
  if (starts_with(p, "--")) {
    p = consume(p, "--");
    // search (and consume) the option
    o = consume_option_by_longname(&p, options);
  }

  // check for a short option
  else if (starts_with(p, "-")) {
    p = consume(p, "-");
    // search (and consume) the option
    o = consume_option_by_shortname(&p, options);
  }

  // if o == nullptr then the parameter neither started with '-' or '--'
  if (o == 0) {
    *param = p;
    return (GETOPTS_PARAMETER);
  }

  // are options required?
  if (ISSET(o->flags, GETOPTS_NEED_ARGUMENT) && p == 0)
    return (GETOPTS_MISSING_PARAM);

  *param = p;

  return (o->name);
}

static void
getopts_init(int argc_, char **argv_, const char *program_)
{
  cur = 0;
  argc = argc_ - 1;
  argv = argv_ + 1;
  program = program_;
}

static void
getopts_usage(option_t *options)
{
  printf("usage: %s <options>\n", program);
  for (; options->shortopt || options->longopt; options++) {
    if (ISSET(options->flags, GETOPTS_NEED_ARGUMENT)) {
      if (options->shortopt)
        printf("  -%s, --%s=<arg>: %s\n",
               options->shortopt, options->longopt, options->helpdesc);
      else
        printf("  --%s=<arg>: %s\n",
               options->longopt, options->helpdesc);
    }
    else {
      if (options->shortopt)
        printf("  -%s, --%s: %s\n",
               options->shortopt, options->longopt, options->helpdesc);
      else
        printf("  --%s: %s\n",
               options->longopt, options->helpdesc);
    }
  }
}

static unsigned int
getopts(option_t *options, const char **param)
{
  // sanity checks
  if (!argv || !options || !param)
    return (GETOPTS_NO_INIT);

  if (cur >= argc || is_empty(argv[cur]))
    return (0);

  // fetch the next parameter from argv[]
  const char *p = argv[cur];
  cur++;

  // parse the parameter
  try {
    return (parse_parameter(p, options, param));
  }
  catch (unsigned int e) {
    // return integer error code
    return (e);
  }
}

} // namespace Impl

void
getopts_init(int argc, char **argv, const char *program)
{
  Impl::getopts_init(argc, argv, program);
}

void
getopts_usage(option_t *options)
{
  Impl::getopts_usage(options);
}

unsigned int
getopts(option_t *options, const char **param)
{
  return (Impl::getopts(options, param));
}

