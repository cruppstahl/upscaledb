/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#include <stdio.h>
#include <string.h>
#include "getopts.h"

static int g_a=0;
static int g_argc=0;
static char **g_argv=0;
static const char *g_program=0;

void
getopts_init(int argc, char **argv, const char *program)
{
  g_a=0;
  g_argc=argc-1;
  g_argv=argv+1;
  g_program=program;
}

void
getopts_usage(option_t *options)
{
  printf("usage: %s <options>\n", g_program);
  for (; options->shortopt || options->longopt; options++) {
    if (options->flags & GETOPTS_NEED_ARGUMENT) {
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

unsigned int
getopts(option_t *options, char **param)
{
  char *p;
  option_t *o=options;

  if (!g_argv)
    return (GETOPTS_NO_INIT);

  if (g_a>=g_argc || g_argv[g_a]==0)
    return (0);

  /*
   * check for a long option with --
   */
  if (g_argv[g_a][0]=='-' && g_argv[g_a][1]=='-') {
    *param=&g_argv[g_a][2];
    for (; o->longopt; o++) {
      int found=0;
      if (!strcmp(o->longopt, &g_argv[g_a][2]))
        found=1;
      else if (strstr(&g_argv[g_a][2], o->longopt)==&g_argv[g_a][2]) {
        if (g_argv[g_a][2+strlen(o->longopt)]==':')
          found=1;
        else if (g_argv[g_a][2+strlen(o->longopt)]=='=')
          found=1;
      }
      if (found) {
        if (o->flags & GETOPTS_NEED_ARGUMENT) {
          p=strchr(&g_argv[g_a][1], ':');
          if (p) {
            *param=p+1;
            g_a++;
            return (o->name);
          }
          p=strchr(&g_argv[g_a][1], '=');
          if (p) {
            *param=p+1;
            g_a++;
            return (o->name);
          }
          if (g_a==g_argc)
            return (GETOPTS_MISSING_PARAM);
          *param=g_argv[g_a+1];
          g_a++;
        }
        g_a++;
        return (o->name);
      }
    }
    return (GETOPTS_UNKNOWN);
  }

  /*
   * check for a short option name
   */
  else if (g_argv[g_a][0]=='-') {
    *param=&g_argv[g_a][1];
    for (; o->shortopt; o++) {
      if (!strcmp(o->shortopt, &g_argv[g_a][1])) {
        if (o->flags & GETOPTS_NEED_ARGUMENT) {
          if (g_a==g_argc)
            return (GETOPTS_MISSING_PARAM);
          *param=g_argv[g_a+1];
          g_a++;
        }
        g_a++;
        return (o->name);
      }
    }
    return (GETOPTS_UNKNOWN);
  }

  if (param)
    *param=g_argv[g_a];
  g_a++;
  return (GETOPTS_PARAMETER);
}
