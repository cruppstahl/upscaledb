/* 
 * getopts()

  Copyright (C) 2006 Christoph Rupp, www.crupp.de

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the authors be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

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
    for (; options->shortopt; options++) {
        if (options->flags & GETOPTS_NEED_ARGUMENT)
            printf("    -%s, --%s:arg: %s\n", 
                options->shortopt, options->longopt, options->helpdesc);
        else
            printf("    -%s, --%s: %s\n", 
                options->shortopt, options->longopt, options->helpdesc);
    }
    printf("\n");
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
    else if (g_argv[g_a][0]=='-' || g_argv[g_a][0]=='/') {
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
