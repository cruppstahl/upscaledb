/* 
 * getopts()

  Copyright (C) 2005-2007 Christoph Rupp, www.crupp.de

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

/**
 * getopts() is a small library for reading and parsing command line 
 * parameters. It supports
 *
 * - options with a short- and a long name
 *      i.e. an option with the short name "h" and the long name "help"
 *      can be used in the following ways:
 *      "-h", "/h" or "--help"
 *
 *  - options with a parameter
 *      i.e. an option "input" (short "in") with a parameter "filename" can 
 *      be used like this:
 *      "-in <filename>", "/in <filename>", "--input:<filename>", 
 *      "--input=<filename>" or "--input <filename>"
 *
 *  - parameters (without an option)
 *      i.e. grep accepts several options, and the last command line 
 *      parameter usually is the filename:
 *      "grep -i needle haystack.txt"
 *      in this case the filename would be the parameter.
 *
 *  getopts() also provides the function getopts_usage(), which generates 
 *  the "help-screen" and prints all options and arguments.
 *
 *  getopts() works with an option-table, which is described below. An 
 *  example for a function, which accepts two options:
 *
 *      -h/--help for the help screen
 *      -f/--file for the filename (this option needs an argument)
 *
 *  In this case, the table would look like this:
 *
  option_t opts[]={
    { 
        ARG_HELP,               // symbolic name of this option
        "h",                    // short option 
        "help",                 // long option 
        "this help screen",     // help string
        0 },                    // no flags
    { 
        ARG_FILE,               // symbolic name of this option
        "f",                    // short option
        "file",                 // long option
        "<filename> input file name",   // help string
        GETOPTS_NEED_ARGUMENT }, // this option needs an argument

    { 0, 0, 0, 0, 0 }           // the terminating entry
  };
 *
 * In this example, the constants ARG_HELP and ARG_FILE could be defined as 1 
 * or 2 (you can use any number but 0, and you should avoid the predefined
 * values, which are defined below).
 *
 * Your first step now would be to initialize getopts() with your argc/argv
 * values. The last parameter is your program name - it will appear in 
 * your help screen. You can use argv[0] or any other identifier.
 *
    getopts_init(argc, argv, "test");

 * Now the parameters can be checked with getopts(). getopts() will return
 * the symbolic name of the parameter, or one of the predefined constants
 * defined below (i.e. GETOPTS_UNKNOWN for an unknown parameter).
 
    unsigned int opt;
    char *param;
    while ((opt=getopts(&opts[0], &param))) {
        if (opt==ARG_HELP) {
            getopts_usage(&opts[0]);
        }
        else if (opt==ARG_FILE) {
            printf("getopt: file is %s\n", param);
        }
        else if (opt==GETOPTS_UNKNOWN) {
            printf("getopt: unknown parameter %s\n", param);
        }
        // etc...
    }

 * Note that the variable 'param' receives the parameter of the --file-option.
 *
 * In case of an unknown parameter, 'param' will receive the value of the
 * unknown parameter.
 *
 */

#ifndef GETOPTS_H__
#define GETOPTS_H__

#ifdef __cplusplus
extern "C" {
#endif

/**
 * a structure which describes an option; getopts() expects an array
 * of this structure, with the last entry of this array having only
 * 0-entries.
 */
typedef struct 
{
    /**
     * the "name", or identifier, of this option. It's also the return
     * value of getopts(). Some values are predefined and used by
     * getopts() (see below for GETOPT_UNKNOWN etc). And you should not 
     * use zero as a name.
     */
    unsigned int name;

    /**
     * the short option string, i.e. "f" for "-f" and "/f"
     */
    const char *shortopt;

    /**
     * the long option string, i.e. "file" for "--file"
     */
    const char *longopt; 

    /**
     * the help description, printed by getopts_usage
     */
    const char *helpdesc;

    /**
     * flags of this entry; see below
     */
    unsigned int flags;

} option_t;

/**
 * an option_t flag; set this flag if this option needs a parameter. 
 * the following syntaxes are supported:
 * "program -f <filename>" or "program --file:<filename>" or 
 * "program --file=<filename>" or "program --file <filename>".
 */
#define GETOPTS_NEED_ARGUMENT                 1

/**
 * getopts_init()
 *
 * @parameter argc argc-parameter of your main()-function
 * @parameter argv argv-parameter of your main()-function
 * @parameter program the name of the program, usually argv[0]
 *
 * @remark initialize the getopts-function
 *
 * @remark argc/argv are stored statically, and therefore getopts()
 * is NOT thread-safe!
 */
extern void
getopts_init(int argc, char **argv, const char *program);

/**
 * getopts_usage()
 *
 * @parameter options array of option_t elements; the last element is zero'd 
 *
 * @remark prints a help screen
 */
extern void
getopts_usage(option_t *options);

/**
 * getopts()
 *
 * @parameter options array of option_t elements; the last element is zero'd 
 * @parameter param will point to the argument of an option
 *
 * @return returns the name of the next element, or one of the 
 * predefined return values below, or 0 if there are no more parameters.
 *
 */
extern unsigned int 
getopts(option_t *options, char **param);

/**
 * return value of getopts(), if you forgot to call getopts_init()
 */
#define GETOPTS_NO_INIT              0xffffffff

/**
 * return value of getopts() for unknown options
 */
#define GETOPTS_UNKNOWN              0xfffffffe

/**
 * return value of getopts() if user entered an option which expected
 * a parameter, but the parameter is missing
 */
#define GETOPTS_MISSING_PARAM        0xfffffffc

/**
 * found a parameter which is not an option (i.e. doesn't start with 
 * '--', '-' or '/'. 
 * i.e. in "grep pattern", "pattern" is not an option but a simple
 * parameter.
 */
#define GETOPTS_PARAMETER            0xfffffffb

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* GETOPTS_H__ */
