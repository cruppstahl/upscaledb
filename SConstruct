#
# Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
# see file LICENSE for licence information
#
# This is the SConstruct file for hamsterdb. To build hamsterdb, you 
# need SCons (www.scons.org). Run 'scons' to build hamsterdb, 
# 'scons install' to install it and 'scons -h' for help and a list of 
# available targets and options.
#


import os
import sys
import SCons

# ----------------------------------------------------------------------
# check if gcc version is < 4.1.1 - if yes, we can't use optimization,
# because it breaks hamsterdb
gccversion_test_src = """
#if __GNUC__
#    define GCC_VERSION (__GNUC__ * 10000 \
                     + __GNUC_MINOR__ * 100 \
                     + __GNUC_PATCHLEVEL__)
#    if GCC_VERSION<40101
#       error gcc version too old, disabling -O flags
#    endif
#endif /* __GNUC__ */

int main(void){ return 0; }
"""

def CheckGccVersion(context):
    context.Message("Checking GCC version... ")
    is_ok = context.TryCompile(gccversion_test_src, ".c")
    context.Result(is_ok)
    return is_ok

def CheckWordSize(context):
    context.Message("Checking word size (32bit or 64bit)... ")
    ret=context.TryRun("""
    #include <stdio.h>
    int main() { printf("%d", sizeof(void*)); return 0; }
    """, ".c")
    context.Result(ret[1])
    return ret[1]

# ----------------------------------------------------------------------
# a function for verbose output
def vprint(s):
    try:
        global env
        if env['verbose']: 
            print s
    except:
        pass

opts = Options('build.cfg', ARGUMENTS)

opts.AddOptions(
    BoolOption('debug',   'Compile with debug symbols', 0),
    BoolOption('static',  'Build a static library', 0),
    BoolOption('dynamic', 'Build a dynamic library', 1),
    BoolOption('profile', 'Enable profiling (only with gcc)', 0)
    )

# ----------------------------------------------------------------------
# global flags
include = ['.', '../include']
cflags  = ['-Wall', '-ansi']
env = Environment(CCFLAGS=cflags, CPPPATH=include, options=opts)
env['buildpath']='build/'

# ----------------------------------------------------------------------
# check the gcc version
conf=Configure(env, custom_tests={'CheckGccVersion': CheckGccVersion, 
                                  'CheckWordSize': CheckWordSize})

if conf.CheckGccVersion():
    vprint("Gcc version >= 4.1.1, compiling with -O3")
    conf.env['GCC_VERSION_OK']=True;

ws=conf.CheckWordSize()
if ws=='4':
    vprint("Compiling for a 32bit system")
    conf.env['WORDSIZE']=32;
if ws=='8':
    vprint("Compiling for a 64bit system")
    conf.env['WORDSIZE']=64;

env=conf.Finish()

# ----------------------------------------------------------------------
# help string
Help("""
This is the SConscript build file for hamsterdb. The following targets 
and options are supported:

Options (use with <option>=0 or <option>=1):
  - debug
        Compile with debug symbols (default: off)
  - static
        Build a static library (default: off)
  - dynamic
        Build a dynamic library (default: on)
  - profile
        Enable profiling (only with gcc - default: off)

Targets:
  - clean
        Cleans all intermediate files of the current build configuration.
  - allclean
        Cleans all intermediate files of ALL build configurations.
  - doc
        Uses doxygen to build the hamsterdb documentation.
  - install
        Installs header files and libraries. You might need administrator/root
        privileges.
  - (Default target)
        Builds the hamsterdb library; the library is stored in the /build-
        subdirectory.
""")

# ----------------------------------------------------------------------
# get the current platform
if not env.has_key('platform'):
    env['platform']=env['PLATFORM']
elif env['platform']=='linux':
    env['platform']='posix'

# ----------------------------------------------------------------------
# create preprocessor defines and compiler switches depending 
# on platform, endianness etc
if env['platform']=='posix':
    vprint("building for posix")
    env.Append(CCFLAGS=['-DHAM_OS_POSIX'])
else:
    vprint("building for win32")
    env.Append(CCFLAGS=['-DHAM_OS_WIN32'])

if env['debug']:
    vprint("debug build")
    env.Append(CCFLAGS=['-DHAM_DEBUG'])
    env['buildpath']+='dbg'
    env.Append(CCFLAGS=['-g'])
    env['suffix']='_dbg'
else:
    vprint("release build")
    env.Append(CCFLAGS=['-DHAM_RELEASE'])
    env['buildpath']+='rel'
    # if gcc<4.1.1: don't use -O!!
    if conf.env['GCC_VERSION_OK']:
        env.Append(CCFLAGS='-O3')
    env.Append(CCFLAGS=['-fomit-frame-pointer', \
        '-ffast-math', '-funit-at-a-time'])
    env['suffix']='_rel'

if env['WORDSIZE']==32:
    env.Append(CCFLAGS='-DHAM_32BIT')
if env['WORDSIZE']==64:
    env.Append(CCFLAGS='-DHAM_64BIT')

if env['profile']:
    print("profile")
    env['buildpath']+='_prof'
    env.Append(CCFLAGS=['-pg'])
    env['suffix']+='_prof'
else:
    print("no profile")

if env['static']:
    print("static")
    env['buildpath']+='_a'
    env.Append(CCFLAGS=['-fPIC']) # nur gcc auf AMD64!
    env['suffix']+='_prof'
else:
    print("dynamic")
    env['buildpath']+='_so'
    env.Append(CCFLAGS=['-fPIC']) 
    env['suffix']+='_so'

if sys.byteorder=='little':
    env['byteorder']='little'
    env.Append(CCFLAGS=['-DHAM_LITTLE_ENDIAN']) 
    vprint("system is little endian")
else:
    env['byteorder']='big'
    env.Append(CCFLAGS=['-DHAM_BIG_ENDIAN']) 
    vprint("system is big endian")

# ----------------------------------------------------------------------
# start the build process
Export('env vprint')

# ----------------------------------------------------------------------
# target 'clean' is just a wrapper for scons -c
if 'clean' in sys.argv[1:]:
    env.Command('clean', '', ['scons -c'])

# ----------------------------------------------------------------------
# target 'allclean' removes the whole build-directory
if 'allclean' in sys.argv[1:]:
    env.Command('allclean', '', ['\rm -rf ./build'])

# ----------------------------------------------------------------------
# target 'doc' runs doxygen
if 'doc' in sys.argv[1:]:
    env.Command('doc', 'Doxyfile', ['doxygen'])

# ----------------------------------------------------------------------
# main targets: build the library, and the sample
BuildDir('./build/%s' % env['buildpath'], './src', duplicate=0)
SConscript('src/SConscript')
if os.path.exists('samples'):
    SConscript('samples/SConscript')
if os.path.exists('tests'):
    SConscript('tests/SConscript')
