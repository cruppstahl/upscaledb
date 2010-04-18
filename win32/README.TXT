
hamsterdb Win32 build environment              Sun Apr 18 21:55:09 WEST 2010
(C) Christoph Rupp, chris@crupp.de; http://www.hamsterdb.com

1. Overview

The Win32 build environment is a Solution file for Microsoft Visual C++ 2008 
(win32/hamsterdb.sln). It compiles a static and a dynamic library, the samples
and the unittest. 

All libraries and executable files are built in the 'out'-directory. 
Intermediate files are in the 'tmp'-directory.

The static library is built in 'out/lib[_debug][_x64]/libhamsterdb.lib', 
the dynamic library is in 'out/dll[_debug][_x64]/hamsterdb.dll'. Import
libraries and pdb files for debug builds are also provided. 
All libraries can be downloaded separately from the hamsterdb webpage.

To download Microsoft Visual C++ 2008 Express Edition for free, go to
http://msdn.microsoft.com/vstudio/express/visualc/default.aspx.

2. Precompiled Libraries

Precompiled libraries are usually available from http://hamsterdb.com/download.

The precompiled libraries for x86 and x86_64 are built with Microsoft
Visual Studio 2008. In order to use the executables (i.e. tools), you have 
to install the Microsoft Visual C++ 2008 Redistributable Package (x86) 
from here: 
http://www.microsoft.com/downloads/details.aspx?FamilyID=9B2DA534-3E03-4391-8A4D-074B9F2BC1BF.

3. Contact

Author of hamsterdb is 
    Christoph Rupp
    Paul-Preuﬂ-Straﬂe 63
    80995 Muenchen/Germany
    email: chris@crupp.de
    web: http://www.hamsterdb.com
