
hamsterdb Win32 build environment              Thu May 17 14:13:01 UTC 2007
(C) Christoph Rupp, chris@crupp.de; http://www.hamsterdb.com

The Win32 build environment is a Solution file for Microsoft Visual C++ 2005 
(win32/hamsterdb/hamsterdb.sln). It compiles a static and a dynamic library. 
The static library is built in '/win32/hamsterdb/release/libhamsterdb.lib', 
the dynamic library is in '/win32/hamsterdb/release/hamsterdb.dll'. An import
library is also provided. All libraries can be downloaded separately from 
the hamsterdb webpage.

The unittests need cppunit, available at
http://sourceforge.net/projects/cppunit. You will have to modify 
the unittest properties, and change the include path and library 
dependency for cppunit.

To download Microsoft Visual C++ 2005 Express Edition for free, go to
http://msdn.microsoft.com/vstudio/express/visualc/default.aspx.

