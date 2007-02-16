# Microsoft Developer Studio Project File - Name="lib" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Static Library" 0x0104

CFG=lib - Win32 Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "lib.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "lib.mak" CFG="lib - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "lib - Win32 Release" (based on "Win32 (x86) Static Library")
!MESSAGE "lib - Win32 Debug" (based on "Win32 (x86) Static Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
RSC=rc.exe

!IF  "$(CFG)" == "lib - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "Release"
# PROP Intermediate_Dir "Release"
# PROP Target_Dir ""
# ADD BASE CPP /nologo /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_MBCS" /D "_LIB" /YX /FD /c
# ADD CPP /nologo /W3 /GX- /O2 /Ob2 /I "..\..\..\include" /D "WIN32" /D "NDEBUG" /D "_MBCS" /D "_LIB" /D "HAM_LITTLE_ENDIAN" /YX /FD /c
# ADD BASE RSC /l 0x407 /d "NDEBUG"
# ADD RSC /l 0x407 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LIB32=link.exe -lib
# ADD BASE LIB32 /nologo
# ADD LIB32 /nologo /out:"..\..\..\build\release\libhamsterdb.lib"

!ELSEIF  "$(CFG)" == "lib - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "Debug"
# PROP BASE Intermediate_Dir "Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "Debug"
# PROP Intermediate_Dir "Debug"
# PROP Target_Dir ""
# ADD BASE CPP /nologo /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_MBCS" /D "_LIB" /YX /FD /GZ  /c
# ADD CPP /nologo /W3 /Gm /GX /ZI /Od /I "..\..\..\include" /D "WIN32" /D "_DEBUG" /D "_MBCS" /D "_LIB" /D "HAM_LITTLE_ENDIAN" /YX /FD /GZ  /c
# ADD BASE RSC /l 0x407 /d "_DEBUG"
# ADD RSC /l 0x407 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LIB32=link.exe -lib
# ADD BASE LIB32 /nologo
# ADD LIB32 /nologo /out:"..\..\..\build\debug\libhamsterdb.lib"

!ENDIF 

# Begin Target

# Name "lib - Win32 Release"
# Name "lib - Win32 Debug"
# Begin Source File

SOURCE=..\..\..\src\backend.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\blob.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\blob.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_check.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_cursor.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_cursor.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_enum.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_erase.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_find.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\btree_insert.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\cache.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\cache.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\config.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\cursor.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\db.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\db.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\endian.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\error.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\error.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\extkeys.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\extkeys.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\freelist.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\freelist.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\hamsterdb.c
# End Source File
# Begin Source File

SOURCE=..\..\..\include\ham\hamsterdb.h
# End Source File
# Begin Source File

SOURCE=..\..\..\include\ham\hamsterdb_int.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\keys.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\keys.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\mem.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\mem.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\os.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\os_win32.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\packstart.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\packstop.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\page.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\page.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\txn.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\txn.h
# End Source File
# Begin Source File

SOURCE=..\..\..\include\ham\types.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\util.c
# End Source File
# Begin Source File

SOURCE=..\..\..\src\util.h
# End Source File
# Begin Source File

SOURCE=..\..\..\src\version.h
# End Source File
# End Target
# End Project
