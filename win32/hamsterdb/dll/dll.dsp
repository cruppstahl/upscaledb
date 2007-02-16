# Microsoft Developer Studio Project File - Name="dll" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Dynamic-Link Library" 0x0102

CFG=dll - Win32 Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "dll.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "dll.mak" CFG="dll - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "dll - Win32 Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "dll - Win32 Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
MTL=midl.exe
RSC=rc.exe

!IF  "$(CFG)" == "dll - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "Release"
# PROP Intermediate_Dir "Release"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MT /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "DLL_EXPORTS" /YX /FD /c
# ADD CPP /nologo /MT /W3 /GX /O2 /Ob2 /I "..\..\..\include" /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "DLL_EXPORTS" /D "HAM_LITTLE_ENDIAN" /YX /FD /c
# ADD BASE MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x407 /d "NDEBUG"
# ADD RSC /l 0x407 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386 /out:"..\..\..\build\release\hamsterdb.dll"

!ELSEIF  "$(CFG)" == "dll - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "Debug"
# PROP BASE Intermediate_Dir "Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "Debug"
# PROP Intermediate_Dir "Debug"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MTd /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "DLL_EXPORTS" /YX /FD /GZ  /c
# ADD CPP /nologo /MTd /W3 /Gm /GX /ZI /Od /I "..\..\..\include" /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "DLL_EXPORTS" /D "HAM_LITTLE_ENDIAN" /YX /FD /GZ  /c
# ADD BASE MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x407 /d "_DEBUG"
# ADD RSC /l 0x407 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /debug /machine:I386 /pdbtype:sept
# ADD LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /map /debug /machine:I386 /out:"..\..\..\build\debug\hamsterdb.dll" /pdbtype:sept

!ENDIF 

# Begin Target

# Name "dll - Win32 Release"
# Name "dll - Win32 Debug"
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
