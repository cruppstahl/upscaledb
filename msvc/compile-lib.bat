@echo off

@rem
@rem
@rem This script builds a static lib in debug mode. just execute it from 
@rem this directory. 
@rem

mkdir ..\build

cl.exe /DWIN32 /D_DEBUG /DDEBUG /DHAM_LITTLE_ENDIAN /I..\include /c /GZ /GS /RTCs /RTCu /RTCc /RTC1 /Zi /MLd /TC ..\src\blob.c ..\src\btree_find.c ..\src\extkeys.c ..\src\btree.c ..\src\btree_insert.c ..\src\freelist.c ..\src\os_win32.c ..\src\btree_check.c ..\src\cache.c ..\src\hamsterdb.c ..\src\page.c ..\src\btree_enum.c ..\src\db.c ..\src\keys.c ..\src\txn.c ..\src\btree_erase.c ..\src\error.c ..\src\mem.c ..\src\util.c 

lib.exe -out:..\build\libhamsterdb.lib *.obj
