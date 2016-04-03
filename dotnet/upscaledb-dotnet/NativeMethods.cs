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

using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Upscaledb
{
  internal static class NativeMethods
  {
    static NativeMethods() {
        // Since managed assembly targets AnyCPU, at runtime we need to check which version of the native dll to load.
        // This assumes that the native dlls are in subdirectories called x64 and x86, as is the case when using the NuGet package.
        // See http://stackoverflow.com/questions/10852634/
        var subdir = (IntPtr.Size == 8) ? "x64" : "x86";
        if (System.IO.Directory.Exists(subdir)) {
            LoadLibrary(subdir + "/upscaledb-2.2.0.dll");
        }
        else
        {
            // Legacy behaviour: assume native dll is in same directory as managed assembly
        }
    }

    [DllImport("kernel32.dll")]
    private static extern IntPtr LoadLibrary(string dllToLoad);

    [StructLayout(LayoutKind.Sequential)]
    unsafe struct RecordStruct
    {
      public Int32 size;
      public byte *data;
      public Int32 flags;
    }

    [StructLayout(LayoutKind.Sequential)]
    unsafe struct KeyStruct
    {
      public Int16 size;
      public byte *data;
      public Int32 flags;
      public Int32 _flags;
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_set_error_handler",
      CallingConvention = CallingConvention.Cdecl)]
    static public extern void SetErrorHandler(ErrorHandler eh);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_strerror",
      CallingConvention=CallingConvention.Cdecl)]
    static public extern IntPtr StringErrorImpl(int error);

    static public string StringError(int error) {
      IntPtr s = StringErrorImpl(error);
      return System.Runtime.InteropServices.Marshal.PtrToStringAnsi(s);
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_get_version",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern void GetVersion(out int major, out int minor,
        out int revision);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_create",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvCreate(out IntPtr handle, String fileName,
        int flags, int mode, Parameter[] parameters);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_open",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvOpen(out IntPtr handle, String fileName,
        int flags, Parameter[] parameters);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_create_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvCreateDatabase(IntPtr handle,
        out IntPtr dbhandle, short name, int flags,
        Parameter[] parameters);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_open_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvOpenDatabase(IntPtr handle,
        out IntPtr dbhandle, short name, int flags,
        Parameter[] parameters);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_rename_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvRenameDatabase(IntPtr handle,
        short oldName, short newName);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_erase_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvEraseDatabase(IntPtr handle,
        short name, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_flush",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvFlush(IntPtr handle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_get_database_names",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvGetDatabaseNamesLow(IntPtr handle,
        IntPtr dbnames, ref int count);

    static public int EnvGetDatabaseNames(IntPtr handle, out short[] names) {
      // alloc space for 2000 database names
      int count = 2000;
      IntPtr array = Marshal.AllocHGlobal(2 * count);
      int st = EnvGetDatabaseNamesLow(handle, array, ref count);
      if (st != 0) {
        Marshal.FreeHGlobal(array);
        names = null;
        return st;
      }
      names = new short[count];
      Marshal.Copy(array, names, 0, count);
      Marshal.FreeHGlobal(array);
      return 0;
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvClose(IntPtr handle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_txn_begin",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int TxnBegin(out IntPtr txnhandle, IntPtr envhandle,
        String filename, IntPtr reserved, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_txn_commit",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int TxnCommit(IntPtr handle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_txn_abort",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int TxnAbort(IntPtr handle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_get_error",
      CallingConvention = CallingConvention.Cdecl)]
    static public extern int GetLastError(IntPtr handle);

    // TODO this is new, but lots of effort b/c of complex
    // marshalling. if you need this function pls drop me a mail.
/*
    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_get_parameters",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int GetParameters(IntPtr handle, Parameter[] parameters);
*/

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_get_env",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern IntPtr GetEnv(IntPtr handle);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_register_compare",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int RegisterCompare(String name,
        CompareFunc foo);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_env_select_range",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvSelectRange(IntPtr handle,
        String query, IntPtr begin, IntPtr end, out IntPtr result);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_set_compare_func",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int SetCompareFunc(IntPtr handle,
        CompareFunc foo);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_find",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int FindLow(IntPtr handle, IntPtr txnhandle,
        ref KeyStruct key, ref RecordStruct record, int flags);

    static public unsafe int Find(IntPtr handle, IntPtr txnhandle,
                ref byte[] keydata, ref byte[] recdata, int flags) {
      KeyStruct key = new KeyStruct();
      RecordStruct record = new RecordStruct();
      key.size = (short)keydata.Length;
      fixed (byte *bk = keydata) {
        key.data = bk;
        int st = FindLow(handle, txnhandle, ref key, ref record, flags);
        if (st != 0)
          return st;
        // I didn't find a way to avoid the copying...
        IntPtr recData = new IntPtr(record.data);
        byte[] newRecData = new byte[record.size];
        Marshal.Copy(recData, newRecData, 0, record.size);
        recdata = newRecData;
        // also copy the key data if approx. matching was requested
        if ((flags & (UpsConst.UPS_FIND_LT_MATCH | UpsConst.UPS_FIND_GT_MATCH)) != 0) {
          IntPtr keyData = new IntPtr(key.data);
          byte[] newKeyData = new byte[key.size];
          Marshal.Copy(keyData, newKeyData, 0, key.size);
          keydata = newKeyData;
        }
        return 0;
      }
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_insert",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int InsertLow(IntPtr handle, IntPtr txnhandle,
        ref KeyStruct key, ref RecordStruct record, int flags);

    static public unsafe int Insert(IntPtr handle, IntPtr txnhandle,
        byte[] keyData, byte[] recordData, int flags) {
      KeyStruct key = new KeyStruct();
      RecordStruct record = new RecordStruct();
      fixed (byte* br = recordData, bk = keyData) {
        record.data = br;
        record.size = recordData.GetLength(0);
        key.data = bk;
        key.size = (short)keyData.GetLength(0);
        return InsertLow(handle, txnhandle, ref key, ref record, flags);
      }
    }

    static public unsafe int InsertRecNo(IntPtr handle, IntPtr txnhandle,
        ref byte[] keydata, byte[] recordData, int flags)
    {
        KeyStruct key = new KeyStruct();
        RecordStruct record = new RecordStruct();
        fixed (byte* br = recordData)
        {
            record.data = br;
            record.size = recordData.GetLength(0);
            key.data = null;
            key.size = 0;
            int st = InsertLow(handle, txnhandle, ref key, ref record, flags);
            if (st != 0)
                return st;
            IntPtr keyData = new IntPtr(key.data);
            byte[] newKeyData = new byte[key.size];
            Marshal.Copy(keyData, newKeyData, 0, key.size);
            keydata = newKeyData;            
            return 0;
        }
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_erase",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int EraseLow(IntPtr handle, IntPtr txnhandle,
        ref KeyStruct key, int flags);

    static public unsafe int Erase(IntPtr handle, IntPtr txnhandle,
                byte[] data, int flags) {
      KeyStruct key = new KeyStruct();
      fixed (byte* b = data) {
        key.data = b;
        key.size = (short)data.GetLength(0);
        return EraseLow(handle, txnhandle, ref key, flags);
      }
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_count",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int GetCount(IntPtr handle, IntPtr txnhandle,
        int flags, out Int64 count);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_db_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int Close(IntPtr handle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_create",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorCreate(out IntPtr chandle, IntPtr dbhandle,
        IntPtr txnhandle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_clone",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorClone(IntPtr handle, out IntPtr clone);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        IntPtr key, IntPtr record, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        ref KeyStruct key, IntPtr record, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        IntPtr key, ref RecordStruct record, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        ref KeyStruct key, ref RecordStruct record, int flags);

    static public int CursorMove(IntPtr handle, int flags) {
      return CursorMoveLow(handle, IntPtr.Zero, IntPtr.Zero, flags);
    }

    static unsafe public byte[] CursorGetRecord(IntPtr handle, int flags) {
      RecordStruct record = new RecordStruct();
      int st = CursorMoveLow(handle, IntPtr.Zero, ref record, flags);
      if (st == 0) {
        // I didn't found a way to avoid the copying...
        IntPtr recData = new IntPtr(record.data);
        byte[] newArray = new byte[record.size];
        Marshal.Copy(recData, newArray, 0, record.size);
        return newArray;
      }
      throw new DatabaseException(st);
    }

    static unsafe public byte[] CursorGetKey(IntPtr handle, int flags) {
      KeyStruct key = new KeyStruct();
      int st = CursorMoveLow(handle, ref key, IntPtr.Zero, flags);
      if (st == 0) {
        // I didn't found a way to avoid the copying...
        IntPtr keyData = new IntPtr(key.data);
        byte[] newArray = new byte[key.size];
        Marshal.Copy(keyData, newArray, 0, key.size);
        return newArray;
      }
      throw new DatabaseException(st);
    }

    static unsafe public int CursorGet(IntPtr handle, int flags, ref byte[] keyArray, ref byte[] recordArray)
    {
        KeyStruct key = new KeyStruct();
        RecordStruct record = new RecordStruct();
        int st = CursorMoveLow(handle, ref key, ref record, flags);
        if (st == 0)
        {
            IntPtr keyData = new IntPtr(key.data);
            keyArray = new byte[key.size];
            Marshal.Copy(keyData, keyArray, 0, key.size);

            IntPtr recData = new IntPtr(record.data);
            recordArray = new byte[record.size];
            Marshal.Copy(recData, recordArray, 0, record.size);
        }
        return st;
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_overwrite",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorOverwriteLow(IntPtr handle,
        ref RecordStruct record, int flags);

    static unsafe public int CursorOverwrite(IntPtr handle, byte[] data, int flags) {
      RecordStruct record = new RecordStruct();
      fixed (byte* b = data) {
        record.data = b;
        record.size = data.GetLength(0);
        return CursorOverwriteLow(handle, ref record, flags);
      }
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_find",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorFindLow(IntPtr handle,
        ref KeyStruct key, ref RecordStruct record, int flags);

    static unsafe public int CursorFind(IntPtr handle, ref byte[] keydata,
                            ref byte[] recdata, int flags) {
      KeyStruct key = new KeyStruct();
      RecordStruct record = new RecordStruct();

      fixed (byte* bk = keydata) {
        key.data = bk;
        key.size = (short)keydata.Length;
        int st = CursorFindLow(handle, ref key, ref record, flags);
        if (st != 0)
          return st;
        // I didn't found a way to avoid the copying...
        IntPtr recData = new IntPtr(record.data);
        byte[] newRecData = new byte[record.size];
        Marshal.Copy(recData, newRecData, 0, record.size);
        recdata = newRecData;
        // also copy the key data if approx. matching was requested
        if ((flags & (UpsConst.UPS_FIND_LT_MATCH | UpsConst.UPS_FIND_GT_MATCH)) != 0) {
          IntPtr keyData = new IntPtr(key.data);
          byte[] newKeyData = new byte[key.size];
          Marshal.Copy(keyData, newKeyData, 0, key.size);
          keydata = newKeyData;
        }
        return 0;
      }
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_insert",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorInsertLow(IntPtr handle,
        ref KeyStruct key, ref RecordStruct record, int flags);

    static public unsafe int CursorInsert(IntPtr handle,
            byte[] keyData, byte[] recordData, int flags) {
      RecordStruct record = new RecordStruct();
      KeyStruct key = new KeyStruct();
      fixed (byte *br = recordData, bk = keyData) {
        record.data = br;
        record.size = recordData.GetLength(0);
        key.data = bk;
        key.size = (short)keyData.GetLength(0);
        return CursorInsertLow(handle, ref key, ref record, flags);
      }
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_erase",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorErase(IntPtr handle, int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_get_duplicate_count",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorGetDuplicateCount(IntPtr handle, out int count,
        int flags);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "ups_cursor_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorClose(IntPtr handle);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "uqi_result_get_row_count",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int ResultGetRowCount(IntPtr handle);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "uqi_result_get_key_type",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int ResultGetKeyType(IntPtr handle);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "uqi_result_get_record_type",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int ResultGetRecordType(IntPtr handle);

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "uqi_result_get_key",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern void ResultGetKeyLow(IntPtr handle,
        int row, ref KeyStruct key);

    static unsafe public byte[] ResultGetKey(IntPtr handle, int row) {
        KeyStruct key = new KeyStruct();
        ResultGetKeyLow(handle, row, ref key);
        IntPtr pdata = new IntPtr(key.data);
        byte[] data = new byte[key.size];
        Marshal.Copy(pdata, data, 0, key.size);
        return data;
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "uqi_result_get_record",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern void ResultGetRecordLow(IntPtr handle,
        int row, ref RecordStruct record);

    static unsafe public byte[] ResultGetRecord(IntPtr handle, int row) {
        RecordStruct record = new RecordStruct();
        ResultGetRecordLow(handle, row, ref record);
        IntPtr pdata = new IntPtr(record.data);
        byte[] data = new byte[record.size];
        Marshal.Copy(pdata, data, 0, record.size);
        return data;
    }

    [DllImport("upscaledb-2.2.0.dll", EntryPoint = "uqi_result_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern void ResultClose(IntPtr handle);
  }
}
