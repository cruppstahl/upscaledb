/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Text;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Hamster
{
  internal sealed class NativeMethods
  {
    private NativeMethods() {
    }

    [StructLayout(LayoutKind.Sequential)]
    unsafe struct RecordStruct
    {
      public Int32 size;
      public byte *data;
      public Int32 partial_offset;
      public Int32 partial_size;
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

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_set_errhandler",
      CallingConvention = CallingConvention.Cdecl)]
    static public extern void SetErrorHandler(ErrorHandler eh);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_strerror",
      CallingConvention=CallingConvention.Cdecl)]
    static public extern String StringError(int error);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_get_version",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern void GetVersion(out int major, out int minor,
        out int revision);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_create",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvCreate(out IntPtr handle, String fileName,
        int flags, int mode, Parameter[] parameters);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_open",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvOpen(out IntPtr handle, String fileName,
        int flags, Parameter[] parameters);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_create_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvCreateDatabase(IntPtr handle,
        out IntPtr dbhandle, short name, int flags,
        Parameter[] parameters);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_open_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvOpenDatabase(IntPtr handle,
        out IntPtr dbhandle, short name, int flags,
        Parameter[] parameters);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_rename_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvRenameDatabase(IntPtr handle,
        short oldName, short newName);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_erase_db",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvEraseDatabase(IntPtr handle,
        short name, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_flush",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvFlush(IntPtr handle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_get_database_names",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvGetDatabaseNamesLow(IntPtr handle,
        IntPtr dbnames, ref int count);

    static public int EnvGetDatabaseNames(IntPtr handle, out short[] names) {
      // alloc space for 2000 database names
      int count = 2000;
      IntPtr array = Marshal.AllocHGlobal(2*count);
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

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_env_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int EnvClose(IntPtr handle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_txn_begin",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int TxnBegin(out IntPtr txnhandle, IntPtr envhandle,
        String filename, IntPtr reserved, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_txn_commit",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int TxnCommit(IntPtr handle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_txn_abort",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int TxnAbort(IntPtr handle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_get_error",
      CallingConvention = CallingConvention.Cdecl)]
    static public extern int GetLastError(IntPtr handle);

    // TODO this is new, but lots of effort b/c of complex
    // marshalling. if you need this function pls drop me a mail.
/*
    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_get_parameters",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int GetParameters(IntPtr handle, Parameter[] parameters);
*/

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_get_env",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern IntPtr GetEnv(IntPtr handle);

    // TODO this is new, but lots of effort b/c of complex
    // marshalling. if you need this function pls drop me a mail.
/*
    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_key_get_approximate_match",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int KeyGetApproximateMatch(ref KeyStruct key);
*/

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int CompareFunc(IntPtr handle,
        IntPtr lhs, int lhsLength,
        IntPtr rhs, int rhsLength);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_set_compare_func",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int SetCompareFunc(IntPtr handle,
        NativeMethods.CompareFunc foo);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int DuplicateCompareFunc(IntPtr handle,
        byte[] lhs, int lhsLength,
        byte[] rhs, int rhsLength);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_find",
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
        // I didn't found a way to avoid the copying...
        IntPtr recData = new IntPtr(record.data);
        byte[] newRecData = new byte[record.size];
        Marshal.Copy(recData, newRecData, 0, record.size);
        recdata = newRecData;
        // also copy the key data if approx. matching was requested
        if ((flags & (HamConst.HAM_FIND_LT_MATCH | HamConst.HAM_FIND_GT_MATCH)) != 0) {
          IntPtr keyData = new IntPtr(key.data);
          byte[] newKeyData = new byte[key.size];
          Marshal.Copy(keyData, newKeyData, 0, key.size);
          keydata = newKeyData;
        }
        return 0;
      }
    }

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_insert",
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

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_erase",
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

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_get_key_count",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int GetKeyCount(IntPtr handle, IntPtr txnhandle,
        int flags, out Int64 keycount);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_db_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int Close(IntPtr handle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_create",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorCreate(out IntPtr chandle, IntPtr dbhandle,
        IntPtr txnhandle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_clone",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorClone(IntPtr handle, out IntPtr clone);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        IntPtr key, IntPtr record, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        ref KeyStruct key, IntPtr record, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_move",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorMoveLow(IntPtr handle,
        IntPtr key, ref RecordStruct record, int flags);

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

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_overwrite",
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

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_find",
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
        if ((flags & (HamConst.HAM_FIND_LT_MATCH | HamConst.HAM_FIND_GT_MATCH)) != 0) {
          IntPtr keyData = new IntPtr(key.data);
          byte[] newKeyData = new byte[key.size];
          Marshal.Copy(keyData, newKeyData, 0, key.size);
          keydata = newKeyData;
        }
        return 0;
      }
    }

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_insert",
       CallingConvention = CallingConvention.Cdecl)]
    static private extern int CursorInsertLow(IntPtr handle,
        ref KeyStruct key, ref RecordStruct record, int flags);

    static public unsafe int CursorInsert(IntPtr handle,
        byte[] keyData, byte[] recordData, int flags) {
      RecordStruct record = new RecordStruct();
      KeyStruct key = new KeyStruct();
      fixed (byte* br = recordData, bk = keyData) {
        record.data = br;
        record.size = recordData.GetLength(0);
        key.data = bk;
        key.size = (short)keyData.GetLength(0);
        return CursorInsertLow(handle, ref key, ref record, flags);
      }
    }

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_erase",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorErase(IntPtr handle, int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_get_duplicate_count",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorGetDuplicateCount(IntPtr handle, out int count,
        int flags);

    [DllImport("hamsterdb-2.1.10.dll", EntryPoint = "ham_cursor_close",
       CallingConvention = CallingConvention.Cdecl)]
    static public extern int CursorClose(IntPtr handle);
  }
}
