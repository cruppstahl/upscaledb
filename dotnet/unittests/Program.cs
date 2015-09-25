/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
using System.Collections.Generic;
using System.Text;
using Hamster;

/*
 * The .NET unittest framework no longer works in MSVC2013: loading
 * the native DLL throws exceptions.
 * 
 * http://stackoverflow.com/questions/9202988/badimageformatexception-when-anycpu-test-assembly-implements-interface-from-x64?rq=1
 * http://stackoverflow.com/questions/3595117/badimageformatexception-during-net-assembly-load-issue?rq=1
 * http://stackoverflow.com/questions/2494520/alternate-cause-of-badimageformatexception-in-net-assembly
 * http://stackoverflow.com/questions/20106086/badimageformatexception-occurring-when-loading-a-native-dll-in-net-4-0
 * 
 * After 3 days of trying to figure out what's wrong i am giving up and
 * resort to a plain application for running the tests.
 */
namespace Unittests
{
  class Program
  {
    static void Main(string[] args)
    {
      EnvironmentTest environmentTest = new EnvironmentTest();
      environmentTest.Run();

      DatabaseExceptionTest databaseExTest = new DatabaseExceptionTest();
      databaseExTest.Run();

      TransactionTest transactionTest = new TransactionTest();
      transactionTest.Run();

      CursorTest cursorTest = new CursorTest();
      cursorTest.Run();

      DatabaseTest databaseTest = new DatabaseTest();
      databaseTest.Run();

      Console.Out.WriteLine("Success!");
    }
  }
}
