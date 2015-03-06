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
