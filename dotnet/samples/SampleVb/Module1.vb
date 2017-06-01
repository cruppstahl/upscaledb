'
' Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
'
' Licensed under the Apache License, Version 2.0 (the "License");
' you may not use this file except in compliance with the License.
' You may obtain a copy of the License at
'
'   http://www.apache.org/licenses/LICENSE-2.0
'
' Unless required by applicable law or agreed to in writing, software
' distributed under the License is distributed on an "AS IS" BASIS,
' WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
' See the License for the specific language governing permissions and
' limitations under the License.
'
' See the file COPYING for License information.
'

Imports Upscaledb

Module Module1

    Sub Main()
        Dim i As Integer
        Dim db As Database = New Database
        Dim env As Upscaledb.Environment = New Environment

        ' create a new Database
        env.Create("test.db")
        db = env.CreateDatabase(1)

        ' insert some values
        For i = 0 To 10
            Dim key(5) As System.Byte
            Dim record(5) As System.Byte

            key(0) = i
            record(0) = i

            db.Insert(key, record)
        Next i

        ' look up the values
        For i = 0 To 10
            Dim key(5) As System.Byte
            Dim record(5) As System.Byte

            key(0) = i
            record = db.Find(key)
            If record(0) <> i Then
                Console.WriteLine("db.Find failed")
                Return
            End If
        Next i

        Console.WriteLine("Success")
    End Sub

End Module
