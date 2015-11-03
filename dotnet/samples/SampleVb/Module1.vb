'
' Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
' All Rights Reserved.
'
' This program is free software: you can redistribute it and/or modify
' it under the terms of the GNU General Public License as published by
' the Free Software Foundation, either version 3 of the License, or
' (at your option) any later version.
'
' This program is distributed in the hope that it will be useful,
' but WITHOUT ANY WARRANTY; without even the implied warranty of
' MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
' GNU General Public License for more details.
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
