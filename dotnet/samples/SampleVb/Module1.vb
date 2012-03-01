'
' Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
'
' This program is free software; you can redistribute it and/or modify it
' under the terms of the GNU General Public License as published by the
' Free Software Foundation; either version 2 of the License, or 
' (at your option) any later version.
'
' See file COPYING.GPL2 and COPYING.GPL3 for License information.
'

Imports Hamster

Module Module1

    Sub Main()
        Dim i As Integer
        Dim db As Database = New Database

        ' create a new Database
        db.Create("test.db")

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
