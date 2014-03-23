'
' Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
'
' Permission is hereby granted, free of charge, to any person obtaining a copy
' of this software and associated documentation files (the "Software"), to
' deal in the Software without restriction, including without limitation the
' rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
' sell copies of the Software, and to permit persons to whom the Software is
' furnished to do so, subject to the following conditions:
'
' The above copyright notice and this permission notice shall be included in
' all copies or substantial portions of the Software.
'
' THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
' IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
' FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
' AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
' LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
' FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
' DEALINGS IN THE SOFTWARE.
'

Imports Hamster

Module Module1

    Sub Main()
        Dim i As Integer
        Dim db As Database = New Database
        Dim env As Hamster.Environment = New Environment

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
