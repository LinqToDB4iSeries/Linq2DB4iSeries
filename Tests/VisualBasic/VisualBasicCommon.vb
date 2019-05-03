Imports System
Imports System.Collections.Generic
Imports System.Linq

Imports Tests.Model

Public Module VisualBasicCommon

	Public Function ParamenterName(ByVal db As ITestDataContext) As IEnumerable(Of Parent)
		Dim id As Integer
		id = 1
		Return From p In db.Parent Where p.ParentID = id Select p
	End Function

	Public Function SearchCondition1(ByVal db As ITestDataContext) As IEnumerable(Of LinqDataTypes)
		Return _
			From t In db.Types _
			Where Not t.BoolValue And (t.SmallIntValue = 5 Or t.SmallIntValue = 7 Or (t.SmallIntValue Or 2) = 10) _
			Select t
	End Function
End Module
