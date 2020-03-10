SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
/*
	Stored procedure that deletes row in [Person].[Person_Temporal]
	and corresponding row in [HumanResources].[Employee_Temporal]
*/
CREATE PROCEDURE [Person].[sp_DeletePerson_Temporal]
@BusinessEntityID INT
AS

DELETE FROM [HumanResources].[Employee_Temporal] WHERE [BusinessEntityID] = @BusinessEntityID;
DELETE FROM [Person].[Person_Temporal] WHERE [BusinessEntityID] = @BusinessEntityID;
GO