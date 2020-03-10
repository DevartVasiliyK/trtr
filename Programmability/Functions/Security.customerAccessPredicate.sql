SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE FUNCTION [Security].[customerAccessPredicate](@TerritoryID int)
	RETURNS TABLE
	WITH SCHEMABINDING
AS
	RETURN SELECT 1 AS accessResult
	FROM HumanResources.Employee e 
	INNER JOIN Sales.SalesPerson sp ON sp.BusinessEntityID = e.BusinessEntityID
	WHERE
		( RIGHT(e.LoginID, LEN(e.LoginID) - LEN('adventure-works\')) = USER_NAME() AND sp.TerritoryID = @TerritoryID ) 
		OR IS_MEMBER('SalesManagers') = 1
		OR IS_MEMBER('db_owner') = 1
GO