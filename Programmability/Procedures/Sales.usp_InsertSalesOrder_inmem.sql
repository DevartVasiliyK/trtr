SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE PROCEDURE [Sales].[usp_InsertSalesOrder_inmem]
	@SalesOrderID int OUTPUT,
	@DueDate [datetime2](7) NOT NULL,
	@CustomerID [int] NOT NULL,
	@BillToAddressID [int] NOT NULL,
	@ShipToAddressID [int] NOT NULL,
	@ShipMethodID [int] NOT NULL,
	@SalesOrderDetails Sales.SalesOrderDetailType_inmem READONLY,
	@Status [tinyint] NOT NULL = 1,
	@OnlineOrderFlag [bit] NOT NULL = 1,
	@PurchaseOrderNumber [nvarchar](25) = NULL,
	@AccountNumber [nvarchar](15) = NULL,
	@SalesPersonID [int] NOT NULL = -1,
	@TerritoryID [int] = NULL,
	@CreditCardID [int] = NULL,
	@CreditCardApprovalCode [varchar](15) = NULL,
	@CurrencyRateID [int] = NULL,
	@Comment nvarchar(128) = NULL
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH
  (TRANSACTION ISOLATION LEVEL = SNAPSHOT,
   LANGUAGE = N'us_english')

	DECLARE @OrderDate datetime2 NOT NULL = SYSDATETIME()

	DECLARE @SubTotal money NOT NULL = 0

	SELECT @SubTotal = ISNULL(SUM(p.ListPrice * (1 - ISNULL(so.DiscountPct, 0))),0)
	FROM @SalesOrderDetails od 
		JOIN Production.Product_inmem p on od.ProductID=p.ProductID
		LEFT JOIN Sales.SpecialOffer_inmem so on od.SpecialOfferID=so.SpecialOfferID

	INSERT INTO Sales.SalesOrderHeader_inmem
	(	DueDate,
		Status,
		OnlineOrderFlag,
		PurchaseOrderNumber,
		AccountNumber,
		CustomerID,
		SalesPersonID,
		TerritoryID,
		BillToAddressID,
		ShipToAddressID,
		ShipMethodID,
		CreditCardID,
		CreditCardApprovalCode,
		CurrencyRateID,
		Comment,
		OrderDate,
		SubTotal,
		ModifiedDate)
	VALUES
	(	
		@DueDate,
		@Status,
		@OnlineOrderFlag,
		@PurchaseOrderNumber,
		@AccountNumber,
		@CustomerID,
		@SalesPersonID,
		@TerritoryID,
		@BillToAddressID,
		@ShipToAddressID,
		@ShipMethodID,
		@CreditCardID,
		@CreditCardApprovalCode,
		@CurrencyRateID,
		@Comment,
		@OrderDate,
		@SubTotal,
		@OrderDate
	)

    SET @SalesOrderID = SCOPE_IDENTITY()

	INSERT INTO Sales.SalesOrderDetail_inmem
	(
		SalesOrderID,
		OrderQty,
		ProductID,
		SpecialOfferID,
		UnitPrice,
		UnitPriceDiscount,
		ModifiedDate
	)
    SELECT 
		@SalesOrderID,
		od.OrderQty,
		od.ProductID,
		od.SpecialOfferID,
		p.ListPrice,
		ISNULL(p.ListPrice * so.DiscountPct, 0),
		@OrderDate
	FROM @SalesOrderDetails od 
		JOIN Production.Product_inmem p on od.ProductID=p.ProductID
		LEFT JOIN Sales.SpecialOffer_inmem so on od.SpecialOfferID=so.SpecialOfferID

END
GO