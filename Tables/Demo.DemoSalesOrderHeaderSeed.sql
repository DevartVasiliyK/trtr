CREATE TABLE [Demo].[DemoSalesOrderHeaderSeed] (
  [DueDate] [datetime2] NOT NULL,
  [CustomerID] [int] NOT NULL,
  [SalesPersonID] [int] NOT NULL,
  [BillToAddressID] [int] NOT NULL,
  [ShipToAddressID] [int] NOT NULL,
  [ShipMethodID] [int] NOT NULL,
  [LocalID] [int] IDENTITY,
  PRIMARY KEY NONCLUSTERED ([LocalID])
)
WITH (MEMORY_OPTIMIZED = ON)
GO