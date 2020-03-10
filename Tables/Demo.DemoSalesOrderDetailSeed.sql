CREATE TABLE [Demo].[DemoSalesOrderDetailSeed] (
  [OrderQty] [smallint] NOT NULL,
  [ProductID] [int] NOT NULL,
  [SpecialOfferID] [int] NOT NULL,
  [OrderID] [int] NOT NULL,
  [LocalID] [int] IDENTITY,
  PRIMARY KEY NONCLUSTERED ([LocalID])
)
WITH (MEMORY_OPTIMIZED = ON)
GO

ALTER TABLE [Demo].[DemoSalesOrderDetailSeed]
  ADD INDEX [IX_OrderID] ([OrderID])
  WITH (STATISTICS_NORECOMPUTE = ON)
GO