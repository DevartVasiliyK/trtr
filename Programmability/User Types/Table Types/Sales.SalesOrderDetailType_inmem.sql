CREATE TYPE [Sales].[SalesOrderDetailType_inmem] AS TABLE (
  [OrderQty] [smallint] NOT NULL,
  [ProductID] [int] NOT NULL,
  [SpecialOfferID] [int] NOT NULL,
  INDEX [IX_ProductID] HASH ([ProductID]) WITH (BUCKET_COUNT = 8),
  INDEX [IX_SpecialOfferID] HASH ([SpecialOfferID]) WITH (BUCKET_COUNT = 8)
)
WITH (MEMORY_OPTIMIZED = ON)
GO