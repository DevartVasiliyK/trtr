CREATE TYPE [Sales].[SalesOrderDetailType_ondisk] AS TABLE (
  [OrderQty] [smallint] NOT NULL,
  [ProductID] [int] NOT NULL,
  [SpecialOfferID] [int] NOT NULL,
  INDEX [IX_ProductID] CLUSTERED ([ProductID]),
  INDEX [IX_SpecialOfferID] ([SpecialOfferID])
)
GO