CREATE TABLE [Sales].[SalesOrderDetail_inmem] (
  [SalesOrderID] [int] NOT NULL,
  [SalesOrderDetailID] [bigint] IDENTITY,
  [CarrierTrackingNumber] [nvarchar](25) NULL,
  [OrderQty] [smallint] NOT NULL,
  [ProductID] [int] NOT NULL,
  [SpecialOfferID] [int] NOT NULL,
  [UnitPrice] [money] NOT NULL,
  [UnitPriceDiscount] [money] NOT NULL CONSTRAINT [IMDF_SalesOrderDetail_UnitPriceDiscount] DEFAULT (0.0),
  [ModifiedDate] [datetime2] NOT NULL,
  CONSTRAINT [imPK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID] PRIMARY KEY NONCLUSTERED ([SalesOrderID], [SalesOrderDetailID]),
  CONSTRAINT [IMCK_SalesOrderDetail_OrderQty] CHECK ([OrderQty]>(0)),
  CONSTRAINT [IMCK_SalesOrderDetail_UnitPrice] CHECK ([UnitPrice]>=(0.00)),
  CONSTRAINT [IMCK_SalesOrderDetail_UnitPriceDiscount] CHECK ([UnitPriceDiscount]>=(0.00))
)
WITH (MEMORY_OPTIMIZED = ON)
GO

ALTER TABLE [Sales].[SalesOrderDetail_inmem]
  ADD INDEX [IX_ProductID] HASH ([ProductID]) WITH (BUCKET_COUNT = 1048576)
GO

ALTER TABLE [Sales].[SalesOrderDetail_inmem]
  ADD INDEX [IX_SalesOrderID] HASH ([SalesOrderID]) WITH (BUCKET_COUNT = 16777216)
GO

ALTER TABLE [Sales].[SalesOrderDetail_inmem]
  ADD CONSTRAINT [IMFK_SalesOrderDetail_SalesOrderHeader_SalesOrderID] FOREIGN KEY ([SalesOrderID]) REFERENCES [Sales].[SalesOrderHeader_inmem] ([SalesOrderID])
GO

ALTER TABLE [Sales].[SalesOrderDetail_inmem]
  ADD CONSTRAINT [IMFK_SalesOrderDetail_SpecialOfferProduct_SpecialOfferIDProductID] FOREIGN KEY ([SpecialOfferID], [ProductID]) REFERENCES [Sales].[SpecialOfferProduct_inmem] ([SpecialOfferID], [ProductID])
GO