CREATE TABLE [Sales].[SalesOrderDetail_ondisk] (
  [SalesOrderID] [int] NOT NULL,
  [SalesOrderDetailID] [bigint] IDENTITY,
  [CarrierTrackingNumber] [nvarchar](25) NULL,
  [OrderQty] [smallint] NOT NULL,
  [ProductID] [int] NOT NULL,
  [SpecialOfferID] [int] NOT NULL,
  [UnitPrice] [money] NOT NULL,
  [UnitPriceDiscount] [money] NOT NULL CONSTRAINT [ODDF_SalesOrderDetail_UnitPriceDiscount] DEFAULT (0.0),
  [ModifiedDate] [datetime2] NOT NULL,
  CONSTRAINT [ODPK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID] PRIMARY KEY CLUSTERED ([SalesOrderID], [SalesOrderDetailID]),
  CONSTRAINT [ODCK_SalesOrderDetail_OrderQty] CHECK ([OrderQty]>(0)),
  CONSTRAINT [ODCK_SalesOrderDetail_UnitPrice] CHECK ([UnitPrice]>=(0.00)),
  CONSTRAINT [ODCK_SalesOrderDetail_UnitPriceDiscount] CHECK ([UnitPriceDiscount]>=(0.00))
)
ON [PRIMARY]
GO

CREATE INDEX [IX_ProductID]
  ON [Sales].[SalesOrderDetail_ondisk] ([ProductID])
  ON [PRIMARY]
GO

ALTER TABLE [Sales].[SalesOrderDetail_ondisk]
  ADD CONSTRAINT [ODFK_SalesOrderDetail_SalesOrderHeader_SalesOrderID] FOREIGN KEY ([SalesOrderID]) REFERENCES [Sales].[SalesOrderHeader_ondisk] ([SalesOrderID])
GO

ALTER TABLE [Sales].[SalesOrderDetail_ondisk]
  ADD CONSTRAINT [ODFK_SalesOrderDetail_SpecialOfferProduct_SpecialOfferIDProductID] FOREIGN KEY ([SpecialOfferID], [ProductID]) REFERENCES [Sales].[SpecialOfferProduct_ondisk] ([SpecialOfferID], [ProductID])
GO