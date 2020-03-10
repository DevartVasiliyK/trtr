CREATE TABLE [Sales].[SpecialOfferProduct_inmem] (
  [SpecialOfferID] [int] NOT NULL,
  [ProductID] [int] NOT NULL,
  [ModifiedDate] [datetime2] NOT NULL CONSTRAINT [IMDF_SpecialOfferProduct_ModifiedDate] DEFAULT (sysdatetime()),
  CONSTRAINT [IMPK_SpecialOfferProduct_SpecialOfferID_ProductID] PRIMARY KEY NONCLUSTERED ([SpecialOfferID], [ProductID])
)
WITH (MEMORY_OPTIMIZED = ON)
GO

ALTER TABLE [Sales].[SpecialOfferProduct_inmem]
  ADD INDEX [ix_ProductID] ([ProductID])
  WITH (STATISTICS_NORECOMPUTE = ON)
GO

ALTER TABLE [Sales].[SpecialOfferProduct_inmem]
  ADD CONSTRAINT [IMFK_SpecialOfferProduct_Product_ProductID] FOREIGN KEY ([ProductID]) REFERENCES [Production].[Product_inmem] ([ProductID])
GO

ALTER TABLE [Sales].[SpecialOfferProduct_inmem]
  ADD CONSTRAINT [IMFK_SpecialOfferProduct_SpecialOffer_SpecialOfferID] FOREIGN KEY ([SpecialOfferID]) REFERENCES [Sales].[SpecialOffer_inmem] ([SpecialOfferID])
GO