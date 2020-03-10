CREATE TABLE [Sales].[SpecialOfferProduct_ondisk] (
  [SpecialOfferID] [int] NOT NULL,
  [ProductID] [int] NOT NULL,
  [ModifiedDate] [datetime2] NOT NULL CONSTRAINT [ODDF_SpecialOfferProduct_ModifiedDate] DEFAULT (sysdatetime()),
  CONSTRAINT [ODPK_SpecialOfferProduct_SpecialOfferID_ProductID] PRIMARY KEY NONCLUSTERED ([SpecialOfferID], [ProductID])
)
ON [PRIMARY]
GO

CREATE INDEX [ix_ProductID]
  ON [Sales].[SpecialOfferProduct_ondisk] ([ProductID])
  ON [PRIMARY]
GO

ALTER TABLE [Sales].[SpecialOfferProduct_ondisk]
  ADD CONSTRAINT [ODFK_SpecialOfferProduct_Product_ProductID] FOREIGN KEY ([ProductID]) REFERENCES [Production].[Product_ondisk] ([ProductID])
GO

ALTER TABLE [Sales].[SpecialOfferProduct_ondisk]
  ADD CONSTRAINT [ODFK_SpecialOfferProduct_SpecialOffer_SpecialOfferID] FOREIGN KEY ([SpecialOfferID]) REFERENCES [Sales].[SpecialOffer_ondisk] ([SpecialOfferID])
GO