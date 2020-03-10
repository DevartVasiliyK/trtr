CREATE TABLE [Sales].[SpecialOffer_inmem] (
  [SpecialOfferID] [int] IDENTITY,
  [Description] [nvarchar](255) NOT NULL,
  [DiscountPct] [smallmoney] NOT NULL CONSTRAINT [IMDF_SpecialOffer_DiscountPct] DEFAULT (0.00),
  [Type] [nvarchar](50) NOT NULL,
  [Category] [nvarchar](50) NOT NULL,
  [StartDate] [datetime2] NOT NULL,
  [EndDate] [datetime2] NOT NULL,
  [MinQty] [int] NOT NULL CONSTRAINT [IMDF_SpecialOffer_MinQty] DEFAULT (0),
  [MaxQty] [int] NULL,
  [ModifiedDate] [datetime2] NOT NULL CONSTRAINT [IMDF_SpecialOffer_ModifiedDate] DEFAULT (sysdatetime()),
  CONSTRAINT [IMPK_SpecialOffer_SpecialOfferID] PRIMARY KEY NONCLUSTERED HASH ([SpecialOfferID]) WITH (BUCKET_COUNT = 1048576),
  CONSTRAINT [IMCK_SpecialOffer_DiscountPct] CHECK ([DiscountPct]>=(0.00)),
  CONSTRAINT [IMCK_SpecialOffer_EndDate] CHECK ([EndDate]>=[StartDate]),
  CONSTRAINT [IMCK_SpecialOffer_MaxQty] CHECK ([MaxQty]>=(0)),
  CONSTRAINT [IMCK_SpecialOffer_MinQty] CHECK ([MinQty]>=(0))
)
WITH (MEMORY_OPTIMIZED = ON)
GO