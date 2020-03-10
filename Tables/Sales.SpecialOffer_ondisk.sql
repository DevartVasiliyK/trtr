CREATE TABLE [Sales].[SpecialOffer_ondisk] (
  [SpecialOfferID] [int] IDENTITY,
  [Description] [nvarchar](255) NOT NULL,
  [DiscountPct] [smallmoney] NOT NULL CONSTRAINT [ODDF_SpecialOffer_DiscountPct] DEFAULT (0.00),
  [Type] [nvarchar](50) NOT NULL,
  [Category] [nvarchar](50) NOT NULL,
  [StartDate] [datetime2] NOT NULL,
  [EndDate] [datetime2] NOT NULL,
  [MinQty] [int] NOT NULL CONSTRAINT [ODDF_SpecialOffer_MinQty] DEFAULT (0),
  [MaxQty] [int] NULL,
  [ModifiedDate] [datetime2] NOT NULL CONSTRAINT [ODDF_SpecialOffer_ModifiedDate] DEFAULT (sysdatetime()),
  CONSTRAINT [ODPK_SpecialOffer_SpecialOfferID] PRIMARY KEY CLUSTERED ([SpecialOfferID]),
  CONSTRAINT [ODCK_SpecialOffer_DiscountPct] CHECK ([DiscountPct]>=(0.00)),
  CONSTRAINT [ODCK_SpecialOffer_EndDate] CHECK ([EndDate]>=[StartDate]),
  CONSTRAINT [ODCK_SpecialOffer_MaxQty] CHECK ([MaxQty]>=(0)),
  CONSTRAINT [ODCK_SpecialOffer_MinQty] CHECK ([MinQty]>=(0))
)
ON [PRIMARY]
GO