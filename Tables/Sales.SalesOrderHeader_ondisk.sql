CREATE TABLE [Sales].[SalesOrderHeader_ondisk] (
  [SalesOrderID] [int] IDENTITY,
  [RevisionNumber] [tinyint] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_RevisionNumber] DEFAULT (0),
  [OrderDate] [datetime2] NOT NULL,
  [DueDate] [datetime2] NOT NULL,
  [ShipDate] [datetime2] NULL,
  [Status] [tinyint] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_Status] DEFAULT (1),
  [OnlineOrderFlag] [bit] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_OnlineOrderFlag] DEFAULT (1),
  [PurchaseOrderNumber] [nvarchar](25) NULL,
  [AccountNumber] [nvarchar](15) NULL,
  [CustomerID] [int] NOT NULL,
  [SalesPersonID] [int] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_SalesPersonID] DEFAULT (-1),
  [TerritoryID] [int] NULL,
  [BillToAddressID] [int] NOT NULL,
  [ShipToAddressID] [int] NOT NULL,
  [ShipMethodID] [int] NOT NULL,
  [CreditCardID] [int] NULL,
  [CreditCardApprovalCode] [varchar](15) NULL,
  [CurrencyRateID] [int] NULL,
  [SubTotal] [money] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_SubTotal] DEFAULT (0.00),
  [TaxAmt] [money] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_TaxAmt] DEFAULT (0.00),
  [Freight] [money] NOT NULL CONSTRAINT [ODDF_SalesOrderHeader_Freight] DEFAULT (0.00),
  [Comment] [nvarchar](128) NULL,
  [ModifiedDate] [datetime2] NOT NULL,
  PRIMARY KEY CLUSTERED ([SalesOrderID]),
  CONSTRAINT [ODCK_SalesOrderHeader_DueDate] CHECK ([DueDate]>=[OrderDate]),
  CONSTRAINT [ODCK_SalesOrderHeader_Freight] CHECK ([Freight]>=(0.00)),
  CONSTRAINT [ODCK_SalesOrderHeader_ShipDate] CHECK ([ShipDate]>=[OrderDate] OR [ShipDate] IS NULL),
  CONSTRAINT [ODCK_SalesOrderHeader_Status] CHECK ([Status]>=(0) AND [Status]<=(8)),
  CONSTRAINT [ODCK_SalesOrderHeader_SubTotal] CHECK ([SubTotal]>=(0.00)),
  CONSTRAINT [ODCK_SalesOrderHeader_TaxAmt] CHECK ([TaxAmt]>=(0.00))
)
ON [PRIMARY]
GO

CREATE INDEX [IX_CustomerID]
  ON [Sales].[SalesOrderHeader_ondisk] ([CustomerID])
  ON [PRIMARY]
GO

CREATE INDEX [IX_SalesPersonID]
  ON [Sales].[SalesOrderHeader_ondisk] ([SalesPersonID])
  ON [PRIMARY]
GO