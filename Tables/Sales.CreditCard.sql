CREATE TABLE [Sales].[CreditCard] (
  [CreditCardID] [int] IDENTITY,
  [CardType] [nvarchar](50) NOT NULL,
  [CardNumber] [nvarchar](25) NOT NULL,
  [ExpMonth] [tinyint] NOT NULL,
  [ExpYear] [smallint] NOT NULL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_CreditCard_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_CreditCard_CreditCardID] PRIMARY KEY CLUSTERED ([CreditCardID])
)
ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_CreditCard_CardNumber]
  ON [Sales].[CreditCard] ([CardNumber])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Customer credit card information.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for CreditCard records.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'COLUMN', N'CreditCardID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Credit card name.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'COLUMN', N'CardType'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Credit card number.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'COLUMN', N'CardNumber'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Credit card expiration month.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'COLUMN', N'ExpMonth'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Credit card expiration year.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'COLUMN', N'ExpYear'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'INDEX', N'AK_CreditCard_CardNumber'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'INDEX', N'PK_CreditCard_CreditCardID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'CONSTRAINT', N'PK_CreditCard_CreditCardID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Sales', 'TABLE', N'CreditCard', 'CONSTRAINT', N'DF_CreditCard_ModifiedDate'
GO