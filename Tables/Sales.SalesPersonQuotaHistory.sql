CREATE TABLE [Sales].[SalesPersonQuotaHistory] (
  [BusinessEntityID] [int] NOT NULL,
  [QuotaDate] [datetime] NOT NULL,
  [SalesQuota] [money] NOT NULL,
  [rowguid] [uniqueidentifier] NOT NULL CONSTRAINT [DF_SalesPersonQuotaHistory_rowguid] DEFAULT (newid()) ROWGUIDCOL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_SalesPersonQuotaHistory_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_SalesPersonQuotaHistory_BusinessEntityID_QuotaDate] PRIMARY KEY CLUSTERED ([BusinessEntityID], [QuotaDate]),
  CONSTRAINT [CK_SalesPersonQuotaHistory_SalesQuota] CHECK ([SalesQuota]>(0.00))
)
ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_SalesPersonQuotaHistory_rowguid]
  ON [Sales].[SalesPersonQuotaHistory] ([rowguid])
  ON [PRIMARY]
GO

ALTER TABLE [Sales].[SalesPersonQuotaHistory]
  ADD CONSTRAINT [FK_SalesPersonQuotaHistory_SalesPerson_BusinessEntityID] FOREIGN KEY ([BusinessEntityID]) REFERENCES [Sales].[SalesPerson] ([BusinessEntityID])
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Foreign key constraint referencing SalesPerson.SalesPersonID.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'CONSTRAINT', N'FK_SalesPersonQuotaHistory_SalesPerson_BusinessEntityID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Sales performance tracking.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Sales person identification number. Foreign key to SalesPerson.BusinessEntityID.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'COLUMN', N'BusinessEntityID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Sales quota date.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'COLUMN', N'QuotaDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Sales quota amount.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'COLUMN', N'SalesQuota'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'COLUMN', N'rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index. Used to support replication samples.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'INDEX', N'AK_SalesPersonQuotaHistory_rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'INDEX', N'PK_SalesPersonQuotaHistory_BusinessEntityID_QuotaDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'CONSTRAINT', N'PK_SalesPersonQuotaHistory_BusinessEntityID_QuotaDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Check constraint [SalesQuota] > (0.00)', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'CONSTRAINT', N'CK_SalesPersonQuotaHistory_SalesQuota'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'CONSTRAINT', N'DF_SalesPersonQuotaHistory_ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of NEWID()', 'SCHEMA', N'Sales', 'TABLE', N'SalesPersonQuotaHistory', 'CONSTRAINT', N'DF_SalesPersonQuotaHistory_rowguid'
GO