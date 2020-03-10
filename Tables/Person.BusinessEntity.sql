CREATE TABLE [Person].[BusinessEntity] (
  [BusinessEntityID] [int] IDENTITY NOT FOR REPLICATION,
  [rowguid] [uniqueidentifier] NOT NULL CONSTRAINT [DF_BusinessEntity_rowguid] DEFAULT (newid()) ROWGUIDCOL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_BusinessEntity_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_BusinessEntity_BusinessEntityID] PRIMARY KEY CLUSTERED ([BusinessEntityID])
)
ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_BusinessEntity_rowguid]
  ON [Person].[BusinessEntity] ([rowguid])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Source of the ID that connects vendors, customers, and employees with address and contact information.', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for all customers, vendors, and employees.', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'COLUMN', N'BusinessEntityID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'COLUMN', N'rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index. Used to support replication samples.', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'INDEX', N'AK_BusinessEntity_rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'INDEX', N'PK_BusinessEntity_BusinessEntityID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'CONSTRAINT', N'PK_BusinessEntity_BusinessEntityID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'CONSTRAINT', N'DF_BusinessEntity_ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of NEWID()', 'SCHEMA', N'Person', 'TABLE', N'BusinessEntity', 'CONSTRAINT', N'DF_BusinessEntity_rowguid'
GO