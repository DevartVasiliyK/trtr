CREATE TABLE [Production].[ProductCategory] (
  [ProductCategoryID] [int] IDENTITY,
  [Name] [dbo].[Name] NOT NULL,
  [rowguid] [uniqueidentifier] NOT NULL CONSTRAINT [DF_ProductCategory_rowguid] DEFAULT (newid()) ROWGUIDCOL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductCategory_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_ProductCategory_ProductCategoryID] PRIMARY KEY CLUSTERED ([ProductCategoryID])
)
ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_ProductCategory_Name]
  ON [Production].[ProductCategory] ([Name])
  ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_ProductCategory_rowguid]
  ON [Production].[ProductCategory] ([rowguid])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'High-level product categorization.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for ProductCategory records.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'COLUMN', N'ProductCategoryID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Category description.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'COLUMN', N'Name'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'COLUMN', N'rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'INDEX', N'AK_ProductCategory_Name'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index. Used to support replication samples.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'INDEX', N'AK_ProductCategory_rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'INDEX', N'PK_ProductCategory_ProductCategoryID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'CONSTRAINT', N'PK_ProductCategory_ProductCategoryID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'CONSTRAINT', N'DF_ProductCategory_ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of NEWID()()', 'SCHEMA', N'Production', 'TABLE', N'ProductCategory', 'CONSTRAINT', N'DF_ProductCategory_rowguid'
GO