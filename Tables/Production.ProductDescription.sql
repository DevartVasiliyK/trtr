CREATE TABLE [Production].[ProductDescription] (
  [ProductDescriptionID] [int] IDENTITY,
  [Description] [nvarchar](400) NOT NULL,
  [rowguid] [uniqueidentifier] NOT NULL CONSTRAINT [DF_ProductDescription_rowguid] DEFAULT (newid()) ROWGUIDCOL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductDescription_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_ProductDescription_ProductDescriptionID] PRIMARY KEY CLUSTERED ([ProductDescriptionID])
)
ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_ProductDescription_rowguid]
  ON [Production].[ProductDescription] ([rowguid])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Product descriptions in several languages.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for ProductDescription records.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'COLUMN', N'ProductDescriptionID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Description of the product.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'COLUMN', N'Description'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'COLUMN', N'rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index. Used to support replication samples.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'INDEX', N'AK_ProductDescription_rowguid'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'INDEX', N'PK_ProductDescription_ProductDescriptionID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'CONSTRAINT', N'PK_ProductDescription_ProductDescriptionID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'CONSTRAINT', N'DF_ProductDescription_ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of NEWID()', 'SCHEMA', N'Production', 'TABLE', N'ProductDescription', 'CONSTRAINT', N'DF_ProductDescription_rowguid'
GO