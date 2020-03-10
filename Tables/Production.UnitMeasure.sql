CREATE TABLE [Production].[UnitMeasure] (
  [UnitMeasureCode] [nchar](3) NOT NULL,
  [Name] [dbo].[Name] NOT NULL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_UnitMeasure_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_UnitMeasure_UnitMeasureCode] PRIMARY KEY CLUSTERED ([UnitMeasureCode])
)
ON [PRIMARY]
GO

CREATE UNIQUE INDEX [AK_UnitMeasure_Name]
  ON [Production].[UnitMeasure] ([Name])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unit of measure lookup table.', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key.', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'COLUMN', N'UnitMeasureCode'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unit of measure description.', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'COLUMN', N'Name'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Unique nonclustered index.', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'INDEX', N'AK_UnitMeasure_Name'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'INDEX', N'PK_UnitMeasure_UnitMeasureCode'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'CONSTRAINT', N'PK_UnitMeasure_UnitMeasureCode'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Production', 'TABLE', N'UnitMeasure', 'CONSTRAINT', N'DF_UnitMeasure_ModifiedDate'
GO