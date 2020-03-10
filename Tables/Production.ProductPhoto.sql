CREATE TABLE [Production].[ProductPhoto] (
  [ProductPhotoID] [int] IDENTITY,
  [ThumbNailPhoto] [varbinary](max) NULL,
  [ThumbnailPhotoFileName] [nvarchar](50) NULL,
  [LargePhoto] [varbinary](max) NULL,
  [LargePhotoFileName] [nvarchar](50) NULL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductPhoto_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_ProductPhoto_ProductPhotoID] PRIMARY KEY CLUSTERED ([ProductPhotoID])
)
ON [PRIMARY]
TEXTIMAGE_ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Product images.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for ProductPhoto records.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'COLUMN', N'ProductPhotoID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Small image of the product.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'COLUMN', N'ThumbNailPhoto'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Small image file name.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'COLUMN', N'ThumbnailPhotoFileName'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Large image of the product.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'COLUMN', N'LargePhoto'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Large image file name.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'COLUMN', N'LargePhotoFileName'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'INDEX', N'PK_ProductPhoto_ProductPhotoID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'CONSTRAINT', N'PK_ProductPhoto_ProductPhotoID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Production', 'TABLE', N'ProductPhoto', 'CONSTRAINT', N'DF_ProductPhoto_ModifiedDate'
GO