CREATE TABLE [dbo].[AWBuildVersion] (
  [SystemInformationID] [tinyint] IDENTITY,
  [Database Version] [nvarchar](25) NOT NULL,
  [VersionDate] [datetime] NOT NULL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_AWBuildVersion_ModifiedDate] DEFAULT (getdate()),
  [column1] [varchar](50) NULL,
  CONSTRAINT [PK_AWBuildVersion_SystemInformationID] PRIMARY KEY CLUSTERED ([SystemInformationID])
)
ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Current version number of the AdventureWorks 2014 sample database. ', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for AWBuildVersion records.', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'COLUMN', N'SystemInformationID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Version number of the database in 9.yy.mm.dd.00 format.', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'COLUMN', N'Database Version'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'COLUMN', N'VersionDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'INDEX', N'PK_AWBuildVersion_SystemInformationID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'CONSTRAINT', N'PK_AWBuildVersion_SystemInformationID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'dbo', 'TABLE', N'AWBuildVersion', 'CONSTRAINT', N'DF_AWBuildVersion_ModifiedDate'
GO