CREATE TABLE [dbo].[ErrorLog] (
  [ErrorLogID] [int] IDENTITY,
  [ErrorTime] [datetime] NOT NULL CONSTRAINT [DF_ErrorLog_ErrorTime] DEFAULT (getdate()),
  [UserName] [sysname] NOT NULL,
  [ErrorNumber] [int] NOT NULL,
  [ErrorSeverity] [int] NULL,
  [ErrorState] [int] NULL,
  [ErrorProcedure] [nvarchar](126) NULL,
  [ErrorLine] [int] NULL,
  [ErrorMessage] [nvarchar](4000) NOT NULL,
  CONSTRAINT [PK_ErrorLog_ErrorLogID] PRIMARY KEY CLUSTERED ([ErrorLogID])
)
ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Audit table tracking errors in the the AdventureWorks database that are caught by the CATCH block of a TRY...CATCH construct. Data is inserted by stored procedure dbo.uspLogError when it is executed from inside the CATCH block of a TRY...CATCH construct.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for ErrorLog records.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorLogID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The date and time at which the error occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorTime'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The user who executed the batch in which the error occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'UserName'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The error number of the error that occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorNumber'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The severity of the error that occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorSeverity'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The state number of the error that occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorState'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The name of the stored procedure or trigger where the error occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorProcedure'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The line number at which the error occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorLine'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The message text of the error that occurred.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'COLUMN', N'ErrorMessage'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'INDEX', N'PK_ErrorLog_ErrorLogID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'CONSTRAINT', N'PK_ErrorLog_ErrorLogID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'dbo', 'TABLE', N'ErrorLog', 'CONSTRAINT', N'DF_ErrorLog_ErrorTime'
GO