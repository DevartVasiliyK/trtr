CREATE TABLE [Person].[PhoneNumberType] (
  [PhoneNumberTypeID] [int] IDENTITY,
  [Name] [dbo].[Name] NOT NULL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PhoneNumberType_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_PhoneNumberType_PhoneNumberTypeID] PRIMARY KEY CLUSTERED ([PhoneNumberTypeID])
)
ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Type of phone number of a person.', 'SCHEMA', N'Person', 'TABLE', N'PhoneNumberType'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key for telephone number type records.', 'SCHEMA', N'Person', 'TABLE', N'PhoneNumberType', 'COLUMN', N'PhoneNumberTypeID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Name of the telephone number type', 'SCHEMA', N'Person', 'TABLE', N'PhoneNumberType', 'COLUMN', N'Name'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Person', 'TABLE', N'PhoneNumberType', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Person', 'TABLE', N'PhoneNumberType', 'CONSTRAINT', N'PK_PhoneNumberType_PhoneNumberTypeID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Default constraint value of GETDATE()', 'SCHEMA', N'Person', 'TABLE', N'PhoneNumberType', 'CONSTRAINT', N'DF_PhoneNumberType_ModifiedDate'
GO