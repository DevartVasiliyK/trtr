CREATE TABLE [Person].[PersonPhone] (
  [BusinessEntityID] [int] NOT NULL,
  [PhoneNumber] [dbo].[Phone] NOT NULL,
  [PhoneNumberTypeID] [int] NOT NULL,
  [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_PersonPhone_ModifiedDate] DEFAULT (getdate()),
  CONSTRAINT [PK_PersonPhone_BusinessEntityID_PhoneNumber_PhoneNumberTypeID] PRIMARY KEY CLUSTERED ([BusinessEntityID], [PhoneNumber], [PhoneNumberTypeID])
)
ON [PRIMARY]
GO

CREATE INDEX [IX_PersonPhone_PhoneNumber]
  ON [Person].[PersonPhone] ([PhoneNumber])
  ON [PRIMARY]
GO

ALTER TABLE [Person].[PersonPhone]
  ADD CONSTRAINT [FK_PersonPhone_Person_BusinessEntityID] FOREIGN KEY ([BusinessEntityID]) REFERENCES [Person].[Person] ([BusinessEntityID])
GO

ALTER TABLE [Person].[PersonPhone]
  ADD CONSTRAINT [FK_PersonPhone_PhoneNumberType_PhoneNumberTypeID] FOREIGN KEY ([PhoneNumberTypeID]) REFERENCES [Person].[PhoneNumberType] ([PhoneNumberTypeID])
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Telephone number and type of a person.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Business entity identification number. Foreign key to Person.BusinessEntityID.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'COLUMN', N'BusinessEntityID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Telephone number identification number.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'COLUMN', N'PhoneNumber'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Kind of phone number. Foreign key to PhoneNumberType.PhoneNumberTypeID.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'COLUMN', N'PhoneNumberTypeID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Date and time the record was last updated.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'COLUMN', N'ModifiedDate'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Nonclustered index.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'INDEX', N'IX_PersonPhone_PhoneNumber'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index created by a primary key constraint.', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'INDEX', N'PK_PersonPhone_BusinessEntityID_PhoneNumber_PhoneNumberTypeID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Person', 'TABLE', N'PersonPhone', 'CONSTRAINT', N'PK_PersonPhone_BusinessEntityID_PhoneNumber_PhoneNumberTypeID'
GO