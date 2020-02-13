CREATE TABLE [Person].[Person_Temporal_History] (
  [BusinessEntityID] [int] NOT NULL,
  [PersonType] [nchar](2) NOT NULL,
  [NameStyle] [dbo].[NameStyle] NOT NULL,
  [Title] [nvarchar](8) NULL,
  [FirstName] [dbo].[Name] NOT NULL,
  [MiddleName] [dbo].[Name] NULL,
  [LastName] [dbo].[Name] NOT NULL,
  [Suffix] [nvarchar](10) NULL,
  [EmailPromotion] [int] NOT NULL,
  [ValidFrom] [datetime2] NOT NULL,
  [ValidTo] [datetime2] NOT NULL
)
ON [PRIMARY]
GO

CREATE CLUSTERED INDEX [ix_Person_Temporal_History]
  ON [Person].[Person_Temporal_History] ([BusinessEntityID], [ValidFrom], [ValidTo])
  ON [PRIMARY]
GO