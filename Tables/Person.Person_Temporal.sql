CREATE TABLE [Person].[Person_Temporal] (
  [BusinessEntityID] [int] NOT NULL,
  [PersonType] [nchar](2) NOT NULL,
  [NameStyle] [dbo].[NameStyle] NOT NULL,
  [Title] [nvarchar](8) NULL,
  [FirstName] [dbo].[Name] NOT NULL,
  [MiddleName] [dbo].[Name] NULL,
  [LastName] [dbo].[Name] NOT NULL,
  [Suffix] [nvarchar](10) NULL,
  [EmailPromotion] [int] NOT NULL,
  [ValidFrom] [datetime2] GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
  [ValidTo] [datetime2] GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
  PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo),
  CONSTRAINT [PK_Person_Temporal_BusinessEntityID] PRIMARY KEY CLUSTERED ([BusinessEntityID])
)
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [Person].[Person_Temporal_History], DATA_CONSISTENCY_CHECK = ON))
GO