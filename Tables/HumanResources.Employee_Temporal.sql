CREATE TABLE [HumanResources].[Employee_Temporal] (
  [BusinessEntityID] [int] NOT NULL,
  [NationalIDNumber] [nvarchar](15) NOT NULL,
  [LoginID] [nvarchar](256) NOT NULL,
  [OrganizationNode] [hierarchyid] NULL,
  [OrganizationLevel] AS ([OrganizationNode].[GetLevel]()),
  [JobTitle] [nvarchar](50) NOT NULL,
  [BirthDate] [date] NOT NULL,
  [MaritalStatus] [nchar](1) NOT NULL,
  [Gender] [nchar](1) NOT NULL,
  [HireDate] [date] NOT NULL,
  [VacationHours] [smallint] NOT NULL,
  [SickLeaveHours] [smallint] NOT NULL,
  [ValidFrom] [datetime2] GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
  [ValidTo] [datetime2] GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
  PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo),
  CONSTRAINT [PK_Employee_History_BusinessEntityID] PRIMARY KEY CLUSTERED ([BusinessEntityID])
)
ON [PRIMARY]
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [HumanResources].[Employee_Temporal_History], DATA_CONSISTENCY_CHECK = ON))
GO