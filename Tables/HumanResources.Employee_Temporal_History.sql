CREATE TABLE [HumanResources].[Employee_Temporal_History] (
  [BusinessEntityID] [int] NOT NULL,
  [NationalIDNumber] [nvarchar](15) NOT NULL,
  [LoginID] [nvarchar](256) NOT NULL,
  [OrganizationNode] [hierarchyid] NULL,
  [OrganizationLevel] [smallint] NULL,
  [JobTitle] [nvarchar](50) NOT NULL,
  [BirthDate] [date] NOT NULL,
  [MaritalStatus] [nchar](1) NOT NULL,
  [Gender] [nchar](1) NOT NULL,
  [HireDate] [date] NOT NULL,
  [VacationHours] [smallint] NOT NULL,
  [SickLeaveHours] [smallint] NOT NULL,
  [ValidFrom] [datetime2] NOT NULL,
  [ValidTo] [datetime2] NOT NULL
)
ON [PRIMARY]
GO

CREATE CLUSTERED INDEX [ix_Employee_Temporal_History]
  ON [HumanResources].[Employee_Temporal_History] ([BusinessEntityID], [ValidFrom], [ValidTo])
  ON [PRIMARY]
GO