CREATE TABLE [Production].[Product_inmem] (
  [ProductID] [int] IDENTITY,
  [Name] [nvarchar](50) NOT NULL,
  [ProductNumber] [nvarchar](25) NOT NULL,
  [MakeFlag] [bit] NOT NULL CONSTRAINT [IMDF_Product_MakeFlag] DEFAULT (1),
  [FinishedGoodsFlag] [bit] NOT NULL CONSTRAINT [IMDF_Product_FinishedGoodsFlag] DEFAULT (1),
  [Color] [nvarchar](15) NULL,
  [SafetyStockLevel] [smallint] NOT NULL,
  [ReorderPoint] [smallint] NOT NULL,
  [StandardCost] [money] NOT NULL,
  [ListPrice] [money] NOT NULL,
  [Size] [nvarchar](5) NULL,
  [SizeUnitMeasureCode] [nchar](3) NULL,
  [WeightUnitMeasureCode] [nchar](3) NULL,
  [Weight] [decimal](8, 2) NULL,
  [DaysToManufacture] [int] NOT NULL,
  [ProductLine] [nchar](2) NULL,
  [Class] [nchar](2) NULL,
  [Style] [nchar](2) NULL,
  [ProductSubcategoryID] [int] NULL,
  [ProductModelID] [int] NULL,
  [SellStartDate] [datetime2] NOT NULL,
  [SellEndDate] [datetime2] NULL,
  [DiscontinuedDate] [datetime2] NULL,
  [ModifiedDate] [datetime2] NOT NULL CONSTRAINT [IMDF_Product_ModifiedDate] DEFAULT (sysdatetime()),
  CONSTRAINT [IMPK_Product_ProductID] PRIMARY KEY NONCLUSTERED ([ProductID]),
  CONSTRAINT [IMCK_Product_Class] CHECK ([Class]='H' OR [Class]='h' OR [Class]='M' OR [Class]='m' OR [Class]='L' OR [Class]='l' OR [Class] IS NULL),
  CONSTRAINT [IMCK_Product_DaysToManufacture] CHECK ([DaysToManufacture]>=(0)),
  CONSTRAINT [IMCK_Product_ListPrice] CHECK ([ListPrice]>=(0.00)),
  CONSTRAINT [IMCK_Product_ProductLine] CHECK ([ProductLine]='R' OR [ProductLine]='r' OR [ProductLine]='M' OR [ProductLine]='m' OR [ProductLine]='T' OR [ProductLine]='t' OR [ProductLine]='S' OR [ProductLine]='s' OR [ProductLine] IS NULL),
  CONSTRAINT [IMCK_Product_ReorderPoint] CHECK ([ReorderPoint]>(0)),
  CONSTRAINT [IMCK_Product_SafetyStockLevel] CHECK ([SafetyStockLevel]>(0)),
  CONSTRAINT [IMCK_Product_SellEndDate] CHECK ([SellEndDate]>=[SellStartDate] OR [SellEndDate] IS NULL),
  CONSTRAINT [IMCK_Product_StandardCost] CHECK ([StandardCost]>=(0.00)),
  CONSTRAINT [IMCK_Product_Style] CHECK ([Style]='U' OR [Style]='u' OR [Style]='M' OR [Style]='m' OR [Style]='W' OR [Style]='w' OR [Style] IS NULL),
  CONSTRAINT [IMCK_Product_Weight] CHECK ([Weight]>(0.00))
)
WITH (MEMORY_OPTIMIZED = ON)
GO

ALTER TABLE [Production].[Product_inmem]
  ADD INDEX [IX_Name] ([Name])
  WITH (STATISTICS_NORECOMPUTE = ON)
GO

ALTER TABLE [Production].[Product_inmem]
  ADD INDEX [IX_ProductNumber] ([ProductNumber])
  WITH (STATISTICS_NORECOMPUTE = ON)
GO