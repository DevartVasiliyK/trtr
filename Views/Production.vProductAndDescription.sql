SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

CREATE VIEW [Production].[vProductAndDescription] 
WITH SCHEMABINDING 
AS 
-- View (indexed or standard) to display products and product descriptions by language.
SELECT 
    p.[ProductID] 
    ,p.[Name] 
    ,pm.[Name] AS [ProductModel] 
    ,pmx.[CultureID] 
    ,pd.[Description] 
FROM [Production].[Product] p 
    INNER JOIN [Production].[ProductModel] pm 
    ON p.[ProductModelID] = pm.[ProductModelID] 
    INNER JOIN [Production].[ProductModelProductDescriptionCulture] pmx 
    ON pm.[ProductModelID] = pmx.[ProductModelID] 
    INNER JOIN [Production].[ProductDescription] pd 
    ON pmx.[ProductDescriptionID] = pd.[ProductDescriptionID];
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE UNIQUE CLUSTERED INDEX [IX_vProductAndDescription]
  ON [Production].[vProductAndDescription] ([CultureID], [ProductID])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Clustered index on the view vProductAndDescription.', 'SCHEMA', N'Production', 'VIEW', N'vProductAndDescription', 'INDEX', N'IX_vProductAndDescription'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Product names and descriptions. Product descriptions are provided in multiple languages.', 'SCHEMA', N'Production', 'VIEW', N'vProductAndDescription'
GO