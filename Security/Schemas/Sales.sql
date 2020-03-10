CREATE SCHEMA [Sales] AUTHORIZATION [dbo]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Contains objects related to customers, sales orders, and sales territories.', 'SCHEMA', N'Sales'
GO