CREATE TABLE [dbo].[table_for_encryption] (
  [id] [int] NULL
)
ON [PRIMARY]
GO

SET QUOTED_IDENTIFIER, ANSI_NULLS OFF
GO
CREATE TRIGGER [dbo].[trg_for_encryption]
ON [table_for_encryption]
WITH ENCRYPTION
FOR INSERT
AS
SET NOCOUNT ON
SELECT 2
GO