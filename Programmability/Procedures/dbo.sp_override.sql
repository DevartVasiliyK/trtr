﻿SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE PROCEDURE [dbo].[sp_override];1
	@prm_1 INT
WITH ENCRYPTION
AS
SELECT 1;
GO
CREATE PROCEDURE [dbo].[sp_override];2
	@prm_1 INT,@prm_2 INT
WITH ENCRYPTION
AS
SELECT 2;
GO