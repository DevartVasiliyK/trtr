﻿SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[ufnLeadingZeros_native](
    @Value int
) 
RETURNS varchar(8) 
WITH NATIVE_COMPILATION, SCHEMABINDING
AS 
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'English')

    DECLARE @ReturnValue varchar(8);

    SET @ReturnValue = CONVERT(varchar(8), @Value);

	DECLARE @i int = 0, @count int = 8 - LEN(@ReturnValue)

	WHILE @i < @count
	BEGIN
		SET @ReturnValue = '0' + @ReturnValue;
		SET @i += 1
	END

    RETURN (@ReturnValue);

END
GO