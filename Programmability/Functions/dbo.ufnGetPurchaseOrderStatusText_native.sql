SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE FUNCTION [dbo].[ufnGetPurchaseOrderStatusText_native] (@Status tinyint)
RETURNS nvarchar(15) 
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'English')

    IF @Status=1 RETURN 'Pending'
    IF @Status=2 RETURN 'Approved'
    IF @Status=3 RETURN 'Rejected'
    IF @Status=4 RETURN 'Complete'
    
    RETURN '** Invalid **'

END
GO