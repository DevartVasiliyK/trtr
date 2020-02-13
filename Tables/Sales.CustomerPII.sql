CREATE TABLE [Sales].[CustomerPII] (
  [CustomerID] [int] NOT NULL,
  [FirstName] [dbo].[Name] NOT NULL,
  [LastName] [dbo].[Name] NOT NULL,
  [SSN] [nvarchar](11) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = CEK_Auto1, ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
  [CreditCardNumber] [nvarchar](25) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = CEK_Auto1, ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
  [EmailAddress] [nvarchar](50) MASKED WITH (FUNCTION = 'email()') NULL,
  [PhoneNumber] [nvarchar](25) MASKED WITH (FUNCTION = 'default()') NULL,
  [TerritoryID] [int] NULL
)
ON [PRIMARY]
GO