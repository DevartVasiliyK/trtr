CREATE TABLE [Sales].[OrderTracking] (
  [OrderTrackingID] [int] IDENTITY,
  [SalesOrderID] [int] NOT NULL,
  [CarrierTrackingNumber] [nvarchar](25) NULL,
  [TrackingEventID] [int] NOT NULL,
  [EventDetails] [nvarchar](2000) NOT NULL,
  [EventDateTime] [datetime2] NOT NULL,
  CONSTRAINT [PK_OrderTracking] PRIMARY KEY CLUSTERED ([OrderTrackingID])
)
ON [PRIMARY]
GO

CREATE INDEX [IX_OrderTracking_CarrierTrackingNumber]
  ON [Sales].[OrderTracking] ([CarrierTrackingNumber])
  ON [PRIMARY]
GO

CREATE INDEX [IX_OrderTracking_SalesOrderID]
  ON [Sales].[OrderTracking] ([SalesOrderID])
  ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Individual tracking events associated with a specific sales order.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'COLUMN', N'OrderTrackingID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Sales order identification number.  Foreign key to SalesOrderHeader.SalesOrderID.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'COLUMN', N'SalesOrderID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Shipment tracking number supplied by the shipper.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'COLUMN', N'CarrierTrackingNumber'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Tracking delivery event for Order shipped to customer. Foreign key to TrackingEvent.TrackingEventID.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'COLUMN', N'TrackingEventID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Details for a delivery tracking event.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'COLUMN', N'EventDetails'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'The date and time that a tracking event has occurred.', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'COLUMN', N'EventDateTime'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Sales', 'TABLE', N'OrderTracking', 'CONSTRAINT', N'PK_OrderTracking'
GO