CREATE TABLE [Sales].[TrackingEvent] (
  [TrackingEventID] [int] IDENTITY,
  [EventName] [nvarchar](255) NOT NULL,
  CONSTRAINT [PK_TrackingEvent_TrackingEventID] PRIMARY KEY CLUSTERED ([TrackingEventID])
)
ON [PRIMARY]
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Tracking event lookup table.', 'SCHEMA', N'Sales', 'TABLE', N'TrackingEvent'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key.', 'SCHEMA', N'Sales', 'TABLE', N'TrackingEvent', 'COLUMN', N'TrackingEventID'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Tracking event name.', 'SCHEMA', N'Sales', 'TABLE', N'TrackingEvent', 'COLUMN', N'EventName'
GO

EXEC sys.sp_addextendedproperty N'MS_Description', N'Primary key (clustered) constraint', 'SCHEMA', N'Sales', 'TABLE', N'TrackingEvent', 'CONSTRAINT', N'PK_TrackingEvent_TrackingEventID'
GO