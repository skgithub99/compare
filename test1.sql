CREATE procedure [integr].[GBSync]
(
--declare
                @reloadEquipCache       bit                                                           =             0
                ,@reloadBulkCache        bit                                                           =             0
                ,@reloadOnly                    bit                                                           =             0
                ,@env                                                   varchar(50)                         =             'QA'
)
as
set transaction isolation level read uncommitted
set deadlock_priority low
set nocount on
set lock_timeout 15000

--This is set to on so that if needed, DTC can create nested transactions.
set xact_abort on

----proc params
--declare
--             @reloadEquipCache       bit                                                           =             0
--             ,@reloadBulkCache        bit                                                           =             0
--             ,@reloadOnly                    bit                                                           =             0
--             ,@env                                                   varchar(50)                         =             'DEV'
----proc params

declare
                @processName                                                varchar(50)                                         =             'GBSync'                                                                                               --Name of this process, used to isolate the config values specific to this procedure.
                ,@remoteServer                                                              sysname                                                              =             ''                                                                                                                              --The GB server to interact with.
                ,@remoteDB                                                                     sysname                                                              =             ''                                                                                                                              --The GB database.
                ,@remoteSchema                                                           sysname                                                              =             ''                                                                                                                              --The GB schema.
                ,@sourceDB                                                                       nvarchar(128)                    =             ''                                                                                                                              --The Mimix database to interact with.
                ,@sourceSchema                                                             nvarchar(128)                    =             ''                                                                                                                              --The Mimix database schema.
                ,@sourceTB                                                                        nvarchar(128)                    =             ''                                                                                                                              --The Mimix table to scan.
                ,@targetDB                                                                        nvarchar(128)                    =             ''                                                                                                                              --The database containing the persistent cache.
                ,@targetSchema                                                              nvarchar(128)                    =             ''                                                                                                                              --The schema of the cache.
                ,@targetTB                                                                         nvarchar(128)                    =             ''                                                                                                                              --The cache table.
                ,@edwServer                                                                    nvarchar(128)                    =             ''                                                                                                                              --Name of EDW server.
                ,@edwDB                                                                                            nvarchar(128)                    =             ''                                                                                                                              --Name of EDW database.
                ,@edwSchema                                                                 nvarchar(128)                    =             ''                                                                                                                              --The schema that contains all the EDW tables.
                ,@sql                                                                                     nvarchar(max)                  =             ''                                                                                                                              --The dynamic SQL statement to execute.
                ,@hashExprOut                                                varchar(max)                     =             ''                                                                                                                              --The dynamically generated TSQL statement that builds the HASHBYTE() expression.
                ,@startTime                                                                       datetime2                                           =             '1900-01-01 00:00:00.000'                      --The start time used to scan rows in Mimix.
                ,@endTime                                                                        datetime2                                           =             '1900-01-01 00:00:00.000'                      --The end time.
                ,@prevStartTime                                                             datetime2                                           =             '1900-01-01 00:00:00.000'                      --Prior start time.
                ,@currentStartTime                                        datetime2                                           =             getdate()                                                                                             --Current start time.
                ,@runDate                                                                          datetime                                             =             getdate()
                ,@logSessionID                                                 uniqueidentifier               =             newid()
                ,@sessionID                                                                       uniqueidentifier               =             newid()
                ,@rowCountOut                                                              int                                                                           =             0
                ,@compData                                                                      varbinary(max)                 =             null
                ,@json                                                                                  nvarchar(max)                  =             N''
                ,@jsonOut                                                                          nvarchar(max)                  =             N''
                ,@e                                                                                                        int                                                                           =             0
                ,@eLine                                                                                               int                                                                           =             0
                ,@eMsg                                                                                               nvarchar(2048)                  =             ''
                ,@eFmtMsg                                                                       nvarchar(2048)                  =             ''
                ,@proc                                                                                 sysname                                                              =             ''
                ,@tranStarted                                                   bit                                                                           =             0
                ,@procID                                                                             int                                                                           =             0
                ,@syncSessionID                                                              uniqueidentifier               =             newid()

declare
                @GB_ACTION_IDLE                                                        int                           =             0
                ,@GB_ACTION_ADD                                                       int                           =             1
                ,@GB_ACTION_UPD                                                       int                           =             2
                ,@GB_ACTION_DEL                                                        int                           =             3
                ,@QUEUE_RC_UNKNOWN                                          int                           =             0
                ,@QUEUE_RC_CATCLASS_MISSING         int                           =             1

--Get name of proc.
set @procID       =             @@procid
set @proc            =             object_schema_name(@procID) + '.' + object_name(@procID)

------------------------- Get config values -------------------------
select
                @remoteServer =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'RemoteServer'

select
                @remoteDB       =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'RemoteDB'

select
                @remoteSchema            =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'RemoteSchema'

select
                @sourceDB                        =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'SourceDB'

select
                @sourceSchema              =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'SourceSchema'

select
                @targetDB                          =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'TargetDB'

select
                @targetSchema               =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'TargetSchema'

select
                @startTime                                        =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'StartDateTimeUTC'

--select
--             @endTime                          =             c.KeyValue
--from [Configuration].config.Config c
--where
--             c.Environment  =             @env
--             and c.Process    =             @processName
--             and c.KeyName =             'EndDateTimeUTC'

select
                @prevStartTime                               =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'PrevStartDateTimeUTC'

select
                @edwServer                     =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'EDW_Server'

select
                @edwDB                             =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'EDW_DB'

select
                @edwSchema                   =             c.KeyValue
from [Configuration].config.Config c
where
                c.Environment  =             @env
                and c.Process    =             @processName
                and c.KeyName =             'EDW_Schema'
-------------------------* Get config values *-------------------------




------------------------- Create the necessary tables -------------------------
--Active PCs in GB.
if (object_id('tempdb..#tGBActivePC') is null)
begin
                create table #tGBActivePC
                (
                                ID                                            bigint                                                                                                                                                                                     not null
                                ,PC                                         varchar(6)           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CMP                     varchar(2)           collate SQL_Latin1_General_CP1_CS_AS not null default ('01')
                )
end
else
begin
                truncate table #tGBActivePC
end

--Remote SKUs.
if (object_id('tempdb..#tGBSKUList') is null)
begin
                create table #tGBSKUList
                (
                                ID                                                                                                            bigint                                                     null
                                ,DateAdded                                                                       datetime                                             null
                                ,LastUpdated                                                     datetime                                             null
                                ,HQLocationID                                                   bigint                                                     null
                                ,UPC                                                                                      nvarchar              (100)      collate SQL_Latin1_General_CP1_CS_AS              null
                                ,ProductID                                                                          nvarchar              (50)        collate SQL_Latin1_General_CP1_CS_AS              null
                )
end
else
begin
                truncate table #tGBSKUList
end

--Remote inventory.
if (object_id('tempdb..#tGBInventory') is null)
begin
                create table #tGBInventory
                (
                                ID                                                                            bigint                                                     null
                                ,DateAdded                                       datetime                                             null
                                ,LastUpdated                     datetime                                             null
                                ,HQLocationID                   bigint                                                     null
                                ,HQSKUID                                            bigint                                                     null
                                ,TagID                                                   nvarchar              (100)      null
                                ,[Status]                                               nvarchar              (100)      null
                                ,AssetID                                               nvarchar              (200)      null
                                ,IsActive                                               bit                                                                           null
                                ,IsBulk                                                   bit                                                                           null
                )
end
else
begin
                truncate table #tGBInventory
end

--SKUs in Wynne/DLK.
--This is NOT all of the cat/classes in Wynne.
--Contains just the cat/classes of the records that were "changed".
if (object_id('tempdb..#tWynneSKUList') is null)
begin
                create table #tWynneSKUList
                (
                                GB_PCID                                                                                              int                                                                                                                                                                                                                           not null
                                ,CMP                                                                                     varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS              null
                                ,PC                                                                                                         varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS              null
                                ,CAT                                                                                       decimal(3,0)                                                                                                                                                                       null
                                ,CLAS                                                                                     decimal(4,0)                                                                                                                                                                       null
                                ,ProductID                                                                          varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS              null
                                ,[Description]                                                    varchar(40)                         collate SQL_Latin1_General_CP1_CS_AS              null
                                ,UPC                                                                                      varchar(3)                           collate SQL_Latin1_General_CP1_CS_AS              null
                                ,SKUListType                                                      varchar(8)                           collate SQL_Latin1_General_CP1_CS_AS              null
                                ,IsConsumable                                                  bit                                                                                                                                                                                                                           null                         default (0)
                                ,IsBulk                                                                                   bit                                                                                                                                                                                                                           not null default (0)
                                ,GBCurrentMax_RFID_TagID       int                                                                                                                                                                                                                           not null default (0)
                                ,HQLocationID                                                   int                                                                                                                                                                                                                           not null default (0)
                                ,HQSKUID                                                                            int                                                                                                                                                                                                                           not null default (0)
                                ,GBAction                                                                            int                                                                                                                                                                                                                           not null default (0)
                )
end
else
begin
                truncate table #tWynneSKUList
end

--EQPMASFL temp cache.
if (object_id('tempdb..#tEQPMASFL_Cache') is null)
begin
                create table #tEQPMASFL_Cache
                (
                                EMCMP                varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,[EMEQP#]          varchar(10)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,[Hash]                 varbinary(8000) not null
                                ,ChangeDate      datetime                             not null
                );

                alter table #tEQPMASFL_Cache add constraint tDF_integ_EQPMASFL_Cache_ChangeDate default (getdate()) for ChangeDate;
                create unique clustered index tPK_integ_EQPMASFL_Cache_EMEQP on #tEQPMASFL_Cache ([EMEQP#], EMCMP) with (fillfactor = 80, data_compression = page);
end
else
begin
                truncate table #tEQPMASFL_Cache
end

--EQPCCLTRG temp cache.
if (object_id('tempdb..#tEQPCCLTRG_Cache') is null)
begin
                create table #tEQPCCLTRG_Cache
                (
                                RowID                                   bigint                                                                                                                                                                                                                     not null identity(1,1)
                                ,RRN_FIELD_DATA           bigint                                                                                                                                                                                                                     not null
                                ,SYNCDTTM                        datetime2                                                                                                                                                                                                           not null
                                ,ELEVENT                             varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELCMP                                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELLOC                                  varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELCATG                                               decimal(3,0)                                                                                                                                                                                       not null
                                ,ELCLAS                                decimal(4,0)                                                                                                                                                                                       not null
                                ,ELQOWN                                            decimal(6,0)                                                                                                                                                                                       not null
                                ,PrevELQOWN                   decimal(6,0)                                                                                                                                                                                       not null
                                ,[Hash]                                 varbinary(8000)                                                                                                                                                                                not null
                                ,ChangeDate                      datetime                             not null
                                                constraint tDF_integ_EQPCCLTRG_Cache_ChangeDate default(getdate())

                                ,constraint tPK_integ_EQPCCLTRG_Cache_ELCATG_ELCLAS primary key clustered (ELCATG, ELCLAS, ELLOC, ELCMP, ELEVENT, SYNCDTTM, RowID) with (fillfactor = 80, data_compression = page)
                )
end
else
begin
                truncate table #tEQPCCLTRG_Cache
end

--EQPMASLF temp change table.
if (object_id('tempdb..#tEQPMASFL_Change') is null)
begin
                create table #tEQPMASFL_Change
                (
                                EMCMP                                varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,[EMEQP#]                          varchar(10)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,ChangeType                     varchar(3)                                                                                                                                                                                           not null
                                --,QueueRowID                 int                                                                                                                                                                                                                           not null default (0)
                )
end
else
begin
                truncate table #tEQPMASFL_Change
end

--EQPCCLTRG temp change table.
if (object_id('tempdb..#tEQPCCLTRG_Change') is null)
begin
                create table #tEQPCCLTRG_Change
                (
                                ELCMP                                                                                  varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                              not null
                                ,ELLOC                                                                                  varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS                              not null
                                ,ELCATG                                                                                               decimal(3,0)                                                                                                                                                                                       not null
                                ,ELCLAS                                                                                decimal(4,0)                                                                                                                                                                                       not null
                                ,ELEVENT                                                                             varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                              not null
                                ,RRN_FIELD_DATA                                                           bigint                                                                                                                                                                                                                     not null
                                ,SYNCDTTM                                                                        datetime2                                                                                                                                                                                                           not null
                                ,QtyToSend                                                                        decimal(6,0)                                                                                                                                                                                       not null
                                ,ChangeType                                                                     varchar(3)                                                                                                                                                                                                           not null
                )
end
else
begin
                truncate table #tEQPCCLTRG_Change
end

--Table used to store what equipment change rows were queued.
--Queued rows indicate that the change wasn't sent because of missing
--information.  They should be picked up and sent once all of the required
--information is available.
if (object_id('tempdb..#tEQPMASFL_Queue_Output') is null)
begin
                create table #tEQPMASFL_Queue_Output
                (
                                EMCMP                                                varchar(2)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,[EMEQP#]                                          varchar(10)                                         collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,EMCLOC                                                             varchar(4)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,CAT                                                       varchar(3)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,CLAS                                                     varchar(4)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,QueueReasonCode        int                                                                           not null
                                ,DetectionDate                 datetime                                             not null
                                ,SyncSessionID                  uniqueidentifier               not null
                )
end
else
begin
                truncate table #tEQPMASFL_Queue_Output
end

--Table used to capture bulk items that were queued.
if (object_id('tempdb..#tBulkItemToSend_Queue_Output') is null)
begin
                create table #tBulkItemToSend_Queue_Output
                (
                                RJCMP                                                  varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                 not null
                                ,RJTLOC                                                varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS                 not null
                                ,RJCATG                                                               numeric(3,0)                                                                                                                                                                                      null
                                ,RJCLAS                                                numeric(4,0)                                                                                                                                                                                      null
                                ,RJQTYR                                                                numeric(7,2)                                                                                                                                                                                      null
                                ,RJSTTS                                                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                 null
                                ,QueueReasonCode        int                                                                                                                                                                                                                                           not null default (0)
                                ,DetectionDate                 datetime                                                                                                                                                                                                             not null
                                ,SyncSessionID                  uniqueidentifier                                                                                                                                                                               not null
                )
end
else
begin
                truncate table #tBulkItemToSend_Queue_Output
end

--GB customer job site.
if (object_id('tempdb..#tGBCustomerJobSite') is null)
begin
                create table #tGBCustomerJobSite
                (
                                CJID                                                       bigint                                     null
                                ,HQLocationID                   bigint                                     null
                                ,CompanyCode                 nvarchar(2)                         null
                                ,AccountNumber                             nvarchar(50)      null
                                ,JobNumber                                       nvarchar(50)      null
                                ,JobLocation                       nvarchar(40)      null
                                ,ContactName                   nvarchar(50)      null
                                ,[Name]                                                               nvarchar(50)      null
                                ,Address1                                            nvarchar(100)    null
                                ,Address2                                            nvarchar(100)    null
                                ,City                                                       nvarchar(50)      null
                                ,[State]                                 nvarchar(30)      null
                                ,Zip                                                         nvarchar(10)      null
                                ,OrderedByAreaCode     int                                                           null
                                ,OrderedByPhone                           int                                                           null
                                ,SalesRep                                            nvarchar(25)      null
                                ,PONumber                                        nvarchar(30)      null
                                ,PCName                                                             nvarchar(50)      null
                                ,[Hash]                                                 varbinary(8000) not null default (0x0)
                )
end
else
begin
                truncate table #tGBCustomerJobSite
end

--Customer jobsite temp cache.
if (object_id('tempdb..#tCustJob_Cache') is null)
begin
                create table #tCustJob_Cache
                (
                                CJID                       bigint                                     not null
                                ,CJCMP                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJCUS#                decimal(7,0)       not null
                                ,CJJOB#                varchar(20)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJDLCD                                varchar(1)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJLOC                  varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJRCDT                                numeric(8,0)      not null
                                ,CJJLOC                 varchar(40)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJCOM                varchar(30)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJNAME                             varchar(30)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJADR1                               varchar(30)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJADR2                               varchar(30)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJCITY                  varchar(20)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJST                      varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJZIP                    varchar(10)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJAREA                               decimal(3,0)       not null
                                ,CJPHON                              decimal(7,0)       not null
                                ,CJTAXD                               varchar(9)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CJTERR                decimal(3,0)       not null
                                ,CJSREP                decimal(6,0)       not null
                                ,CJPO#                  varchar(26)                         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,[Hash]                 varbinary(8000) not null default (0x0)
                )
end
else
begin
                truncate table #tCustJob_Cache
end

--EQPMASFL cache.
if (object_id('integr.EQPMASFL_Cache') is null)
begin
                create table integr.EQPMASFL_Cache
                (
                                EMCMP                varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,[EMEQP#]          varchar(10)                         collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,[Hash]                 varbinary(8000)                                                                                                                                                                                not null
                                ,CacheDate         datetime                                                                                                                                                                                                             not null
                                                constraint DF_integ_EQPMASFL_Cache_CacheDate default(getdate())

                                ,constraint PK_integ_EQPMASFL_Cache_EMEQP primary key clustered ([EMEQP#], EMCMP) with (fillfactor = 80, data_compression = page)
                )
end

--EQPCCLFL cache.
if (object_id('integr.EQPCCLFL_Cache') is null)
begin
                create table integr.EQPCCLFL_Cache
                (
                                --RRN_FIELD_DATA         bigint                                                                                                                                                                                                                     not null
                                --,SYNCDTTM                     datetime2                                                                                                                                                                                                           not null
                                ELCMP                                  varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELLOC                                  varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELCATG                                               decimal(3,0)                                                                                                                                                                                       not null
                                ,ELCLAS                                decimal(4,0)                                                                                                                                                                                       not null
                                ,ELQOWN                                            decimal(6,0)                                                                                                                                                                                       not null
                                                constraint DF_integ_EQPCCLFL_Cache_ELQOWN default (0)
                                ,[Hash]                                 varbinary(8000)                                                                                                                                                                                not null
                                ,CacheDate                         datetime                                                                                                                                                                                                             not null
                                                constraint DF_integ_EQPCCLFL_Cache_CacheDate default(getdate())

                                ,constraint PK_integ_EQPCCLFL_Cache_ELCATG_ELCLAS primary key clustered (ELCATG, ELCLAS, ELLOC, ELCMP) with (fillfactor = 80, data_compression = page)
                )
end

--EQPCCLTRG cache.
if (object_id('integr.EQPCCLTRG_Cache') is null)
begin
                create table integr.EQPCCLTRG_Cache
                (
                                RRN_FIELD_DATA            bigint                                                                                                                                                                                                                     not null
                                ,SYNCDTTM                        datetime2                                                                                                                                                                                                           not null
                                ,ELEVENT                             varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELCMP                                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELLOC                                  varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS                not null
                                ,ELCATG                                               decimal(3,0)                                                                                                                                                                                       not null
                                ,ELCLAS                                decimal(4,0)                                                                                                                                                                                       not null
                                ,ELQOWN                                            decimal(6,0)                                                                                                                                                                                       not null
                                ,PrevELQOWN                   decimal(6,0)                                                                                                                                                                                       not null
                                                constraint DF_integ_EQPCCLTRG_Cache_PrevELQOWN default (0)
                                ,[Hash]                                 varbinary(8000)                                                                                                                                                                                not null
                                ,CacheDate                         datetime                                                                                                                                                                                                             not null
                                                constraint DF_integ_EQPCCLTRG_Cache_CacheDate default(getdate())

                                ,constraint PK_integ_EQPCCLTRG_Cache_ELCATG_ELCLAS primary key clustered (ELCATG, ELCLAS, ELLOC, ELCMP, ELEVENT, RRN_FIELD_DATA, SYNCDTTM) with (fillfactor = 80, data_compression = page)
                )
end

--Table to hold prior values for attributes needed by GB for auditing purposes.
if (object_id('integr.EQPMASFL_Cache_PrevAttribute') is null)
begin
                create table integr.EQPMASFL_Cache_PrevAttribute
                (
                                [EMEQP#]           varchar(10)         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,EMCMP                              varchar(2)           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,[EMSER#]           varchar(20)         collate SQL_Latin1_General_CP1_CS_AS not null
                                ,CacheDate         datetime                                                                                                                                                                                                             not null
                                                                constraint DF_integ_EQPMASFL_Cache_PrevAttribute_CacheDate default(getdate())
                                ,constraint PK_integ_EQPMASFL_Cache_PrevAttribute_EMEQP_EMCMP primary key clustered ([EMEQP#], EMCMP) with (fillfactor = 80, data_compression = page)
                )
end

--EQPCCLTRG queue table.
--Queue tables hold any rows we held off on sending to GB because all the necessary information was not available.
if (object_id('integr.BulkItemToSend_Queue') is null)
begin
                create table integr.BulkItemToSend_Queue
                (
                                QueueRowID                                     bigint                                                                                                                                                                                                                     not null identity(1,1)
                                ,SyncSessionID                  uniqueidentifier                                                                                                                                                                               not null
                                ,RJCMP                                                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                 not null
                                ,RJTLOC                                                varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS                 not null
                                ,RJCATG                                                               numeric(3,0)                                                                                                                                                                                      null
                                ,RJCLAS                                                numeric(4,0)                                                                                                                                                                                      null
                                ,RJQTYR                                                                numeric(7,2)                                                                                                                                                                                      null
                                ,RJSTTS                                                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                 null
                                --,ELEVENT                                          varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS                 null
                                --,RRN_FIELD_DATA                        bigint                                                                                                                                                                                                                     null
                                ,QueueReasonCode        int                                                                                                                                                                                                                                           not null default (0)
                                ,DetectionDate                 datetime                                                                                                                                                                                                             not null
                                ,SendDate                                           datetime                                                                                                                                                                                                             not null default ('1/1/1900')
                )
end

if (object_id('integr.EQPMASFL_Queue') is null)
begin
                create table integr.EQPMASFL_Queue
                (
                                QueueRowID                                     bigint                                                     not null identity(1,1)
                                                constraint PK_integr_EQPMASFL_QUEUE_RowID primary key clustered with (fillfactor = 80)

                                ,SyncSessionID                  uniqueidentifier                                                                                                                                                                               not null
                                ,EMCMP                                                              varchar(2)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,[EMEQP#]                                          varchar(10)                                         collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,EMCLOC                                                             varchar(4)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,CAT                                                       varchar(3)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,CLAS                                                     varchar(4)                                           collate SQL_Latin1_General_CP1_CS_AS              not null
                                ,QueueReasonCode        int                                                                                                                                                                                                                                           not null default (0)
                                ,DetectionDate                 datetime                                                                                                                                                                                                             not null
                                ,SendDate                                           datetime                                                                                                                                                                                                             not null default ('1/1/1900')
                )
end

--CUSJOBFL Cache.
if (object_id('integr.CUSJOBFL_Cache') is null)
begin
                create table integr.CUSJOBFL_Cache
                (
                                CJID                       bigint                                     not null
                                                constraint            PK_integr_CUSJOBFL_Cache_CJID            primary key clustered with (fillfactor = 80)

                                ,CJCMP                 varchar(2)                           not null
                                ,CJCUS#                decimal(7,0)       not null
                                ,CJJOB#                varchar(20)                         not null
                                ,CJDLCD                                varchar(1)                           not null
                                ,CJLOC                  varchar(4)                           not null
                                ,CJRCDT                                numeric(8,0)      not null
                                ,CJJLOC                 varchar(40)                         not null
                                ,CJCOM                varchar(30)                         not null
                                ,CJNAME                             varchar(30)                         not null
                                ,CJADR1                               varchar(30)                         not null
                                ,CJADR2                               varchar(30)                         not null
                                ,CJCITY                  varchar(20)                         not null
                                ,CJST                      varchar(2)                           not null
                                ,CJZIP                    varchar(10)                         not null
                                ,CJAREA                               decimal(3,0)       not null
                                ,CJPHON                              decimal(7,0)       not null
                                ,CJTAXD                               varchar(9)                           not null
                                ,CJTERR                decimal(3,0)       not null
                                ,CJSREP                decimal(6,0)       not null
                                ,CJPO#                  varchar(26)                         not null
                                ,[Hash]                 varbinary(8000)                                                                                                                                                                                not null
                                ,CacheDate         datetime                                                                                                                                                                                                             not null
                                                constraint DF_integr_CUSJOBFL_Cache_CacheDate default(getdate())
                )
end
else
begin
                truncate table integr.CUSJOBFL_Cache
end


--This table stores the bulk items that we will need to send to GB.
--This table is necessary because while a change may have occurred on a bulk item
--such as its CAT/CLASS changing to a new CAT/CLASS that does not yet exist in GB,
--in which case we want to send the new CAT/CLASS as a record to GB, we only want to
--send the bulk item itself only if there was an add made in Wynne for that type of
--bulk item, that is, if the quantity in Wynne increased.
if (object_id('tempdb..#tBulkItemToSend') is null)
begin
                create table #tBulkItemToSend
                (
                                RJCMP                                  varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,RJTLOC                                varchar(4)                           collate SQL_Latin1_General_CP1_CS_AS not null
                                ,RJCATG                                               numeric(3,0)      null
                                ,RJCLAS                                numeric(4,0)      null
                                ,RJQTYR                                                numeric(7,2)      null
                                ,RJSTTS                                 varchar(2)                           collate SQL_Latin1_General_CP1_CS_AS null
                )
end
else
begin
                truncate table #tBulkItemToSend
end
-------------------------* Create the necessary tables *-------------------------




------------------------- Verify all of our config values are not null or blank -------------------------
if (isnull(@remoteServer, '') = '')
begin
                raiserror('Remote server config value not found.', 18, 1)
end

if (isnull(@remoteDB, '') = '')
begin
                raiserror('Remote database config value not found.', 18, 1)
end

if (isnull(@remoteSchema, '') = '')
begin
                raiserror('Remote schema config value not found.', 18, 1)
end

if (isnull(@sourceDB, '') = '')
begin
                raiserror('Source database config value not found.', 18, 1)
end

if (isnull(@sourceSchema, '') = '')
begin
                raiserror('Source schema config value not found.', 18, 1)
end

if (isnull(@targetDB, '') = '')
begin
                raiserror('Target database config value not found.', 18, 1)
end

if (isnull(@targetSchema, '') = '')
begin
                raiserror('Target schema config value not found.', 18, 1)
end

if (@startTime is null)
begin
                raiserror('Start time config value not found.', 18, 1)
end

if (@edwServer is null)
begin
                raiserror('EDW server config value not found.', 18, 1)
end

if (@edwDB is null)
begin
                raiserror('EDW database name config value not found.', 18, 1)
end

if (@edwSchema is null)
begin
                raiserror('EDW schema name config value not found.', 18, 1)
end

--The start time and end time are stored as UTC.
if (@startTime = '1/1/1900')
begin
                set @startTime =             getdate()
end
else
begin
                set @startTime =             @prevStartTime
end

set @endTime   =             getdate()

--             run                                         start                       end                        prev
--             ----                         -----                        ---                           ----
--             10:00                     10:00                     10:01     10:00
--             10:15                     10:00                     10:15     10:15
--             10:30                     10:15                     10:30     10:15

--Is the start and end time range valid?
if (@startTime > @endTime)
begin
                raiserror('Invalid start and end time.  The start time cannot be greater than the end time.', 18, 1)
end
-------------------------* Verify all of our config values are not null or blank *-------------------------




------------------------- Build the statement that will hash all the columns for the customer jobsite table  REF5 -------------------------
--Get customer job site info from Wynne.
insert into #tCustJob_Cache
(
                CJID       
                ,CJCMP 
                ,CJCUS# 
                ,CJJOB# 
                ,CJDLCD                
                ,CJLOC  
                ,CJRCDT                
                ,CJJLOC 
                ,CJCOM 
                ,CJNAME             
                ,CJADR1               
                ,CJADR2               
                ,CJCITY  
                ,CJST      
                ,CJZIP    
                ,CJAREA               
                ,CJPHON              
                ,CJTAXD               
                ,CJTERR 
                ,CJSREP 
                ,CJPO#  
)
exec integr.GetWynneCustomerJobSite

--Get the hashbyte expression to use for the given table.
set @hashExprOut          =             ''
set @sourceTB                  =             '#tCustJob_Cache'
exec integr.GetHashExpression
                @db                                                      =             'tempdb'
                ,@schema                           =             'dbo'
                ,@tableName                    =             @sourceTB
                ,@hashExprOut =             @hashExprOut output

--Update the temp job site table using the hash expression.
update t
set
                t.[Hash] =             convert(varbinary(8000), @hashExprOut)
from #tCustJob_Cache t

--Add the SQL statement, using the hash statement that will populate the cache table.
set @sql = replace('select CJID,CJCMP,CJCUS#,CJJOB#,CJDLCD,CJLOC,CJRCDT,CJJLOC,CJCOM,CJNAME,CJADR1,CJADR2,CJCITY,CJST,CJZIP,CJAREA,CJPHON,CJTAXD,CJTERR,CJSREP,CJPO#,[hashByteExpr] as [Hash] from [sourceDB].[sourceSchema].[sourceTB] t with (updlock, paglock)', '[hashByteExpr]', @hashExprOut)
set @sql = replace(@sql, '[sourceDB]', 'tempdb')
set @sql = replace(@sql, '[sourceTB]', '#tCustJob_Cache')
set @sql = replace(@sql, '[sourceSchema]', 'dbo')

set @sql = 'insert into integr.[targetTB] (CJID,CJCMP,CJCUS#,CJJOB#,CJDLCD,CJLOC,CJRCDT,CJJLOC,CJCOM,CJNAME,CJADR1,CJADR2,CJCITY,CJST,CJZIP,CJAREA,CJPHON,CJTAXD,CJTERR,CJSREP,CJPO#,[Hash]) ' + @sql
set @sql = replace(@sql, '[targetTB]', 'CUSJOBFL_Cache')
exec (@sql)

--Get GB customer job site.
exec FMQAGBSQL19.Integration.gbw.getCustomerJobSite @sessionID = @sessionID
set @compData = (select t.CompData from FMQAGBSQL19.Integration.gbw.CompData t where t.SessionID = @sessionID and t.Op = 'gbw.getCustomerJobSite')
set @json = cast(decompress(@compData) as nvarchar(max))

--Get response from  GB.
if (@json is not null)
begin
                insert into #tGBCustomerJobSite
                (
                                CJID                                                       
                                ,HQLocationID
                                ,CompanyCode                 
                                ,AccountNumber                             
                                ,JobNumber                                       
                                ,JobLocation                       
                                ,ContactName                   
                                ,[Name]                                                               
                                ,Address1                                            
                                ,Address2                                            
                                ,City                                                       
                                ,[State]                                 
                                ,Zip                                                         
                                ,OrderedByAreaCode     
                                ,OrderedByPhone                           
                                ,SalesRep                                            
                                ,PONumber                                        
                                ,PCName
                )
                select
                                CJID                                                       
                                ,HQLocationID
                                ,CompanyCode                 
                                ,AccountNumber                             
                                ,JobNumber                                       
                                ,JobLocation                       
                                ,ContactName                   
                                ,[Name]                                                               
                                ,Address1                                            
                                ,Address2                                            
                                ,City                                                       
                                ,[State]                                 
                                ,Zip                                                         
                                ,OrderedByAreaCode     
                                ,OrderedByPhone                           
                                ,SalesRep                                            
                                ,PONumber                                        
                                ,PCName
                from openjson(@json)
                with
                (
                                CJID                                                       bigint                                     '$.CJID'
                                ,HQLocationID                   bigint                                     '$.ID'
                                ,CompanyCode                 nvarchar(2)                         '$.CompanyCode'
                                ,AccountNumber                             nvarchar(50)      '$.AccountNumber'
                                ,JobNumber                                       nvarchar(50)      '$.JobNumber'
                                ,JobLocation                       nvarchar(40)      '$.JobLocation'
                                ,ContactName                   nvarchar(50)      '$.ContactName'
                                ,[Name]                                                               nvarchar(50)      '$.Name'
                                ,Address1                                            nvarchar(100)    '$.Address1'
                                ,Address2                                            nvarchar(100)    '$.Address2'
                                ,City                                                       nvarchar(50)      '$.City'
                                ,[State]                                 nvarchar(30)      '$.State'
                                ,Zip                                                         nvarchar(10)      '$.Zip'
                                ,OrderedByAreaCode     int                                                           '$.OrderedByAreaCode'
                                ,OrderedByPhone                           int                                                           '$.OrderedByPhone'
                                ,SalesRep                                            nvarchar(25)      '$.SalesRep'
                                ,PONumber                                        nvarchar(30)      '$.PONumber'
                                ,PCName                                                             nvarchar(50)      '$.PCName'
                ) a
end
else
begin
                raiserror('Failed to retrieve JSON for GB customer job sites.', 18, 1)
end
-------------------------* Build the statement that will hash all the columns for the customer jobsite table  REF5 *-------------------------



----debug
--select * from #tCustJob_Cache
--select * from #tGBCustomerJobSite
--return
----debug

set @json            =
(
                select
                                custJob.CJID
                                ,custJob.HQLocationID
                                ,custJob.CompanyCode
                                ,custJob.AccountNumber
                                ,custJob.JobNumber
                                ,custJob.JobLocation
                                ,custJob.ContactName
                                ,custJob.[Name]
                                ,custJob.Address1
                                ,custJob.Address2
                                ,custJob.City
                                ,custJob.[State]
                                ,custJob.Zip
                                ,custJob.OrderedByAreaCode
                                ,custJob.OrderedByPhone
                                ,custJob.SalesRep
                                ,custJob.PONumber
                from
                (
                                --Try and match GB customer job site based on CJID.
                                select
                                                wynneCache.CJID
                                                ,gbCache.HQLocationID
                                                ,convert(nvarchar(2), wynneCache.CJCMP)                          as            CompanyCode
                                                ,convert(nvarchar(50), wynneCache.CJCUS#)       as            AccountNumber
                                                ,convert(nvarchar(50), wynneCache.CJJOB#)       as            JobNumber
                                                ,convert(nvarchar(40), wynneCache.CJJLOC)        as            JobLocation
                                                ,convert(nvarchar(50), wynneCache.CJCOM)       as            ContactName
                                                ,convert(nvarchar(50), wynneCache.CJNAME)     as            [Name]
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address1
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address2
                                                ,convert(nvarchar(50), wynneCache.CJCITY)        as            City
                                                ,convert(nvarchar(30), wynneCache.CJST)                            as            [State]
                                                ,convert(nvarchar(10), wynneCache.CJZIP)           as            Zip
                                                ,convert(int, wynneCache.CJAREA)                                          as            OrderedByAreaCode
                                                ,convert(int, wynneCache.CJPHON)                                         as            OrderedByPhone
                                                ,convert(nvarchar(25), wynneCache.CJSREP)       as            SalesRep
                                                ,convert(nvarchar(30), wynneCache.CJPO#)         as            PONumber
                                from #tCustJob_Cache wynneCache
                                join #tGBCustomerJobSite gbCache
                                                on gbCache.CJID                                                                              =             wynneCache.CJID
                                union

                                --Try and match GB customer job site based on CompanyCode, PCName, Customer (AccountNumber), JobNumber and JobLocation.
                                select
                                                wynneCache.CJID
                                                ,gbCache.HQLocationID
                                                ,convert(nvarchar(2), wynneCache.CJCMP)                          as            CompanyCode
                                                ,convert(nvarchar(50), wynneCache.CJCUS#)       as            AccountNumber
                                                ,convert(nvarchar(50), wynneCache.CJJOB#)       as            JobNumber
                                                ,convert(nvarchar(40), wynneCache.CJJLOC)        as            JobLocation
                                                ,convert(nvarchar(50), wynneCache.CJCOM)       as            ContactName
                                                ,convert(nvarchar(50), wynneCache.CJNAME)     as            [Name]
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address1
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address2
                                                ,convert(nvarchar(50), wynneCache.CJCITY)        as            City
                                                ,convert(nvarchar(30), wynneCache.CJST)                            as            [State]
                                                ,convert(nvarchar(10), wynneCache.CJZIP)           as            Zip
                                                ,convert(int, wynneCache.CJAREA)                                          as            OrderedByAreaCode
                                                ,convert(int, wynneCache.CJPHON)                                         as            OrderedByPhone
                                                ,convert(nvarchar(25), wynneCache.CJSREP)       as            SalesRep
                                                ,convert(nvarchar(30), wynneCache.CJPO#)         as            PONumber
                                from #tCustJob_Cache wynneCache
                                join #tGBCustomerJobSite gbCache
                                                on gbCache.CompanyCode                                          =             convert(nvarchar(2), wynneCache.CJCMP)                     collate SQL_Latin1_General_CP1_CS_AS
                                                and substring(gbCache.PCName, 3, 10)   =             convert(nvarchar(50), wynneCache.CJLOC)                                collate SQL_Latin1_General_CP1_CS_AS
                                                and gbCache.AccountNumber                    =             convert(nvarchar(50), wynneCache.CJCUS#)
                                                and gbCache.JobNumber                                             =             convert(nvarchar(50), wynneCache.CJJOB#)     collate SQL_Latin1_General_CP1_CS_AS
                                                and gbCache.JobLocation                                             =             convert(nvarchar(40), wynneCache.CJJLOC)     collate SQL_Latin1_General_CP1_CS_AS
                                union
                                select
                                                wynneCache.CJID
                                                ,gbCache.HQLocationID
                                                ,convert(nvarchar(2), wynneCache.CJCMP)                          as            CompanyCode
                                                ,convert(nvarchar(50), wynneCache.CJCUS#)       as            AccountNumber
                                                ,convert(nvarchar(50), wynneCache.CJJOB#)       as            JobNumber
                                                ,convert(nvarchar(40), wynneCache.CJJLOC)        as            JobLocation
                                                ,convert(nvarchar(50), wynneCache.CJCOM)       as            ContactName
                                                ,convert(nvarchar(50), wynneCache.CJNAME)     as            [Name]
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address1
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address2
                                                ,convert(nvarchar(50), wynneCache.CJCITY)        as            City
                                                ,convert(nvarchar(30), wynneCache.CJST)                            as            [State]
                                                ,convert(nvarchar(10), wynneCache.CJZIP)           as            Zip
                                                ,convert(int, wynneCache.CJAREA)                                          as            OrderedByAreaCode
                                                ,convert(int, wynneCache.CJPHON)                                         as            OrderedByPhone
                                                ,convert(nvarchar(25), wynneCache.CJSREP)       as            SalesRep
                                                ,convert(nvarchar(30), wynneCache.CJPO#)         as            PONumber
                                from #tCustJob_Cache wynneCache
                                join #tGBCustomerJobSite gbCache
                                                on gbCache.CompanyCode                                                                          =             convert(nvarchar(2), wynneCache.CJCMP)                     collate SQL_Latin1_General_CP1_CS_AS
                                                and substring(gbCache.PCName, 3, 10)   =             convert(nvarchar(50), wynneCache.CJLOC)                                collate SQL_Latin1_General_CP1_CS_AS
                                union

                                --Find using just the branch key.
                                --GB does not have the Company construct so this is a DANGER because Canada could have the same branch key.
                                --Mitigated right now because we're sourcing the branch from EDW which does have company code.
                                --However, to be safe, at some point, GB should introduce a company lookup in dbo.FSI_LOCATIONS.  See REF6.
                                select
                                                wynneCache.CJID
                                                ,gbCache.HQLocationID
                                                ,convert(nvarchar(2), wynneCache.CJCMP)                          as            CompanyCode
                                                ,convert(nvarchar(50), wynneCache.CJCUS#)       as            AccountNumber
                                                ,convert(nvarchar(50), wynneCache.CJJOB#)       as            JobNumber
                                                ,convert(nvarchar(40), wynneCache.CJJLOC)        as            JobLocation
                                                ,convert(nvarchar(50), wynneCache.CJCOM)       as            ContactName
                                                ,convert(nvarchar(50), wynneCache.CJNAME)     as            [Name]
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address1
                                                ,convert(nvarchar(100), wynneCache.CJADR1)    as            Address2
                                                ,convert(nvarchar(50), wynneCache.CJCITY)        as            City
                                                ,convert(nvarchar(30), wynneCache.CJST)                            as            [State]
                                                ,convert(nvarchar(10), wynneCache.CJZIP)           as            Zip
                                                ,convert(int, wynneCache.CJAREA)                                          as            OrderedByAreaCode
                                                ,convert(int, wynneCache.CJPHON)                                         as            OrderedByPhone
                                                ,convert(nvarchar(25), wynneCache.CJSREP)       as            SalesRep
                                                ,convert(nvarchar(30), wynneCache.CJPO#)         as            PONumber
                                from #tCustJob_Cache wynneCache
                                join #tGBCustomerJobSite gbCache
                                                on substring(gbCache.PCName, 3, 10)     =             convert(nvarchar(50), wynneCache.CJLOC)                                collate SQL_Latin1_General_CP1_CS_AS
                ) custJob for json path
)

----debug
--select @json
--return
----debug

if (@json is not null)
                begin
                                set @compData = compress(@json)
                                exec FMQAGBSQL19.Integration.gbw.upsertCustomerJobSite
                                                @sessionID = @sessionID
                                                ,@comp = @compData
                end

----debug
--select * from #tCustJob_Cache
--select * from #tGBCustomerJobSite
--return
----debug


--Get active PCs.
exec FMQAGBSQL19.Integration.gbw.getActivePCList @sessionID = @sessionID
set @compData = (select t.CompData from FMQAGBSQL19.Integration.gbw.CompData t where t.SessionID = @sessionID and t.Op = 'gbw.getActivePCList')
set @json = cast(decompress(@compData) as nvarchar(max))

--Get response from  GB.
if (@json is not null)
begin
                insert into #tGBActivePC (ID, PC)
                select
                                a.ID
                                ,a.PC
                from openjson(@json)
                with
                (
                                ID                                                                            bigint                                     '$.ID'
                                ,PC                                                                         nvarchar(50)      '$.PC'
                ) a
end
else
begin
                raiserror('Failed to retrieve JSON for active PC list.', 18, 1)
end

--Get GB SKU list.
exec FMQAGBSQL19.Integration.gbw.getSKUList @sessionID = @sessionID
set @compData = (select t.CompData from FMQAGBSQL19.Integration.gbw.CompData t where t.SessionID = @sessionID and t.Op = 'gbw.getSKUList')
set @json = cast(decompress(@compData) as nvarchar(max))

--Get response from GB.
if (@json is not null)
begin
                insert into #tGBSKUList
                (
                                ID
                                ,DateAdded
                                ,LastUpdated
                                ,HQLocationID
                                ,UPC
                                ,ProductID
                )
                select
                                a.ID                                                        
                                ,a.DateAdded                    
                                ,a.LastUpdated 
                                ,a.HQLocationID               
                                ,a.UPC                                  
                                ,a.ProductID                       
                from openjson(@json)
                with
                (
                                ID                                                                            bigint                                     '$.ID'
                                ,DateAdded                                       datetime                             '$.DateAdded'
                                ,LastUpdated                     datetime                             '$.LastUpdated'
                                ,HQLocationID                   bigint                                     '$.HQLocationID'
                                ,UPC                                                      nvarchar(50)      '$.UPC'
                                ,ProductID                                          nvarchar(25)      '$.ProductID'
                ) a
end
else
begin
                raiserror('Failed to retrieve JSON for SKU list.', 18, 1)
end

--Get GB inventory.
exec FMQAGBSQL19.Integration.gbw.getInventory @sessionID = @sessionID
set @compData = (select t.CompData from FMQAGBSQL19.Integration.gbw.CompData t where t.SessionID = @sessionID and t.Op = 'gbw.getInventory')
set @json = cast(decompress(@compData) as nvarchar(max))

--Get response from GB.
if (@json is not null)
begin
                insert into #tGBInventory
                (
                                ID
                                ,DateAdded
                                ,LastUpdated
                                ,HQLocationID
                                ,HQSKUID
                                ,TagID
                                ,[Status]
                                ,AssetID
                                ,IsActive
                )
                select
                                a.ID                                                        
                                ,a.DateAdded                    
                                ,a.LastUpdated 
                                ,a.HQLocationID               
                                ,a.HQSKUID                        
                                ,a.TagID                                                
                                ,a.[Status]                           
                                ,a.AssetID                           
                                ,a.IsActive                           
                from openjson(@json)
                with
                (
                                ID                                                                            bigint                                     '$.ID'
                                ,DateAdded                                       datetime                             '$.DateAdded'
                                ,LastUpdated                     datetime                             '$.LastUpdated'
                                ,HQLocationID                   bigint                                     '$.HQLocationID'
                                ,HQSKUID                                            bigint                                     '$.HQSKUID'
                                ,TagID                                                   nvarchar(50)      '$.TagID'
                                ,[Status]                                               nvarchar(50)      '$.Status'
                                ,AssetID                                               nvarchar(100)    '$.AssetID'
                                ,IsActive                                               bit                                                           '$.IsActive'
                ) a
end
else
begin
                raiserror('Failed to retrieve JSON for GB inventory.', 18, 1)
end

----debug
--select * from #tGBActivePC
--select * from #tGBInventory
--return
----debug

--If reload of cache required, get information about table.
if (@reloadEquipCache = 1)
begin

                ------------------------- Build the statement that will hash all the columns for the given table -------------------------
                truncate table integr.EQPMASFL_Cache

                --Get the hashbyte expression to use for the given table.
                set @hashExprOut          =             ''
                set @sourceTB                  =             'EQPMASFL'
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@hashExprOut =             @hashExprOut output
                -------------------------* Build the statement that will hash all the columns for the given table *-------------------------

                --Add the SQL statement, using the hash statement that will populate the cache table.
                set @sql = replace('select t.EMCMP, t.[EMEQP#], [hashByteExpr] as [Hash] from [sourceDB].[sourceSchema].[sourceTB] t with (updlock, paglock)', '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)

                set @sql = 'insert into integr.[sourceTB]_Cache (EMCMP, [EMEQP#], [Hash]) ' + @sql
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                exec (@sql)

                truncate table integr.EQPMASFL_Cache_PrevAttribute
                insert into integr.EQPMASFL_Cache_PrevAttribute ([EMEQP#], EMCMP, [EMSER#])
                select
                                trim(em.[EMEQP#])
                                ,trim(em.EMCMP)
                                ,trim(em.[EMSER#])
                from WSDATA.WSDATA.EQPMASFL em with (updlock, paglock)
                join #tGBActivePC pc
                                on pc.CMP                          =             em.EMCMP
                                and pc.PC                            =             em.EMALOC
                                and em.EMSTAT                               =             'A'
end

--If reload of cache for bulk (EQPCCLTRG) required, get information about table.
if (@reloadBulkCache = 1)
begin

                ------------------------- Build the statement that will hash all the columns for the given table -------------------------
                truncate table integr.EQPCCLTRG_Cache

                --Get the hashbyte expression to use for the given table.
                set @hashExprOut          =             ''
                set @sourceTB                  =             'EQPCCLTRG'
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@hashExprOut =             @hashExprOut output
                -------------------------* Build the statement that will hash all the columns for the given table *-------------------------

                --Add the SQL statement, using the hash statement that will populate the cache table.
                set @sql = replace('select t.SYNCDTTM, t.RRN_FIELD_DATA, t.ELEVENT, t.ELCATG, t.ELCLAS, t.ELLOC, t.ELCMP, t.ELQOWN, [hashByteExpr] as [Hash] from [sourceDB].[sourceSchema].[sourceTB] t with (updlock, paglock)', '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)

                set @sql = 'insert into integr.[sourceTB]_Cache (SYNCDTTM, RRN_FIELD_DATA, ELEVENT, ELCATG, ELCLAS, ELLOC, ELCMP, ELQOWN, [Hash]) ' + @sql
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                --print @sql
                exec (@sql)

                ------------------------- Build the statement that will hash all the columns for the given table -------------------------
                --Now do the same for EQPCCLFL table.
                truncate table integr.EQPCCLFL_Cache

                --Get the hashbyte expression to use for the given table.
                set @hashExprOut          =             ''
                set @sourceTB                  =             'EQPCCLFL'
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@hashExprOut =             @hashExprOut output
                -------------------------* Build the statement that will hash all the columns for the given table *-------------------------

                --Add the SQL statement, using the hash statement that will populate the cache table.
                set @sql = replace('select t.ELCATG, t.ELCLAS, t.ELLOC, t.ELCMP, t.ELQOWN, [hashByteExpr] as [Hash] from [sourceDB].[sourceSchema].[sourceTB] t', '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)

                set @sql = 'insert into integr.[sourceTB]_Cache (ELCATG, ELCLAS, ELLOC, ELCMP, ELQOWN, [Hash]) ' + @sql
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                --print @sql
                exec (@sql)

                --Now determine what the prior ELQOWN values were for the rows we have in the trigger table.
                update c
                set
                                c.PrevELQOWN =             a.PrevELQOWN
                from integr.EQPCCLTRG_Cache c
                join
                (
                                select
                                                t.RRN_FIELD_DATA
                                                ,t.SYNCDTTM
                                                ,t.ELEVENT
                                                ,t.ELCMP
                                                ,t.ELLOC
                                                ,t.ELCATG
                                                ,t.ELCLAS
                                                ,t.ELQOWN
                                                ,lag(t.ELQOWN, 1, 0) over (partition by t.ELCMP, t.ELLOC, t.ELCATG, t.ELCLAS order by t.RRN_FIELD_DATA) as PrevELQOWN
                                                ,row_number() over (partition by t.ELCMP, t.ELLOC, t.ELCATG, t.ELCLAS order by t.RRN_FIELD_DATA) as RowID
                                                --,trg.SYNCDTTM as sync3
                                from WSDATA.WSDATA.EQPCCLTRG t with (updlock, paglock)
                                join
                                (
                                                select
                                                                t.ELCMP
                                                                ,t.ELLOC
                                                                ,t.ELCATG
                                                                ,t.ELCLAS
                                                from integr.EQPCCLTRG_Cache t
                                                group by
                                                                t.ELCMP
                                                                ,t.ELLOC
                                                                ,t.ELCATG
                                                                ,t.ELCLAS
                                ) agg
                                on t.ELCMP                         =             agg.ELCMP
                                                and t.ELLOC                        =             agg.ELLOC
                                                and t.ELCATG                     =             agg.ELCATG
                                                and t.ELCLAS                      =             agg.ELCLAS
                ) a
                on a.ELCATG                                                                      =             c.ELCATG
                                and a.ELCLAS                                     =             c.ELCLAS
                                and a.ELLOC                                                       =             c.ELLOC
                                and a.ELCMP                                                      =             c.ELCMP

                --For rows in the trigger table where the PrevELQOWN value is 0, dip back into the EQPCCLFL_Cache table to
                --get the ELQOWN value.  When EQPCCLTRG's ELQOWN value is 0, it indicates that there is no "prior" row the LAG()
                --can examine to get the previous ELQOWN value, so we defer back to the last known cached value!
                --This can happen because Wynne's internal process removes any row in EQPCCLTRG where the row's age is older than
                --60 days.
                --
                --After a bulk item has been added to GB, it may be several months before another bulk item of the same CAT/CLASS is
                --added to said GB.  In that case, the prior row in the trigger table may have already been pruned so we cannot depend
                --on said row existing in the trigger table.
                --REF4
                update tc
                set
                                tc.PrevELQOWN               =             c.ELQOWN
                from integr.EQPCCLTRG_Cache tc
                join integr.EQPCCLFL_Cache c
                                on c.ELCATG                                       =             tc.ELCATG
                                and c.ELCLAS                      =             tc.ELCLAS
                                and c.ELLOC                                        =             tc.ELLOC
                                and c.ELCMP                                      =             tc.ELCMP
                where
                                tc.PrevELQOWN = 0
end

--If we are just reloading the cache...
if (@reloadOnly = 1)
begin
                return
end



begin try

                --prod
                begin distributed tran trnGBSync
                --prod
                set @tranStarted = 1


                ------------------------- Get changed rows for equipment. -------------------------
                --Get EQPMASFL rows in Mimix that have changed.  First get the hashbytes statement we'll need to use.
                --This simply compares the hash of all the equipment rows in Mimix's WSDATA.EQPMASFL
                --table that have an SYNCDTTM stamp within our run interval.  If a row is found, it means that
                --an update in Wynne occurred for the equipment and Mimix sent it over.  This tells us that there
                --was a CHANGE but not WHAT changed.  We are simply using this technique to isolate the rows we
                --need to be concerned with.
                set @sourceTB                  =             'EQPMASFL'
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@hashExprOut =             @hashExprOut output

                --Construct statement to insert the changed rows into the temp cache.
                --This will be used to compare the hash value to our persistent cache.
                --If hash values are different between the two tables, we know that
                --some sort of change has been applied.
                set @sql = N'insert into #tEQPMASFL_Cache (EMCMP, [EMEQP#], ChangeDate, [Hash])
                select
                                t.EMCMP
                                ,t.[EMEQP#]
                                ,t.SYNCDTTM
                                ,[hashByteExpr] as [Hash]
                from [sourceDB].[sourceSchema].[sourceTB] t with (updlock, paglock)
                where t.SYNCDTTM between ''[startDateTime]'' and ''[endDateTime]'''

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[startDateTime]', convert(varchar(27), @startTime))
                set @sql = replace(@sql, '[endDateTime]', convert(varchar(27), @endTime))
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)

                --Store changed rows according to Mimix in temp.
                exec (@sql)
                -------------------------* Get changed rows for equipment. *-------------------------




                ------------------------- Get changed rows for bulk. -------------------------
                --Get EQPCCLTRG rows in Mimix that have changed.  First get the hashbytes statement we'll need to use.
                set @sourceTB                  =             'EQPCCLTRG'
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@hashExprOut =             @hashExprOut output

                --Construct statement to insert the changed rows into the temp cache.
                --This will be used to compare the hash value to our persistent cache.
                --If hash values are different between the two tables, we know that
                --some sort of change has been applied.
                set @sql = N'insert into #tEQPCCLTRG_Cache (SYNCDTTM, RRN_FIELD_DATA, ELEVENT, ELCMP, ELLOC, ELCATG, ELCLAS, ELQOWN, PrevELQOWN, ChangeDate, [Hash])
                select
                                t.SYNCDTTM
                                ,t.RRN_FIELD_DATA
                                ,t.ELEVENT
                                ,t.ELCMP              
                                ,t.ELLOC
                                ,t.ELCATG
                                ,t.ELCLAS
                                ,t.ELQOWN
                                ,isnull(c.PrevELQOWN, 0)
                                ,t.SYNCDTTM
                                ,[hashByteExpr] as [Hash]
                from [sourceDB].[sourceSchema].[sourceTB] t with (updlock, paglock)
                left join [targetDB].[targetSchema].EQPCCLTRG_Cache c
                                on c.ELCATG                                                       =             t.ELCATG
                                and c.ELCLAS                                      =             t.ELCLAS
                                and c.ELLOC                                                        =             t.ELLOC
                                and c.ELCMP                                                      =             t.ELCMP
                                and c.ELEVENT                                  =             t.ELEVENT
                                and c.RRN_FIELD_DATA =             t.RRN_FIELD_DATA
                                and c.SYNCDTTM                                             =             t.SYNCDTTM
                where
                                t.SYNCDTTM between ''[startDateTime]'' and ''[endDateTime]'''

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[startDateTime]', convert(varchar(27), @startTime))
                set @sql = replace(@sql, '[endDateTime]', convert(varchar(27), @endTime))
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)

                --Store changed rows according to Mimix in temp.
                exec (@sql)

                

                --Determine what the previous ELQOWN values were for the rows that changed.
                update c
                set
                                c.PrevELQOWN =             a.PrevELQOWN
                --select t.*
                from #tEQPCCLTRG_Cache c
                join
                (
                                select
                                                t.RRN_FIELD_DATA
                                                ,t.SYNCDTTM
                                                ,t.ELEVENT
                                                ,t.ELCMP
                                                ,t.ELLOC
                                                ,t.ELCATG
                                                ,t.ELCLAS
                                                ,t.ELQOWN
                                                ,lag(t.ELQOWN, 1, 0) over (partition by t.ELCMP, t.ELLOC, t.ELCATG, t.ELCLAS order by t.RRN_FIELD_DATA) as PrevELQOWN
                                                ,row_number() over (partition by t.ELCMP, t.ELLOC, t.ELCATG, t.ELCLAS order by t.RRN_FIELD_DATA) as RowID
                                from WSDATA.WSDATA.EQPCCLTRG t with (updlock, paglock)
                                join
                                (
                                                select
                                                                t.ELCMP
                                                                ,t.ELLOC
                                                                ,t.ELCATG
                                                                ,t.ELCLAS
                                                from #tEQPCCLTRG_Cache t
                                                group by
                                                                t.ELCMP
                                                                ,t.ELLOC
                                                                ,t.ELCATG
                                                                ,t.ELCLAS
                                ) agg
                                on t.ELCMP                                         =             agg.ELCMP
                                                and t.ELLOC                                        =             agg.ELLOC
                                                and t.ELCATG                     =             agg.ELCATG
                                                and t.ELCLAS                      =             agg.ELCLAS
                ) a
                on a.ELCATG                                                                      =             c.ELCATG
                                and a.ELCLAS                                     =             c.ELCLAS
                                and a.ELLOC                                                       =             c.ELLOC
                                and a.ELCMP                                                      =             c.ELCMP
                                and a.RRN_FIELD_DATA =             c.RRN_FIELD_DATA

                ----debug
                --select 'before', * from #tEQPCCLTRG_Cache t where t.ELLOC = '0400' and t.ELCATG = 9 and t.ELCLAS = 20
                ----debug

                --REF4
                --If no "prior" row is available in the trigger table, dip back into the cache of EQPCCLFL to get the last known
                --ELQOWN value.
                update tc
                set
                                tc.PrevELQOWN               =             c.ELQOWN
                --from integr.EQPCCLTRG_Cache tc
                from #tEQPCCLTRG_Cache tc
                join integr.EQPCCLFL_Cache c
                                on c.ELCATG                                       =             tc.ELCATG
                                and c.ELCLAS                      =             tc.ELCLAS
                                and c.ELLOC                                        =             tc.ELLOC
                                and c.ELCMP                                      =             tc.ELCMP
                where
                                tc.PrevELQOWN = 0
                -------------------------* Get changed rows for bulk. *-------------------------




                ------------------------- Determine if rows exist in our cache (equipment) -------------------------
                --Which equipment rows have changed or were inserted?
                --Note that we only care about the "active" PCs defined in GB.
                set @sql               =             '
                insert into #tEQPMASFL_Change
                (
                                [EMEQP#]
                                ,EMCMP
                                ,ChangeType
                )
                select
                                chg.[EMEQP#]
                                ,chg.EMCMP
                                ,chg.ChangeType
                from
                (
                                --Updated rows.
                                select
                                                tc.[EMEQP#]
                                                ,tc.EMCMP
                                                ,c.CacheDate
                                                ,tc.ChangeDate
                                                ,0                                                                            as IsBulk
                                                ,''UPD''                                  as ChangeType
                                from #tEQPMASFL_Cache tc
                                join integr.EQPMASFL_Cache c
                                                on c.[EMEQP#] =             tc.[EMEQP#]
                                                and c.EMCMP                    =             tc.EMCMP
                                where
                                                tc.[Hash]                              <>           c.[Hash]

                                union

                                --New rows.
                                select
                                                tc.[EMEQP#]
                                                ,tc.EMCMP
                                                ,''1/1/1900''
                                                ,tc.ChangeDate
                                                ,0                                                                            as IsBulk
                                                ,''NEW''                                 as ChangeType
                                from #tEQPMASFL_Cache tc
                                left join integr.EQPMASFL_Cache c
                                                on c.[EMEQP#] =             tc.[EMEQP#]
                                                and c.EMCMP                    =             tc.EMCMP
                                where
                                                c.CacheDate is null
                ) chg
                join [sourceDB].[sourceSchema].[sourceTB] e with (updlock, paglock)
                                on e.EMCMP                     =             chg.EMCMP
                                and e.[EMEQP#]              =             chg.[EMEQP#]
                join #tGBActivePC pc
                                on pc.CMP                          =             e.EMCMP
                                and pc.PC                            =             e.EMCLOC'

                set @sourceTB = 'EQPMASFL'
                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                exec (@sql)
                -------------------------* Determine if rows exist in our cache (equipment) *-------------------------



                -------------------------Are there any queued serialized items? -------------------------
                --If so, put them into our change table so that they can be reprocessed.
                --REF2
                insert into #tEQPMASFL_Change
                (
                                [EMEQP#]
                                ,EMCMP
                                ,ChangeType
                )
                select
                                q.[EMEQP#]
                                ,q.EMCMP
                                ,'UPD'                                    as ChangeType
                from integr.EQPMASFL_Queue q
                left join #tEQPMASFL_Change chg
                                on chg.[EMEQP#]                            =             q.[EMEQP#]
                                and chg.EMCMP                               =             q.EMCMP
                where
                                q.SendDate                                        =             '1/1/1900'
                                and chg.[EMEQP#]          is null
                                and chg.EMCMP                               is null



                
                ------------------------- Determine if rows exist in our cache (bulk) -------------------------
                --Which bulk item rows have changed or were inserted?
                --Note that we only care about the "active" PCs defined in GB.
                set @sql               =             '
                insert into #tEQPCCLTRG_Change
                (
                                ELCATG
                                ,ELCLAS
                                ,ELLOC
                                ,ELCMP
                                ,ELEVENT
                                ,RRN_FIELD_DATA
                                ,SYNCDTTM
                                ,QtyToSend
                                ,ChangeType
                )
                select distinct
                                chg.ELCATG
                                ,chg.ELCLAS
                                ,chg.ELLOC
                                ,chg.ELCMP
                                ,chg.ELEVENT
                                ,chg.RRN_FIELD_DATA
                                ,chg.SYNCDTTM
                                ,chg.QtyToSend
                                ,chg.ChangeType
                from
                (
                                --New rows to send.
                                select distinct
                                                tc.ELCATG
                                                ,tc.ELCLAS
                                                ,tc.ELLOC
                                                ,tc.ELCMP
                                                ,tc.ELEVENT
                                                ,tc.RRN_FIELD_DATA
                                                ,tc.SYNCDTTM
                                                ,case
                                                                when tc.ELQOWN - tc.PrevELQOWN > 0 then tc.ELQOWN - tc.PrevELQOWN
                                                                else 0
                                                end                                                                        as QtyToSend
                                                ,''1/1/1900''                         as CacheDate
                                                ,tc.ChangeDate
                                                ,1                                                                            as IsBulk
                                                ,case
                                                                when c.ELCATG is null and c.ELCLAS is null and c.ELLOC is null then            ''NEW''
                                                                else ''UPD''
                                                end                                        as ChangeType
                                from #tEQPCCLTRG_Cache tc
                                join [sourceDB].[sourceSchema].EQPCCMFL ccm with (updlock, paglock)
                                                on ccm.ECCMP  =             tc.ELCMP
                                                and ccm.ECCATG              =             tc.ELCATG
                                                and ccm.ECCLAS               =             tc.ELCLAS
                                                and ccm.ECBULK              =             ''Y''
                                left join integr.EQPCCLTRG_Cache c
                                                on c.ELCATG                                                       =             tc.ELCATG
                                                and c.ELCLAS                                      =             tc.ELCLAS
                                                and c.ELLOC                                                        =             tc.ELLOC
                                                and c.ELCMP                                                      =             tc.ELCMP
                                                and c.ELEVENT                                  =             tc.ELEVENT
                                                and c.RRN_FIELD_DATA =             tc.RRN_FIELD_DATA
                                where
                                                tc.SYNCDTTM                                    between ''[startDateTime]'' and ''[endDateTime]''
                ) chg
                join [sourceDB].[sourceSchema].[sourceTB] il with (updlock, paglock)
                                on il.ELCMP                                        =             chg.ELCMP
                                and il.ELLOC                        =             chg.ELLOC
                                and il.ELCATG                    =             chg.ELCATG
                                and il.ELCLAS                      =             chg.ELCLAS
                join #tGBActivePC pc
                                on pc.PC                                              =             il.ELLOC
                where
                                chg.QtyToSend > 0'

                set @sourceTB = 'EQPCCLTRG'
                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[startDateTime]', @startTime)
                set @sql = replace(@sql, '[endDateTime]', @endTime)
                exec (@sql)
                -------------------------* Determine if rows exist in our cache (bulk) *-------------------------



                
                ------------------------- Get the SKUs (CAT/CLASS) to send to GB -------------------------
                --Based on the rows that have changed, populate the Wynne SKU list table for equipment.
                set @sql               =
                'insert into #tWynneSKUList
                (
                                GB_PCID
                                ,CMP
                                ,PC                                                         
                                ,CAT
                                ,CLAS
                                ,ProductID                          
                                ,[Description]    
                                ,UPC                                      
                                ,SKUListType      
                                ,IsConsumable
                )
                select distinct
                                pc.ID
                                ,e.EMCMP
                                ,e.EMCLOC
                                ,e.EMCATG
                                ,e.EMCLAS
                                ,right(''0000'' + trim(convert(varchar(4), e.EMCLAS)), 4)                                                                                                                                                                                                                                                                  as ProductID
                                ,ec.ECDESC                                                                                                                                                                                                                                                                                                                                                                                                                                                                         as [Description]
                                ,right(''000'' + trim(convert(varchar(3), e.EMCATG)), 3)                                                                                                                                                                                                                                                                   as UPC
                                ,right(''000'' + trim(convert(varchar(4), e.EMCATG)), 3) + ''-'' + right(''0000'' + trim(convert(varchar(4), e.EMCLAS)), 4)  as SKUListType
                                ,0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            as IsConsumable
                from #tEQPMASFL_Change chg
                join [sourceDB].[sourceSchema].EQPMASFL e with (updlock, paglock)
                                on chg.EMCMP                 =             e.EMCMP
                                and chg.[EMEQP#]          =             e.[EMEQP#]
                                and e.EMDLCD                  =             ''A''                                                                                                                                         --Active equipment only.
                join [sourceDB].[sourceSchema].EQPCCMFL ec with (updlock, paglock)
                                on e.EMCMP                                     =             ec.ECCMP
                                and e.EMCATG                 =             ec.ECCATG
                                and e.EMCLAS                   =             ec.ECCLAS
                                and ec.ECBULK                  =             ''N''                                                                                                                                         --Non-bulk items.
                join #tGBActivePC pc
                                on pc.CMP                                          =             e.EMCMP
                                and pc.PC                                            =             e.EMCLOC'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                exec (@sql)

                --Based on the rows that have changed, populate the Wynne SKU list table for bulk items.
                set @sql               =
                'insert into #tWynneSKUList
                (
                                GB_PCID
                                ,CMP
                                ,PC                                                         
                                ,CAT
                                ,CLAS
                                ,ProductID                          
                                ,[Description]    
                                ,UPC                                      
                                ,SKUListType      
                                ,IsBulk
                )
                select distinct
                                pc.ID
                                ,il.ELCMP
                                ,il.ELLOC
                                ,ec.ECCATG
                                ,ec.ECCLAS
                                ,right(''0000'' + trim(convert(varchar(4), ec.ECCLAS)), 4)                                                                                                                                                                                                                                                                                 as ProductID
                                ,ec.ECDESC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         as [Description]
                                ,right(''000'' + trim(convert(varchar(3), ec.ECCATG)), 3)                                                                                                                                                                                                                                                                                  as UPC
                                ,right(''000'' + trim(convert(varchar(4), ec.ECCATG)), 3) + ''-'' + right(''0000'' + trim(convert(varchar(4), ec.ECCLAS)), 4)  as SKUListType
                                ,1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            as IsBulk
                from #tEQPCCLTRG_Change chg
                join [sourceDB].[sourceSchema].EQPCCLTRG il with (updlock, paglock)
                                on chg.ELCMP                   =             il.ELCMP
                                and chg.ELLOC                   =             il.ELLOC
                                and chg.ELCATG                               =             il.ELCATG
                                and chg.ELCLAS                 =             il.ELCLAS
                join [sourceDB].[sourceSchema].EQPCCMFL ec with (updlock, paglock)
                                on il.ELCMP                                        =             ec.ECCMP
                                and il.ELCATG                    =             ec.ECCATG
                                and il.ELCLAS                      =             ec.ECCLAS
                                and ec.ECBULK                  =             ''Y''                                                                                                                                          --Bulk items only.
                join #tGBActivePC pc
                                on pc.CMP                                          =             il.ELCMP
                                and pc.PC                                            =             il.ELLOC'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                exec (@sql)
                ------------------------- Get the SKUs (CAT/CLASS) to send to GB -------------------------

                


                ------------------------- Get the bulk items that have been added in Wynne -------------------------
                -- NOTE:               Changes to send are determined by joining to EQPCCLTRG in Wynne.
                --                                             This table is used to store items that are marked as RENTABLE.
                --                                             Therefore, Wynne has to flip NEW items to a ON-RENT status before we will
                --                                             see the item and ship it to GB.  We only want to sync items that are ON-RENT.
                --                                             Since GB only contains RENTABLE items, this is the behavior that we want!
                --                                             This applies to serialized items (equipment) as well.  DO NOT change this behavior
                --                                             without consulting Steve Guerra or Dustin Thompson!!!
                --
                --Bulk items are added to GB via a transfer process in Wynne.
                --The transfer record is recorded in RATDETTRG.
                set @sql = '
                insert into #tBulkItemToSend
                (
                                RJCMP  
                                ,RJTLOC
                                ,RJCATG
                                ,RJCLAS
                                ,RJQTYR
                                ,RJSTTS
                )
                select
                                t.ELCMP               
                                ,t.ELLOC
                                ,t.ELCATG
                                ,t.ELCLAS
                                ,sum(t.QtyToSend)
                                ,''RC''
                --from [sourceDB].[sourceSchema].RATDETTRG t
                from #tEQPCCLTRG_Change t
                join #tGBActivePC pc
                                on pc.PC              =             t.ELLOC
                group by
                                t.ELCMP               
                                ,t.ELLOC
                                ,t.ELCATG
                                ,t.ELCLAS
                having sum(t.QtyToSend) > 0'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[startDateTime]', @startTime)
                set @sql = replace(@sql, '[endDateTime]', @endTime)
                exec (@sql)
                -------------------------* Get the bulk items that have been added in Wynne *-------------------------

                


                --Insert SKU for queued equipment.
                set @sql               =
                'insert into #tWynneSKUList
                (
                                GB_PCID
                                ,CMP
                                ,PC                                                         
                                ,CAT
                                ,CLAS
                                ,ProductID                          
                                ,[Description]    
                                ,UPC                                      
                                ,SKUListType      
                                ,IsConsumable
                )
                select distinct
                                pc.ID
                                ,chg.EMCMP
                                ,chg.EMCLOC
                                ,chg.CAT
                                ,chg.CLAS
                                ,right(''0000'' + trim(convert(varchar(4), chg.CLAS)), 4)                                                                                                                                                                                                                                                                    as ProductID
                                ,ec.ECDESC                                                                                                                                                                                                                                                                                                                                                                                                                                                                         as [Description]
                                ,right(''000'' + trim(convert(varchar(3), chg.CAT)), 3)                                                                                                                                                                                                                                                                                        as UPC
                                ,right(''000'' + trim(convert(varchar(4), chg.CAT)), 3) + ''-'' + right(''0000'' + trim(convert(varchar(4), chg.CLAS)), 4)    as SKUListType
                                ,0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            as IsConsumable
                from integr.EQPMASFL_Queue chg
                --join [sourceDB].[sourceSchema].EQPMASFL e
                --             on chg.EMCMP                 =             e.EMCMP
                --             and chg.[EMEQP#]          =             e.[EMEQP#]
                --             and e.EMDLCD                  =             ''A''                                                                                                                                         --Active equipment only.
                join [sourceDB].[sourceSchema].EQPCCMFL ec with (updlock, paglock)
                                on chg.EMCMP                 =             ec.ECCMP
                                and chg.CAT                                       =             ec.ECCATG
                                and chg.CLAS                     =             ec.ECCLAS
                                and ec.ECBULK                  =             ''N''                                                                                                                                         --Non-bulk items.
                join #tGBActivePC pc
                                on pc.CMP                                          =             chg.EMCMP
                                and pc.PC                                            =             chg.EMCLOC
                left join #tWynneSKUList wsk
                                on wsk.CMP                                       =             chg.EMCMP
                                and wsk.PC                                         =             chg.EMCLOC
                                and wsk.CAT                                      =             chg.CAT
                                and wsk.CLAS                    =             chg.CLAS
                where
                                wsk.CMP                                                             is null
                                and wsk.PC                                         is null
                                and wsk.CAT                                      is null
                                and wsk.CLAS                    is null'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                exec (@sql)

                --For queued bulk items, add the SKU info back into our #tWynneSKUList table if it exists in Mimix
                --for them.
                set @sql               =
                'insert into #tWynneSKUList
                (
                                GB_PCID
                                ,CMP
                                ,PC                                                         
                                ,CAT
                                ,CLAS
                                ,ProductID                          
                                ,[Description]    
                                ,UPC                                      
                                ,SKUListType      
                                ,IsBulk
                )
                select distinct
                                pc.ID
                                ,chg.RJCMP
                                ,chg.RJTLOC
                                ,ec.ECCATG
                                ,ec.ECCLAS
                                ,right(''0000'' + trim(convert(varchar(4), ec.ECCLAS)), 4)                                                                                                                                                                                                                                                                                 as ProductID
                                ,ec.ECDESC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         as [Description]
                                ,right(''000'' + trim(convert(varchar(3), ec.ECCATG)), 3)                                                                                                                                                                                                                                                                                  as UPC
                                ,right(''000'' + trim(convert(varchar(4), ec.ECCATG)), 3) + ''-'' + right(''0000'' + trim(convert(varchar(4), ec.ECCLAS)), 4)  as SKUListType
                                ,1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            as IsBulk
                from integr.BulkItemToSend_Queue chg
                join [sourceDB].[sourceSchema].EQPCCMFL ec with (updlock, paglock)
                                on chg.RJCMP                   =             ec.ECCMP
                                and chg.RJCATG                =             ec.ECCATG
                                and chg.RJCLAS                 =             ec.ECCLAS
                                and ec.ECBULK                  =             ''Y''                                                                                                                                          --Bulk items only.
                join #tGBActivePC pc
                                on pc.CMP                                          =             chg.RJCMP
                                and pc.PC                                            =             chg.RJTLOC
                left join #tWynneSKUList wsk
                                on wsk.CMP                                       =             chg.RJCMP
                                and wsk.CAT                                      =             chg.RJCATG
                                and wsk.CLAS                    =             chg.RJCLAS
                where
                                chg.RJQTYR > 0
                                and wsk.CMP                    is null
                                and wsk.CAT                      is null
                                and wsk.CLAS    is null'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                exec (@sql)




                ------------------------- Add new SKUs to remote -------------------------
                --First, determine which SKUs do not exist in GB.
                --This will be used to log which SKUs were added and when.
                update t
                set
                                t.GBAction          =             @GB_ACTION_ADD
                from #tWynneSKUList t
                join
                (
                                select
                                                t2.CMP
                                                ,t2.PC
                                                ,t2.UPC
                                                ,t2.ProductID
                                from #tWynneSKUList t2
                                join #tGBActivePC pc
                                                on pc.PC = t2.PC
                                left join #tGBSKUList remoteSKUList
                                                on remoteSKUList.UPC = t2.UPC
                                                and remoteSKUList.ProductID = t2.ProductID
                                                and remoteSKUList.HQLocationID = pc.ID
                                where
                                                remoteSKUList.ID is null
                ) a
                on a.CMP                                            =             t.CMP
                                and a.PC                              =             t.PC
                                and a.UPC                           =             t.UPC
                                and a.ProductID               =             t.ProductID

                --Serialize as JSON.
                set @json = (
                select
                                convert(varchar(23), getdate(), 121)        as DateAdded
                                ,convert(varchar(23), getdate(), 121)       as LastUpdated
                                ,convert(varchar(23), getdate(), 121)       as LastNotification
                                ,pc.ID                                                                                                                                    as HQLocationID
                                ,sk.UPC                                                                                                                                as UPC
                                ,sk.ProductID
                                ,sk.SKUListType                                                                                                as SKUListType
                                ,sk.[Description]
                                ,sk.IsConsumable
                                ,1                                                                                                                                                            as IsActive
                                ,1                                                                                                                                                            as QuantityOnHand
                from #tWynneSKUList sk
                join #tGBActivePC pc
                                on pc.PC                                                                                                                              =             sk.PC

                --REF1
                --where
                --             sk.GBAction                                                                                                                       =             @GB_ACTION_ADD
                --REF1
                for json auto)

                --Compress and send to GB.
                if (@json is not null)
                begin
                                set @compData = compress(@json)
                                exec FMQAGBSQL19.Integration.gbw.upsertSKUList
                                                @sessionID = @sessionID
                                                ,@comp = @compData
                end

                ----debug
                --select @startTime, @endTime
                --select * from #tEQPMASFL_Change
                --select * from #tWynneSKUList
                --select @json
                ----rollback tran trnGBSync
                --return
                ----debug

                --Reload the GB SKU list.  SKUs are assigned using an identifier.
                --Since we added the SKUs we need the IDs that were created.
                truncate table #tGBSKUList

                --Get GB SKU list.
                exec FMQAGBSQL19.Integration.gbw.getSKUList @sessionID = @sessionID
                set @compData = (select t.CompData from FMQAGBSQL19.Integration.gbw.CompData t where t.SessionID = @sessionID and t.Op = 'gbw.getSKUList')
                set @json = cast(decompress(@compData) as nvarchar(max))

                if (@json is not null)
                begin
                                insert into #tGBSKUList
                                (
                                                ID
                                                ,DateAdded
                                                ,LastUpdated
                                                ,HQLocationID
                                                ,UPC
                                                ,ProductID
                                )
                                select
                                                a.ID
                                                ,a.DateAdded
                                                ,a.LastUpdated
                                                ,a.HQLocationID
                                                ,a.UPC
                                                ,a.ProductID
                                from openjson(@json)
                                with
                                (
                                                ID                                                                            bigint                                     '$.ID'
                                                ,DateAdded                                       datetime                             '$.DateAdded'
                                                ,LastUpdated                     datetime                             '$.LastUpdated'
                                                ,HQLocationID                   bigint                                     '$.HQLocationID'
                                                ,UPC                                                      nvarchar(50)      '$.UPC'
                                                ,ProductID                                          nvarchar(25)      '$.ProductID'
                                ) a
                end
                else
                begin
                                raiserror('Failed to retrieve JSON for SKU list refresh.', 18, 1)
                end
                -------------------------* Add new SKUs to remote *-------------------------


                --Get the GB HQLocationID (PC) and HQSKUID (CAT/CLASS) while we are at it.
                --This needs to be done because if the HQSKUID in #tWynneSKUList is null or zero,
                --it will be queued because the SKU is missing.  This creates a false "positive"
                --that the CAT/CLASS from Mimix has not arrived yet.
                update wsk
                set
                                wsk.HQLocationID           =             gsk.HQLocationID
                                ,wsk.HQSKUID                  =             gsk.ID
                from #tWynneSKUList wsk
                join #tGBActivePC pc
                                on pc.CMP                                                          =             wsk.CMP
                                and pc.PC                                                            =             wsk.PC
                join #tGBSKUList gsk
                                on gsk.UPC                                                         =             wsk.UPC
                                and gsk.ProductID                           =             wsk.ProductID
                                and gsk.HQLocationID    =             pc.ID


                --debug
                --select * from #tWynneSKUList
                --select * from #tBulkItemToSend
                --return
                --debug


                ------------------------- Queue any bulk items if the CAT/CLASS does not yet exist in our #tWynneSKUList table -------------------------
                --This first insert handles the case where the CAT/CLAS is still missing and the CMP/LOC/CAT/CLAS already exist in the queue
                -- but the total amount in the temp table does is greater than the total amount in the queue table.  This indicates that an addtional
                --amount was added by another transfer.  In this case, we want to store that additional amount as a separate row in the queue table.
                --REF2
                insert into integr.BulkItemToSend_Queue
                (
                                RJCMP
                                ,RJTLOC
                                ,RJCATG
                                ,RJCLAS
                                ,RJQTYR
                                ,RJSTTS
                                --,ELEVENT
                                --,RRN_FIELD_DATA
                                ,QueueReasonCode
                                ,DetectionDate
                                ,SyncSessionID
                )
                output
                                inserted.RJCMP
                                ,inserted.RJTLOC
                                ,inserted.RJCATG
                                ,inserted.RJCLAS
                                ,inserted.RJQTYR
                                ,inserted.RJSTTS
                                ,inserted.QueueReasonCode
                                ,inserted.DetectionDate
                                ,@syncSessionID                                                                                                              as SyncSessionID
                into #tBulkItemToSend_Queue_Output
                --Identify CAT/CLASS items still missing.
                select
                                b.RJCMP
                                ,b.RJTLOC
                                ,b.RJCATG
                                ,b.RJCLAS
                                ,b.RJQTYR                                                                                                            as QtyToSend
                                ,'RC'                                                                                                                       as RJSTTS
                                ,@QUEUE_RC_CATCLASS_MISSING                                                         as QueueReasonCode
                                ,@runDate                                                                                                                          as DetectionDate
                                ,@syncSessionID                                                                                                              as SyncSessionID
                from #tBulkItemToSend b
                join #tGBActivePC pc
                                on pc.CMP                                          =             b.RJCMP
                                and pc.PC                                            =             b.RJTLOC
                left join #tWynneSKUList wsk
                                on wsk.CMP                                       =             b.RJCMP
                                and wsk.PC                                         =             b.RJTLOC
                                and wsk.UPC                                     =             b.RJCATG
                                and wsk.ProductID          =             b.RJCLAS
                                and wsk.IsBulk                  =             1
                where
                                (
                                                wsk.CMP                                                             is null
                                                and wsk.PC                                         is null
                                                and wsk.UPC                                     is null
                                                and wsk.ProductID          is null
                                )
                                or
                                (
                                                wsk.HQSKUID is null or wsk.HQSKUID = 0
                                )
                --join
                --(
                --             select
                --                             a.RJCMP
                --                             ,a.RJTLOC
                --                             ,a.RJCATG
                --                             ,a.RJCLAS
                --                             ,a.QtyToSend - q2.RJQTYR                                                            as QtyToSend
                --                             ,a.RJSTTS
                --                             --,a.ELEVENT
                --                             --,a.RRN_FIELD_DATA

                --                             --prod
                --                             ,@QUEUE_RC_CATCLASS_MISSING                                                         as QueueReasonCode
                --                             ,@runDate                                                                                                                          as DetectionDate
                --                             ,@syncSessionID                                                                                                              as SyncSessionID
                --                             --prod
                --             from
                --             (
                --                             --Get total qty of items still missing.
                --                             select
                --                                             missing.RJCMP
                --                                             ,missing.RJTLOC
                --                                             ,missing.RJCATG
                --                                             ,missing.RJCLAS
                --                                             ,sum(missing.QtyToSend)                                                                            as QtyToSend
                --                                             ,missing.RJSTTS
                --                                             --,q.ELEVENT
                --                                             --,q.RRN_FIELD_DATA

                --                                             --prod
                --                                             ,@QUEUE_RC_CATCLASS_MISSING                                                         as QueueReasonCode
                --                                             ,@runDate                                                                                                                          as DetectionDate
                --                                             ,@syncSessionID                                                                                                              as SyncSessionID
                --                                             --prod
                --                             from integr.BulkItemToSend_Queue q
                --                             join
                --                             (
                --                                             --Identify CAT/CLASS items still missing.
                --                                             select
                --                                                             b.RJCMP
                --                                                             ,b.RJTLOC
                --                                                             ,b.RJCATG
                --                                                             ,b.RJCLAS
                --                                                             ,b.RJQTYR                                                                                                            as QtyToSend
                --                                                             ,'RC'                                                                                                                       as RJSTTS
                --                                             from #tBulkItemToSend b
                --                                             join #tGBActivePC pc
                --                                                             on pc.CMP                                          =             b.RJCMP
                --                                                             and pc.PC                                            =             b.RJTLOC
                --                                             left join #tWynneSKUList wsk
                --                                                             on wsk.CMP                                       =             b.RJCMP
                --                                                             and wsk.PC                                         =             b.RJTLOC
                --                                                             and wsk.UPC                                     =             b.RJCATG
                --                                                             and wsk.ProductID          =             b.RJCLAS
                --                                                             and wsk.IsBulk                  =             1
                --                                             where
                --                                                             (
                --                                                                             wsk.CMP                                                             is null
                --                                                                             and wsk.PC                                         is null
                --                                                                             and wsk.UPC                                     is null
                --                                                                             and wsk.ProductID          is null
                --                                                             )
                --                                                             or
                --                                                             (
                --                                                                             wsk.HQSKUID is null or wsk.HQSKUID = 0
                --                                                             )
                --                             ) missing
                --                             on missing.RJCMP                                           =             q.RJCMP
                --                                             and missing.RJTLOC                        =             q.RJTLOC
                --                                             and missing.RJCATG                       =             q.RJCATG
                --                                             and missing.RJCLAS                         =             q.RJCLAS
                --                             group by
                --                                             missing.RJCMP
                --                                             ,missing.RJTLOC
                --                                             ,missing.RJCATG
                --                                             ,missing.RJCLAS
                --                                             ,missing.RJSTTS
                --                                             --,q.ELEVENT
                --                                             --,q.RRN_FIELD_DATA
                --             ) a
                --             join integr.BulkItemToSend_Queue q2
                --                             on q2.RJCMP                                                     =             a.RJCMP
                --                             and q2.RJTLOC                                  =             a.RJTLOC
                --                             and q2.RJCATG                                 =             a.RJCATG
                --                             and q2.RJCLAS                                   =             a.RJCLAS
                --                             --and q2.ELEVENT                                            =             a.ELEVENT
                --                             --and q2.RRN_FIELD_DATA          =             a.RRN_FIELD_DATA
                --             where
                --                             a.QtyToSend > q2.RJQTYR
                --) missingTotal
                --on missingTotal.RJCMP                              =             t.RJCMP
                --             and missingTotal.RJTLOC              =             t.RJTLOC
                --             and missingTotal.RJCATG             =             t.RJCATG
                --             and missingTotal.RJCLAS               =             t.RJCLAS

                ----This insert handles the case where the CMP/LOC/CAT/CLAS does not exist in the queue table.
                ----REF2
                --insert into integr.BulkItemToSend_Queue
                --(
                --             RJCMP
                --             ,RJTLOC
                --             ,RJCATG
                --             ,RJCLAS
                --             ,RJQTYR
                --             ,RJSTTS
                --             --,ELEVENT
                --             --,RRN_FIELD_DATA
                --             ,QueueReasonCode
                --             ,DetectionDate
                --             ,SyncSessionID
                --)
                --output
                --             inserted.RJCMP
                --             ,inserted.RJTLOC
                --             ,inserted.RJCATG
                --             ,inserted.RJCLAS
                --             ,inserted.RJQTYR
                --             ,inserted.RJSTTS
                --             ,inserted.QueueReasonCode
                --             ,inserted.DetectionDate
                --             ,@syncSessionID                                                                                                              as SyncSessionID
                --into #tBulkItemToSend_Queue_Output
                --select distinct
                --             missing.RJCMP
                --             ,missing.RJTLOC
                --             ,missing.RJCATG
                --             ,missing.RJCLAS
                --             ,missing.QtyToSend
                --             ,missing.RJSTTS
                --             --,'U'                                                                                                                                      as ELEVENT
                --             --,0                                                                                                                                                         as RRN_FIELD_DATA
                --             ,@QUEUE_RC_CATCLASS_MISSING
                --             ,@runDate                                                                                                                          as DetectionDate
                --             ,@syncSessionID                                                                                                              as SyncSessionID
                --from integr.BulkItemToSend_Queue q
                --right join
                --(
                --             select
                --                             b.RJCMP
                --                             ,b.RJTLOC
                --                             ,b.RJCATG
                --                             ,b.RJCLAS
                --                             ,b.RJQTYR                                                                                                            as QtyToSend
                --                             ,'RC'                                                                                                                       as RJSTTS
                --             from #tBulkItemToSend b
                --             join #tGBActivePC pc
                --                             on pc.CMP                                          =             b.RJCMP
                --                             and pc.PC                                            =             b.RJTLOC
                --             left join #tWynneSKUList wsk
                --                             on wsk.CMP                                       =             b.RJCMP
                --                             and wsk.PC                                         =             b.RJTLOC
                --                             and wsk.UPC                                     =             b.RJCATG
                --                             and wsk.ProductID          =             b.RJCLAS
                --                             and wsk.IsBulk                  =             1
                --             where
                --                             (
                --                                             wsk.CMP                                                             is null
                --                                             and wsk.PC                                         is null
                --                                             and wsk.UPC                                     is null
                --                                             and wsk.ProductID          is null
                --                             )
                --                             or
                --                             (
                --                                             wsk.HQSKUID is null or wsk.HQSKUID = 0
                --                             )
                --) missing
                --on missing.RJCMP                                        =             q.RJCMP
                --             and missing.RJTLOC                        =             q.RJTLOC
                --             and missing.RJCATG                       =             q.RJCATG
                --             and missing.RJCLAS                         =             q.RJCLAS
                --where
                --             q.RJCMP                                              is null
                --             and q.RJTLOC     is null
                --             and q.RJCATG    is null
                --             and q.RJCLAS     is null

                --Now get the queued bulk items that exist in our #tWynneSKUList table and add them
                --back into the #tBulkItemToSend.
                insert into #tBulkItemToSend
                (
                                RJCMP
                                ,RJTLOC
                                ,RJCATG
                                ,RJCLAS
                                ,RJQTYR
                                ,RJSTTS
                )
                select
                                q.RJCMP
                                ,q.RJTLOC
                                ,q.RJCATG
                                ,q.RJCLAS
                                ,sum(q.RJQTYR)
                                ,q.RJSTTS
                from integr.BulkItemToSend_Queue q
                join #tWynneSKUList wsk
                                on wsk.CMP                       =             q.RJCMP
                                and wsk.PC                         =             q.RJTLOC
                                and wsk.CAT                      =             q.RJCATG
                                and wsk.CLAS    =             q.RJCLAS
                where
                                q.SendDate = '1/1/1900'
                group by
                                q.RJCMP
                                ,q.RJTLOC
                                ,q.RJCATG
                                ,q.RJCLAS
                                ,q.RJSTTS

                --Now remove the queued bulk items from the send list that were queued by this session.
                --REF2
                delete b
                from #tBulkItemToSend b
                join #tBulkItemToSend_Queue_Output o
                                on o.RJCMP                                                        =             b.RJCMP
                                and o.RJCATG                                    =             b.RJCATG
                                and o.RJCLAS                                     =             b.RJCLAS
                                and o.RJTLOC                                     =             b.RJTLOC
                                and o.RJQTYR                                    =             b.RJQTYR
                                and o.SyncSessionID                      =             @syncSessionID

                --Now that the previously queued bulk items have been added
                --to our send list, go back and mark them as sent.
                --REF2
                update q
                set
                                q.SendDate                                        =             getdate()
                                ,q.SyncSessionID              =             @syncSessionID
                from integr.BulkItemToSend_Queue q
                join #tBulkItemToSend b
                                on b.RJCMP                                        =             q.RJCMP
                                and b.RJCATG                    =             q.RJCATG
                                and b.RJCLAS                     =             q.RJCLAS
                                and b.RJTLOC                     =             q.RJTLOC
                where
                                q.SendDate = '1/1/1900'
                -------------------------* Queue any bulk items if the CAT/CLASS does not yet exist in our WynneSKU table *-------------------------


                ----debug
                --select 'crap' as test, * from #tBulkItemToSend
                --return
                ----debug


                ------------------------- Add bulk items to remote -------------------------
                --Get the current maximum TagID number assigned in GB for the bulk item cat/class.
                --Each bulk item will be sent over as individual records and each record must have a
                --unique TagID number.
                --
                --Example:
                --             Wynne stores the bulk item like so:
                --                             ITEM#                   LOC                        QTY
                --                             -----                        ---                           ---
                --                             AH34                     0999       2
                --
                --We need to send it has two rows like so:
                --                             ITEM#                   LOC                        QTY                        TAG#                                     Notes
                --                             -----                        ---                           ---                           ----                                         -----
                --                             AH34                     0999       1                              AH34_0999_3                    This item exists in GB prior to our transfer!
                --                             AH34                     0999       1                              AH34_0999_4                    * New items must use the naming convention of ITEM_LOCATION_TagSeq
                --                             AH34                     0999       1                              AH34_0999_5                    * ditto
                --
                --Notice that the next tag must start from the last highest tag.  Hence, the update statement below
                --is used to determine the current maximum value of the tag for the given bulk item for the location (PC).
                --In the example above, the current highest tag would be "3".
                update t
                set
                                t.GBCurrentMax_RFID_TagID     =             a.TagIDNum
                from #tWynneSKUList t
                join
                (
                                select
                                                wsk.GB_PCID
                                                ,wsk.CMP
                                                ,wsk.PC
                                                ,wsk.UPC
                                                ,wsk.ProductID
                                                ,max(isnull(try_cast(reverse(left(reverse(i.AssetID), charindex('_', reverse(i.AssetID)) - 1)) as int), 0)) as TagIDNum
                                from #tWynneSKUList wsk
                                join #tGBActivePC pc
                                                on pc.CMP                                          =             wsk.CMP
                                                and pc.PC                                            =             wsk.PC
                                join #tGBSKUList gsk
                                                on gsk.HQLocationID      =             pc.ID
                                                and gsk.UPC                                       =             wsk.UPC
                                                and gsk.ProductID           =             wsk.ProductID
                                join #tGBInventory i
                                                on i.HQLocationID            =             gsk.HQLocationID
                                                and i.HQSKUID                  =             gsk.ID
                                join #tBulkItemToSend b
                                                on b.RJCMP                                        =             pc.CMP
                                                and b.RJTLOC                     =             pc.PC
                                                and b.RJCATG                    =             gsk.UPC
                                                and b.RJCLAS                     =             gsk.ProductID
                                where
                                                patindex('%[0-9][_]%', reverse(i.AssetID)) > 0
                                group by
                                                wsk.GB_PCID
                                                ,wsk.CMP
                                                ,wsk.PC
                                                ,wsk.UPC
                                                ,wsk.ProductID
                ) a
                on a.CMP                                            =             t.CMP
                                and a.GB_PCID =             t.GB_PCID
                                and a.PC                              =             t.PC
                                and a.UPC                           =             t.UPC
                                and a.ProductID               =             t.ProductID

                --Get the GB HQLocationID (PC) and HQSKUID (CAT/CLASS) while we are at it.
                update wsk
                set
                                wsk.HQLocationID           =             gsk.HQLocationID
                                ,wsk.HQSKUID                  =             gsk.ID
                from #tWynneSKUList wsk
                join #tGBActivePC pc
                                on pc.CMP                                                          =             wsk.CMP
                                and pc.PC                                                            =             wsk.PC
                join #tGBSKUList gsk
                                on gsk.UPC                                                         =             wsk.UPC
                                and gsk.ProductID                           =             wsk.ProductID
                                and gsk.HQLocationID    =             pc.ID

                ----debug
                --select * from #tWynneSKUList
                --return
                ----debug

                --Use tally construct to create individual rows based on the RJQTYR column.
                --Bulk items must go into GB as individual records with a QTY of 1.  Wynne stores
                --the bulk item as a single record with a quantity value.
                set @sql               =             N'
                set @json =
                (
                                select distinct
                                                ''[runDate]''                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        as DateAdded
                                                ,wsk.HQLocationID
                                                ,wsk.HQSKUID
                                                ,1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            as            Quantity
                                                ,trim(ecm.[ECEQP#])
                                                                + ''_''
                                                                + right(''0000'' + convert(varchar(4), wsk.PC), 4)
                                                                + ''_''
                                                                + convert(varchar(10), wsk.GBCurrentMax_RFID_TagID + row_number() over (partition by t.RJCMP, t.RJTLOC, t.RJCATG, t.RJCLAS order by tally.Qty))   as TagID
                                                ,trim(ecm.[ECEQP#])
                                                                + ''_''
                                                                + right(''0000'' + convert(varchar(4), wsk.PC), 4)
                                                                + ''_''
                                                                + convert(varchar(10), wsk.GBCurrentMax_RFID_TagID + row_number() over (partition by t.RJCMP, t.RJTLOC, t.RJCATG, t.RJCLAS order by tally.Qty))   as AssetID
                                                ,1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            as            IsActive
                                from #tBulkItemToSend t
                                join
                                (
                                                select Qty = row_number() over (order by (select null))
                                                from sys.all_columns ac
                                ) tally
                                on tally.Qty <= t.RJQTYR
                                join #tWynneSKUList wsk
                                                on wsk.CMP                                       =             t.RJCMP
                                                and wsk.PC                                         =             t.RJTLOC
                                                and wsk.UPC                                     =             t.RJCATG
                                                and wsk.ProductID          =             t.RJCLAS
                                --join #tEQPCCLTRG_Change chg
                                --             on chg.ELCMP                   =             t.RJCMP
                                --             and chg.ELLOC                   =             t.RJTLOC
                                --             and chg.ELCATG                               =             t.RJCATG
                                --             and chg.ELCLAS                 =             t.RJCLAS
                                --join [sourceDB].[sourceSchema].EQPCCLTRG ecc
                                --             on ecc.ELCMP                    =             chg.ELCMP
                                --             and ecc.ELLOC                   =             chg.ELLOC
                                --             and ecc.ELCATG                =             chg.ELCATG
                                --             and ecc.ELCLAS                 =             chg.ELCLAS
                                join [sourceDB].[sourceSchema].EQPCCMFL ecm with (updlock, paglock)
                                                on ecm.ECCMP                 =             t.RJCMP
                                                and ecm.ECCATG                             =             t.RJCATG
                                                and ecm.ECCLAS                              =             t.RJCLAS
                                for json auto
                )'

                set @sql = replace(@sql, '[remoteServer]', @remoteServer)
                set @sql = replace(@sql, '[remoteDB]', @remoteDB)
                set @sql = replace(@sql, '[remoteSchema]', @remoteSchema)
                set @sql = replace(@sql, '[runDate]', convert(varchar(23), @runDate, 121))
                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)

                ----debug
                --print @sql
                --return
                ----debug
                exec sp_executesql @sql, N'@json varchar(max) out', @jsonOut out                                        -- Get the JSON output.
                
                --Compress and send to GB.
                if (@jsonOut is not null)
                begin
                                set @compData = compress(@jsonOut)
                                exec FMQAGBSQL19.Integration.gbw.upsertBulkList
                                                @sessionID = @sessionID
                                                ,@comp = @compData
                end
                -------------------------* Add bulk items to remote *-------------------------




                ------------------------- Update the cache -------------------------
                --Get the hashbyte expression to use for the given table.
                --We are updating our local cache so that the bulk items we just sent
                --do not get sent again unless our cache differs from the source table.
                set @hashExprOut          =             ''
                set @targetDB                   =             'Integration'
                set @targetSchema        =             'integr'
                set @targetTB                   =             'EQPCCLTRG_Cache'
                set @sourceTB                  =             'EQPCCLTRG'

                --This dynamically gets the columns for the given table and determines its data type, length, etc.
                --to build a HASHBYTE expression which is then used by the dynamic statement below to update
                --our cache with the same hashbyte value for the rows we are sending.  See REF1.
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@tableAlias       =             'src'
                                ,@hashExprOut =             @hashExprOut output


                --REF1:  Add the SQL statement, using the hash statement that will update our local cache table.
                --Note that we are updating only those rows that are in the change table.  This first statement
                --handles rows that are already in our cache.
                set @sql               =             '
                update t
                set
                                t.ELQOWN                          =             a.ELQOWN
                                ,t.PrevELQOWN =             a.ELQOWN
                                ,t.[Hash]                              =             a.[Hash]
                                ,t.CacheDate      =             ''[cacheDate]''
                from [targetDB].[targetSchema].[targetTB] t with (updlock, paglock)
                join #tEQPCCLTRG_Change chg (nolock)
                                on chg.ELCATG                                  =             t.ELCATG
                                and chg.ELCLAS                                 =             t.ELCLAS
                                and chg.ELLOC                                   =             t.ELLOC
                                and chg.ELCMP                                 =             t.ELCMP
                                and chg.ELEVENT                                             =             t.ELEVENT
                                and chg.RRN_FIELD_DATA           =             t.RRN_FIELD_DATA
                                and chg.SYNCDTTM                        =             t.SYNCDTTM
                join
                (
                                select
                                                [tableAlias].ELCATG
                                                ,[tableAlias].ELCLAS
                                                ,[tableAlias].ELLOC
                                                ,[tableAlias].ELCMP
                                                ,[tableAlias].ELQOWN
                                                ,[tableAlias].ELEVENT
                                                ,[tableAlias].RRN_FIELD_DATA
                                                ,[tableAlias].SYNCDTTM
                                                ,[hashByteExpr] as [Hash]
                                from [sourceDB].[sourceSchema].[sourceTB] [tableAlias] with (updlock, paglock)
                                join #tEQPCCLTRG_Change chg2
                                                on chg2.ELCMP                                                 =             [tableAlias].ELCMP
                                                and chg2.ELLOC                                                =             [tableAlias].ELLOC
                                                and chg2.ELCATG                                                             =             [tableAlias].ELCATG
                                                and chg2.ELCLAS                                                              =             [tableAlias].ELCLAS
                                                and chg2.ELEVENT                                           =             [tableAlias].ELEVENT
                                                and chg2.RRN_FIELD_DATA                        =             [tableAlias].RRN_FIELD_DATA
                                                and chg2.SYNCDTTM                                      =             [tableAlias].SYNCDTTM
                ) a
                on a.ELCMP                                                                        =             chg.ELCMP
                                and a.ELLOC                                                       =             chg.ELLOC
                                and a.ELCATG                                    =             chg.ELCATG
                                and a.ELCLAS                                     =             chg.ELCLAS
                                and a.ELEVENT                                  =             chg.ELEVENT
                                and a.RRN_FIELD_DATA =             chg.RRN_FIELD_DATA
                                and a.SYNCDTTM                                             =             chg.SYNCDTTM'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[targetTB]', @targetTB)
                set @sql = replace(@sql, '[tableAlias]', 'src')
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[cacheDate]', convert(varchar(23), @runDate, 121))
                exec (@sql)

                --Now insert any rows that are not in our cache.
                set @sql               =             '
                insert into [targetDB].[targetSchema].[targetTB] with (updlock, paglock)
                (
                                RRN_FIELD_DATA
                                ,SYNCDTTM
                                ,ELEVENT
                                ,ELCMP
                                ,ELLOC
                                ,ELCATG
                                ,ELCLAS
                                ,ELQOWN
                                ,PrevELQOWN
                                ,[Hash]
                                ,CacheDate
                )
                select
                                src.RRN_FIELD_DATA
                                ,src.SYNCDTTM
                                ,src.ELEVENT
                                ,src.ELCMP
                                ,src.ELLOC
                                ,src.ELCATG
                                ,src.ELCLAS
                                ,src.ELQOWN
                                ,src.ELQOWN
                                ,[hashByteExpr]
                                ,''[cacheDate]''
                from [[sourceDB]].[[sourceSchema]].EQPCCLTRG src with (updlock, paglock)
                join
                (
                                --rows not in cache.
                                select
                                                cache.RRN_FIELD_DATA
                                                ,cache.SYNCDTTM
                                                ,cache.ELEVENT
                                                ,cache.ELCATG
                                                ,cache.ELCLAS
                                                ,cache.ELLOC
                                                ,cache.ELCMP
                                from [targetDB].[targetSchema].[targetTB] cache with (updlock, paglock)
                                right join #tEQPCCLTRG_Change chg
                                                on chg.ELCATG                                  =             cache.ELCATG
                                                and chg.ELCLAS                                 =             cache.ELCLAS
                                                and chg.ELLOC                                   =             cache.ELLOC
                                                and chg.ELCMP                                 =             cache.ELCMP
                                                and chg.ELEVENT                                             =             cache.ELEVENT
                                                and chg.RRN_FIELD_DATA           =             cache.RRN_FIELD_DATA
                                                and chg.SYNCDTTM                        =             cache.SYNCDTTM
                                where
                                                cache.ELCMP                                                                     is null
                                                and cache.ELLOC                                                              is null
                                                and cache.ELCATG                                           is null
                                                and cache.ELClAS                                             is null
                                                and cache.RRN_FIELD_DATA      is null
                                                and cache.SYNCDTTM                                    is null
                                                and cache.ELEVENT                                         is null
                ) a
                on a.ELCATG                                                                      =             src.ELCATG
                                and a.ELCLAS                                     =             src.ELCLAS
                                and a.ELLOC                                                       =             src.ELLOC
                                and a.ELCMP                                                      =             src.ELCMP
                                and a.ELEVENT                                  =             src.ELEVENT
                                and a.RRN_FIELD_DATA =             src.RRN_FIELD_DATA
                                and a.SYNCDTTM                                             =             src.SYNCDTTM'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[targetTB]', @targetTB)
                set @sql = replace(@sql, '[tableAlias]', 'src')
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[cacheDate]', convert(varchar(23), @runDate, 121))
                exec (@sql)

                --Now update our cache of EQPCCLFL.  Specifically, we need to store the ELQOWN value
                --that is now in EQPCCLFL.
                set @hashExprOut          =             ''
                set @targetDB                   =             'Integration'
                set @targetSchema        =             'integr'
                set @targetTB                   =             'EQPCCLFL_Cache'
                set @sourceTB                  =             'EQPCCLFL'

                --This dynamically gets the columns for the given table and determines its data type, length, etc.
                --to build a HASHBYTE expression which is then used by the dynamic statement below to update
                --our cache with the same hashbyte value for the rows we are sending.  See REF1.
                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@tableAlias       =             'src'
                                ,@hashExprOut =             @hashExprOut output

                set @sql = '
                update t
                set
                                t.ELQOWN          =             src.ELQOWN
                                ,t.[Hash]              =             [hashByteExpr]
                from [targetDB].[targetSchema].[targetTB] t with (updlock, paglock)
                join [sourceDB].[sourceSchema].[sourceTB] src with (updlock, paglock)
                                on src.ELCATG   =             t.ELCATG
                                and src.ELCLAS  =             t.ELCLAS
                                and src.ELLOC    =             t.ELLOC
                                and src.ELCMP  =             t.ELCMP'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[targetTB]', @targetTB)
                set @sql = replace(@sql, '[tableAlias]', 'src')
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[cacheDate]', convert(varchar(23), @runDate, 121))
                exec (@sql)

                --Insert rows not in our cache.
                set @sql = '
                insert into [targetDB].[targetSchema].[targetTB]
                (
                                ELCMP
                                ,ELLOC
                                ,ELCATG
                                ,ELCLAS
                                ,ELQOWN
                                ,[Hash]
                                ,CacheDate
                )
                select
                                src.ELCMP
                                ,src.ELLOC
                                ,src.ELCATG
                                ,src.ELCLAS
                                ,src.ELQOWN
                                ,[hashByteExpr]                as [Hash]
                                ,''[cacheDate]''  as CacheDate
                from [targetDB].[targetSchema].[targetTB] t with (updlock, paglock)
                right join [sourceDB].[sourceSchema].[sourceTB] src with (updlock, paglock)
                                on src.ELCATG   =             t.ELCATG
                                and src.ELCLAS  =             t.ELCLAS
                                and src.ELLOC    =             t.ELLOC
                                and src.ELCMP  =             t.ELCMP
                where
                                t.ELCATG                             is null
                                and t.ELCLAS      is null
                                and t.ELLOC                        is null
                                and t.ELCMP                      is null'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[targetTB]', @targetTB)
                set @sql = replace(@sql, '[tableAlias]', 'src')
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[cacheDate]', convert(varchar(23), @runDate, 121))
                exec (@sql)
                -------------------------* Update the cache *-------------------------
                

                
                ------------------------- Are there equipment changes that need to be sent to GB? -------------------------
                --First, we need to ensure that there is a matching CAT/CLASS record in the Mimix table.
                --If not found, we need to remove the change record so that it does not get sent to GB.
                --Rather, place it in the queue to be sent on the next iteration.  Note the right outer join
                --in the derived "missing" table.  This is to ensure that items that are already in the queue
                --do not get added more than once.  Remember that we added the queued items back into the change
                --table so that they could be reprocessed.  Therefore, if the CAT/CLAS is still missing after a
                --reprocess attempt, we do not add it to the queue again!
                --REF2
                insert into integr.EQPMASFL_Queue
                (
                                EMCMP
                                ,[EMEQP#]
                                ,EMCLOC
                                ,CAT
                                ,CLAS
                                ,QueueReasonCode
                                ,DetectionDate
                                ,SyncSessionID
                )
                output
                                inserted.EMCMP
                                ,inserted.[EMEQP#]
                                ,inserted.EMCLOC
                                ,inserted.CAT
                                ,inserted.CLAS
                                ,inserted.QueueReasonCode
                                ,inserted.DetectionDate
                                ,inserted.SyncSessionID
                into #tEQPMASFL_Queue_Output
                select
                                missing.EMCMP
                                ,missing.[EMEQP#]
                                ,missing.EMCLOC
                                ,missing.CAT
                                ,missing.CLAS
                                ,@QUEUE_RC_CATCLASS_MISSING
                                ,@runDate
                                ,@syncSessionID
                from integr.EQPMASFL_Queue q
                right join
                (
                                select
                                                em.EMCMP
                                                ,em.[EMEQP#]
                                                ,em.EMCLOC
                                                ,right('000' + convert(varchar(3), em.EMCATG), 3)                             as CAT
                                                ,right('0000' + convert(varchar(4), em.EMCLAS), 4)                           as CLAS
                                                --,wsk.*
                                                ,@QUEUE_RC_CATCLASS_MISSING                                                                                                                         as QueueReasonCode
                                                ,@runDate                                                                                                                                                                                          as DetectionDate
                                                ,@syncSessionID                                                                                                                                                                              as SyncSessionID
                                from #tEQPMASFL_Change ec
                                join [WSDATA].[WSDATA].EQPMASFL em with (updlock, paglock)
                                                on em.EMCMP                                                                                                                                                                 =             ec.EMCMP
                                                and em.[EMEQP#]                                                                                                                                                           =             ec.[EMEQP#]
                                join #tGBActivePC pc
                                                on pc.CMP                                                                                                                                                                          =             em.EMCMP
                                                and pc.PC                                                                                                                                                                            =             em.EMCLOC
                                left join #tWynneSKUList wsk
                                                on wsk.CMP                                                                                                                                                                       =             em.EMCMP
                                                and wsk.PC                                                                                                                                                                         =             em.EMCLOC
                                                and wsk.UPC                                                                                                                                                                     =             em.EMCATG
                                                and wsk.ProductID                                                                                                                                          =                em.EMCLAS
                                                and wsk.IsBulk                                                                                                                                                  =                0
                                where
                                                (
                                                                wsk.CMP                                                             is null
                                                                and wsk.PC                                         is null
                                                                and wsk.UPC                                     is null
                                                                and wsk.ProductID          is null
                                                )
                                                or
                                                (
                                                                wsk.HQSKUID is null or wsk.HQSKUID = 0
                                                )
                ) missing
                on missing.[EMEQP#]                    =             q.[EMEQP#]
                                and missing.EMCMP       =             q.EMCMP
                where
                                q.[EMEQP#]                       is null
                                and q.EMCMP                   is null

                --Remove any rows in our change table that have been queued by this session.
                --REF2
                delete ec
                from #tEQPMASFL_Change ec
                join integr.EQPMASFL_Queue qt
                                on qt.[EMEQP#]                                =             ec.[EMEQP#]
                                and qt.EMCMP                                 =             ec.EMCMP
                                and qt.SyncSessionID     =             @syncSessionID

                --Update rows in our queue if they are in our change table where the CAT/CLAS is now in our send list.
                --The equipment can now be sent because the CAT/CLAS will no longer be missing.
                --REF2
                update q
                set
                                q.SendDate                                                        =             getdate()
                                ,q.SyncSessionID                              =             @syncSessionID
                from integr.EQPMASFL_Queue q
                join #tEQPMASFL_Change ec
                                on ec.[EMEQP#]                                               =             q.[EMEQP#]
                                and ec.EMCMP                                 =             q.EMCMP
                join #tWynneSKUList sku
                                on sku.CMP                                                        =             q.EMCMP
                                and sku.CAT                                                       =             q.CAT
                                and sku.CLAS                                     =             q.CLAS
                                and sku.HQSKUID                                            >             0
                where
                                q.SendDate                                                        =             '1/1/1900'

                --Now remove any rows that are still queued.
                --They are identified as rows that still have a send date of '1/1/1900'
                delete ec
                from #tEQPMASFL_Change ec
                join integr.EQPMASFL_Queue qt
                                on qt.[EMEQP#]                                =             ec.[EMEQP#]
                                and qt.EMCMP                                 =             ec.EMCMP
                                and qt.SendDate                                              =             '1/1/1900'

                --We are still sending the CAT/CLASS info to GB even though the equipment has been queued.
                --We just need to update the HQLocationID.
                update sku
                set
                                sku.HQLocationID                            =             pc.ID
                from #tWynneSKUList sku
                join #tGBActivePC pc
                                on pc.PC                                              =             sku.PC
                                and pc.CMP                                        =             sku.CMP
                where
                                sku.HQLocationID                            =             0

                --Resend the SKU list.
                --This ensures that although we queued the equipment because the CAT/CLAS record has not yet arrived,
                --the SKU record will still be created in GB.  GB will only create the SKU record if it does not already exist!
                set @json = (
                                select
                                                convert(varchar(23), getdate(), 121)        as DateAdded
                                                ,convert(varchar(23), getdate(), 121)       as LastUpdated
                                                ,convert(varchar(23), getdate(), 121)       as LastNotification
                                                ,pc.ID                                                                                                                                    as HQLocationID
                                                ,sk.UPC                                                                                                                                as UPC
                                                ,sk.ProductID
                                                ,sk.SKUListType                                                                                                as SKUListType
                                                ,sk.[Description]
                                                ,sk.IsConsumable
                                                ,1                                                                                                                                                            as IsActive
                                                ,1                                                                                                                                                            as QuantityOnHand
                                from #tWynneSKUList sk
                                join #tGBActivePC pc
                                                on pc.PC                                                                                                                              =             sk.PC
                                for json auto
                )

                --Compress and send to GB.
                if (@json is not null)
                begin
                                set @compData = compress(@json)
                                exec FMQAGBSQL19.Integration.gbw.upsertSKUList
                                                @sessionID = @sessionID
                                                ,@comp = @compData
                end
                
                ----debug
                --select 'sku' as opType, @json
                ----debug

                --Now generate the JSON to send to GB.
                set @json =
                (
                                select
                                                @runDate                                                           as            DateAdded
                                                ,@runDate                                                          as            LastUpdated
                                                ,@runDate                                                          as            LastNotification
                                                ,pc.ID                                                                    as            HQLocationID
                                                ,wsk.HQSKUID
                                                ,trim(em.[EMEQP#])                       as            TagID
                                                ,trim(em.[EMEQP#])                       as            AssetID
                                                ,trim(em.[EMSER#])                        as            SerialNumber
                                                ,trim(prev.[EMSER#])     as            PrevSerial
                                from #tEQPMASFL_Change ec
                                join [WSDATA].[WSDATA].EQPMASFL em with (updlock, paglock)
                                                on em.EMCMP                                                                                                                                                                 =             ec.EMCMP
                                                and em.[EMEQP#]                                                                                                                                                           =             ec.[EMEQP#]
                                join #tGBActivePC pc
                                                on pc.CMP                                                                                                                                                                          =             em.EMCMP
                                                and pc.PC                                                                                                                                                                            =             em.EMCLOC
                                --REF3   --left join #tWynneSKUList wsk
                                join #tWynneSKUList wsk
                                                on wsk.CMP                                                                                                                                                                       =             em.EMCMP
                                                and wsk.PC                                                                                                                                                                         =             em.EMCLOC
                                                and wsk.UPC                                                                                                                                                                     =             em.EMCATG
                                                and wsk.ProductID                                                                                                                                          =                em.EMCLAS
                                                and wsk.IsBulk                                                                                                                                                  =                0
                                left join integr.EQPMASFL_Cache_PrevAttribute prev
                                                on prev.[EMEQP#]                                                                                                                                          =                em.[EMEQP#]
                                                and prev.EMCMP                                                                                                                                                            =             em.EMCMP
                                left join #tGBInventory i
                                                on i.AssetID collate SQL_Latin1_General_CP1_CS_AS       =             ec.[EMEQP#]
                                                and i.HQLocationID                                                                                                                                         =                pc.ID
                                for json auto
                )

                ----debug
                --select @json
                ----debug

                ----debug
                --return
                
                --select * from #tEQPMASFL_Cache
                --select * from #tEQPMASFL_Change
                --select * from #tWynneSKUList
                --select * from #tEQPMASFL_Queue_Output
                --select * from config.Config
                
                ----debug

                --Compress and send to GB.
                if (@json is not null)
                begin
                                set @compData = compress(@json)
                                exec FMQAGBSQL19.Integration.gbw.upsertEquipmentList
                                                @sessionID = @sessionID
                                                ,@comp = @compData
                end
                -------------------------* Are there equipment changes that need to be sent to GB? *-------------------------




                ------------------------- Update our equipment cache -------------------------
                --This handles equipment that already existed.
                set @hashExprOut          =             ''
                set @targetDB                   =             'Integration'
                set @targetSchema        =             'integr'
                set @targetTB                   =             'EQPMASFL_Cache'
                set @sourceTB                  =             'EQPMASFL'

                exec integr.GetHashExpression
                                @db                                                      =             @sourceDB
                                ,@schema                           =             @sourceSchema
                                ,@tableName                    =             @sourceTB
                                ,@tableAlias       =             'src'
                                ,@hashExprOut =             @hashExprOut output

                --Add the SQL statement, using the hash statement that will update our local cache table.
                --Note that we are updating only those rows that are in the change table.
                set @sql               =             '
                update t
                set
                                t.[Hash]                =             a.[Hash]
                                ,t.CacheDate      =             ''[cacheDate]''
                from [targetDB].[targetSchema].[targetTB] t with (updlock, paglock)
                join #tEQPMASFL_Change chg (nolock)
                                on chg.[EMEQP#]            =             t.[EMEQP#]
                                and chg.EMCMP               =             t.EMCMP
                join
                (
                                select
                                                [tableAlias].EMCMP
                                                ,[tableAlias].[EMEQP#]
                                                ,[hashByteExpr] as [Hash]
                                from [sourceDB].[sourceSchema].[sourceTB] [tableAlias] with (updlock, paglock)
                                join #tEQPMASFL_Change chg2
                                                on chg2.EMCMP                               =             [tableAlias].EMCMP
                                                and chg2.[EMEQP#]        =             [tableAlias].[EMEQP#]
                ) a
                on a.EMCMP                                      =             chg.EMCMP
                                and a.[EMEQP#]               =             chg.[EMEQP#]'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[targetTB]', @targetTB)
                set @sql = replace(@sql, '[tableAlias]', 'src')
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[cacheDate]', convert(varchar(23), @runDate, 121))
                exec (@sql)
                -------------------------* Update our equipment cache (existing equipment) *-------------------------




                ------------------------- Insert into equipment cache (equipment not in the cache) -------------------------
                set @sql               =             '
                insert into [targetSchema].[targetTB]
                (
                                EMCMP
                                ,[EMEQP#]
                                ,[Hash]
                                ,CacheDate
                )
                select
                                src.EMCMP
                                ,src.[EMEQP#]
                                ,[hashByteExpr]
                                ,''[cacheDate]''
                from [targetDB].[targetSchema].[targetTB] t with (updlock, paglock)
                join
                (
                                select
                                                cache.EMCMP
                                                ,cache.[EMEQP#]
                                from [targetDB].[targetSchema].[targetTB] cache with (updlock, paglock)
                                right join #tEQPMASFL_Change chg
                                                on chg.[EMEQP#]            =             cache.[EMEQP#]
                                                and chg.EMCMP                               =             cache.EMCMP
                                where
                                                cache.EMCMP                                   is null
                                                and cache.[EMEQP#]     is null
                ) a                           --rows not in cache.
                on a.EMCMP                                                      =             t.EMCMP
                                and a.[EMEQP#]                               =             t.[EMEQP#]
                join [[sourceDB]].[[sourceSchema]].EQPMASFL src with (updlock, paglock)
                                on src.EMCMP                  =             a.EMCMP
                                and src.[EMEQP#]           =             a.[EMEQP#]'

                set @sql = replace(@sql, '[sourceDB]', @sourceDB)
                set @sql = replace(@sql, '[sourceTB]', @sourceTB)
                set @sql = replace(@sql, '[sourceSchema]', @sourceSchema)
                set @sql = replace(@sql, '[targetDB]', @targetDB)
                set @sql = replace(@sql, '[targetSchema]', @targetSchema)
                set @sql = replace(@sql, '[targetTB]', @targetTB)
                set @sql = replace(@sql, '[tableAlias]', 'src')
                set @sql = replace(@sql, '[hashByteExpr]', @hashExprOut)
                set @sql = replace(@sql, '[cacheDate]', convert(varchar(23), @runDate, 121))
                exec (@sql)
                -------------------------* Insert into equipment cache (equipment not in the cache) *-------------------------




                

                ----debug
                --rollback transaction trnGBSync
                --return
                ----debug




                ------------------------- Update control table to reflect run-time info -------------------------
                update c
                set
                                c.KeyValue         =             convert(varchar(23), @currentStartTime, 121)
                from [Configuration].config.Config c
                where
                                c.Environment                  =             @env
                                and c.Process                    =             @processName
                                and c.KeyName                =             'PrevStartDateTimeUTC'

                update c
                set
                                c.KeyValue         =             convert(varchar(23), @currentStartTime, 121)
                from [Configuration].config.Config c
                where
                                c.Environment                  =             @env
                                and c.Process                    =             @processName
                                and c.KeyName                =             'StartDateTimeUTC'

                update c
                set
                                c.KeyValue         =             convert(varchar(23), @endTime, 121)
                from [Configuration].config.Config c
                where
                                c.Environment                  =             @env
                                and c.Process                    =             @processName
                                and c.KeyName                =             'EndDateTimeUTC'

                -------------------------* Update control table to reflect run-time info *-------------------------

                --prod
                if (@tranStarted = 1)
                begin
                                commit tran trnGBSync
                end
                --prod

                set @tranStarted             =             0
end try
begin catch
                set @e                  =             error_number()
                set @eLine          =             error_line()
                set @eMsg         =             error_message()
                set @eFmtMsg =             'Error Code: %d Line: %d  Proc: %s  %s'
                
                --prod
                if (@tranStarted = 1)
                begin
                                rollback tran trnGBSync
                end
                --prod

                set @tranStarted             =             0
                raiserror(@eFmtMsg, 18, 1, @e, @eLine, @proc, @eMsg) with log
end catch
GO

