CREATE PROCEDURE LISE.InsertIngestionLogs

    @PipelineName VARCHAR(100),
    @Layer VARCHAR(20),   
    @TargetObject VARCHAR(200),
    @Status VARCHAR(30),    
    @FinishedAtUTC DATETIME2(3),
    @WatermarkBefore VARCHAR(40) = NULL,      
    @WatermarkAfter VARCHAR(40) = NULL,
    @RowsWritten BIGINT = NULL,
    @ErrorMessage VARCHAR(4000) = NULL,
    @RunID VARCHAR(100),        
    @BatchID VARCHAR(50),        
    @TriggerType VARCHAR(50),
    @BytesWritten BIGINT = NULL,
    @FilesWritten INT = NULL,
    @DurationSec INT = NULL,
    @ThroughputMBps DECIMAL(18,2) = NULL

AS 
BEGIN 
    SET NOCOUNT ON;
    INSERT INTO LISE.IngestionLogs(
    IngestionID, PipelineName, Layer, TargetObject, Status, FinishedAtUTC,
    WatermarkBefore, WatermarkAfter, RowsWritten, ErrorMessage, RunID, BatchID, 
    TriggerType, BytesWritten, FilesWritten, DurationSec, ThroughputMBps)
    VALUES ( 
    NEWID(), @PipelineName, @Layer, @TargetObject, @Status, @FinishedAtUTC,
    @WatermarkBefore, @WatermarkAfter, @RowsWritten, @ErrorMessage, @RunID, @BatchID,
    @TriggerType, @BytesWritten, @FilesWritten, @DurationSec, @ThroughputMBps)
    ;

END;