-- Create the database
CREATE DATABASE IF NOT EXISTS pipelineLogs;
USE pipelineLogs;


-- Create the pipelineRuns table
CREATE TABLE IF NOT EXISTS pipelineRuns (
    `Pipeline name` VARCHAR(100),
    `Run start` DATETIME,
    `Run end` DATETIME,
    `Duration` VARCHAR(20),
    `Triggered by` VARCHAR(50),
    `Status` VARCHAR(20),
    `Error` TEXT,
    `Run` VARCHAR(50),
    `Parameters` TEXT,
    `Annotations` TEXT,
    `Run ID` VARCHAR(100) PRIMARY KEY
);

-- Insert sample rows
INSERT INTO pipelineRuns VALUES
-- Row 1: Pipeline 1 - Failed
('pipeline1', '2025-06-10 01:01:00', '2025-06-10 01:01:52', '52s', 'Manual trigger', 'Failed',
 'Source dataset not found in DataLake', 'Original', '{"env":"dev"}', '[]', 'runid-001'),

-- Row 2: Pipeline 2 - Succeeded
('pipeline2', '2025-06-10 01:01:30', '2025-06-10 01:01:48', '18s', 'Schedule trigger', 'Succeeded',
 NULL, 'Retry', '{"env":"prod"}', '[]', 'runid-002'),

-- Row 3: Pipeline 3 - Failed
('pipeline3', '2025-06-10 01:02:00', '2025-06-10 01:02:39', '39s', 'Webhook trigger', 'Failed',
 'Timeout occurred during database write operation', 'Original', '{"env":"staging"}', '[]', 'runid-003'),

-- Row 4: Pipeline 2 - Failed
('pipeline2', '2025-06-10 01:02:30', '2025-06-10 01:03:04', '34s', 'Manual trigger', 'Failed',
 'Permission denied on Blob Storage access', 'Retry', '{"env":"prod"}', '[]', 'runid-004'),

-- Row 5: Pipeline 1 - Succeeded
('pipeline1', '2025-06-10 01:03:00', '2025-06-10 01:03:26', '26s', 'Schedule trigger', 'Succeeded',
 NULL, 'Original', '{"env":"dev"}', '[]', 'runid-005'),

-- Row 6: Pipeline 4 - Failed
('pipeline4', '2025-06-10 01:03:30', '2025-06-10 01:04:12', '42s', 'Webhook trigger', 'Failed',
 'JSON parsing error in pipeline script', 'Original', '{"env":"qa"}', '[]', 'runid-006');
 
 
 Select * from pipelineRuns;
 Select * from pipelineFailureTracker;
 
 
