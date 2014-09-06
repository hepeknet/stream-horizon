package com.threeglav.sh.bauk;

public interface BaukConstants {

	public static final String NEWLINE_STRING = System.getProperty("line.separator");

	public static final int LOW_CARDINALITY_DIMENSION_PRE_CACHE_KEYS_THRESHOLD = 5000;

	public static final int WAIT_FOR_THREADS_TO_DIE_ON_SHUTDOWN_SECONDS = 10;

	public static final String TIMESTAMP_TO_DATE_FORMAT = "dd/MM/yyyy hh:mm:ss.SSS";

	public static final int ONE_MEGABYTE = 1024 * 1024;

	public static final String IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID = "feedProcessingThreadID";
	public static final String IMPLICIT_ATTRIBUTE_MULTI_INSTANCE_FEED_PROCESSOR_ID = "multiInstanceFeedProcessingThreadID";

	public static final String IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_ID = "bulkProcessingThreadID";
	public static final String IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_PARTITIONED_ID = "bulkProcessingPartitionedThreadID";
	public static final String IMPLICIT_ATTRIBUTE_MULTI_INSTANCE_BULK_PROCESSOR_ID = "multiInstanceBulkProcessingThreadID";

	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH = "feedInputFilePath";
	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME = "feedInputFileName";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP = "feedInputFileReceivedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_DATE_TIME = "feedInputFileReceivedDateTime";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_TIMESTAMP = "feedInputFileProcessingStartedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_FINISHED_TIMESTAMP = "feedInputFileProcessingFinishedTimestamp";

	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_DATE_TIME = "feedInputFileProcessingStartedDateTime";
	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE = "feedInputFileSize";
	public static final String IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH = "feedBulkLoadOutputFilePath";

	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH = "bulkFilePath";
	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME = "bulkFileName";
	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_FOR_PROCESSING_TIMESTAMP = "bulkFileReceivedForProcessingTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_STARTED_PROCESSING_TIMESTAMP = "bulkFileProcessingStartedTimestamp";

	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FINISHED_PROCESSING_TIMESTAMP = "bulkFileProcessingFinishedTimestamp";
	public static final String BULK_COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG = "bulkCompletionProcessingSuccessFailureFlag";
	public static final String BULK_COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION = "bulkCompletionProcessingErrorDescription";

	public static final String IMPLICIT_ATTRIBUTE_BULK_JDBC_FINISHED_PROCESSING_TIMESTAMP = "feedInputFileJdbcInsertFinishedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_BULK_JDBC_STARTED_PROCESSING_TIMESTAMP = "feedInputFileJdbcInsertStartedTimestamp";

	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED = "bulkFileAlreadySubmittedForLoading";
	public static final String ALREADY_SUBMITTED_TRUE_VALUE = "T";
	public static final String ALREADY_SUBMITTED_FALSE_VALUE = "F";

	public static final String COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG = "feedCompletionProcessingSuccessFailureFlag";
	public static final String COMPLETION_ATTRIBUTE_NUMBER_OF_ROWS_IN_FEED = "feedCompletionNumberOfTotalRowsInFeed";
	public static final String COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION = "feedCompletionProcessingErrorDescription";

	public static final String ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_START_TIME = "engineInstanceStartTimestamp";
	public static final String ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_IDENTIFIER = "engineInstanceIdentifier";

	public static final String STATEMENT_PLACEHOLDER_DELIMITER_START = "${";
	public static final String STATEMENT_PLACEHOLDER_DELIMITER_END = "}";

	public static final String NATURAL_KEY_DELIMITER = "|";
	public static final String NON_NATURAL_KEY_DELIMITER = "*";
	public static final String NATURAL_NON_NATURAL_DELIMITER = "#";

	public static final String BULK_FILE_NAMED_PIPE_PLACEHOLDER = "bulkNamedPipePath";

}
