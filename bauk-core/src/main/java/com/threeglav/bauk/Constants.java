package com.threeglav.bauk;

public interface Constants {

	public static final String APP_HOME_SYS_PARAM_NAME = "bauk.home";

	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH = "inputFeedFilePath";
	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME = "inputFeedFileName";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP = "inputFeedFileReceivedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSED_TIMESTAMP = "inputFeedFileProcessedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH = "bulkLoadOutputFilePath";

	public static final String HEADER_ATTRIBUTE_PREFIX = "header.";
	public static final String GLOBAL_ATTRIBUTE_PREFIX = "global.";

	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH = "bulkFilePath";
	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME = "bulkFileName";
	public static final String IMPLICIT_ATTRIBUTE_FILE_BULK_FILE_RECEIVED_TIMESTAMP = "bulkFileReceivedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_FILE_BULK_FILE_PROCESSED_TIMESTAMP = "bulkFileProcessedTimestamp";

	public static final String STATEMENT_PLACEHOLDER_DELIMITER_START = "${";
	public static final String STATEMENT_PLACEHOLDER_DELIMITER_END = "}";

	public static final String NATURAL_KEY_DELIMITER = "_|_|_";

	public static final int SQL_EXECUTION_WARNING_THRESHOLD_MILLIS = 1000 * 10;
	public static final String SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME = "sql.execution.warning.threshold.millis";

}
