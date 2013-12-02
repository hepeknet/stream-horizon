package com.threeglav.bauk;

public interface BaukConstants {

	public static final int ONE_MEGABYTE = 1024 * 1024;

	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH = "inputFeedFilePath";
	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME = "inputFeedFileName";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP = "inputFeedFileReceivedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSED_TIMESTAMP = "inputFeedFileProcessedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE = "inputFeedFileSize";
	public static final String IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH = "bulkLoadOutputFilePath";

	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH = "bulkFilePath";
	public static final String IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME = "bulkFileName";
	public static final String IMPLICIT_ATTRIBUTE_FILE_BULK_FILE_RECEIVED_TIMESTAMP = "bulkFileReceivedTimestamp";
	public static final String IMPLICIT_ATTRIBUTE_FILE_BULK_FILE_PROCESSED_TIMESTAMP = "bulkFileProcessedTimestamp";

	public static final String COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG = "feedProcessingSuccessFailureFlag";
	public static final String COMPLETION_ATTRIBUTE_NUMBER_OF_ROWS_IN_FEED = "numberOfRowsInFeed";
	public static final String COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION = "feedProcessingErrorDescription";

	public static final String STATEMENT_PLACEHOLDER_DELIMITER_START = "${";
	public static final String STATEMENT_PLACEHOLDER_DELIMITER_END = "}";

	public static final String NATURAL_KEY_DELIMITER = "_|_|_";

}
