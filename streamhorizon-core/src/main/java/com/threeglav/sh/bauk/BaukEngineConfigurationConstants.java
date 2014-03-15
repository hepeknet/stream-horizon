package com.threeglav.sh.bauk;

public interface BaukEngineConfigurationConstants {

	public static final String DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME = "dimension.local.cache.default.size";
	public static final int DIMENSION_LOCAL_CACHE_SIZE_DEFAULT = 5000;

	public static final String DIMENSION_LOCAL_CACHE_DISABLED = "dimension.local.cache.disable";

	public static final String DETECT_OTHER_BAUK_INSTANCES = "detect.running.bauk.instances";

	/**
	 * Points to installation folder. Startup scripts will ensure that this is set.
	 */
	public static final String APP_HOME_SYS_PARAM_NAME = "bauk.home";

	/**
	 * Used to make it easier for testing - should not be used in production
	 */
	public static final String IDEMPOTENT_FEED_PROCESSING_PARAM_NAME = "idempotent.feed.processing";

	/**
	 * Where internal db data (used for housekeeping) will be stored (relative to home folder)
	 */
	public static final String DB_DATA_FOLDER = "/data/db/";

	public static final String PLUGINS_FOLDER_NAME = "/plugins/";

	public static final String CONFIG_FOLDER_NAME = "/config/";

	public static final String WEB_APPS_FOLDER_NAME = "/data/web/";

	public static final int SQL_EXECUTION_WARNING_THRESHOLD_MILLIS = 1000 * 10;
	public static final String SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME = "sql.execution.warning.threshold.millis";

	public static final float DEFAULT_READ_WRITE_BUFFER_SIZE_MB = 1.0f;

	public static final String READ_BUFFER_SIZE_SYS_PARAM_NAME = "read.buffer.size.mb";

	public static final String WRITE_BUFFER_SIZE_SYS_PARAM_NAME = "write.buffer.size.mb";

	public static final int DEFAULT_MAX_DRAINED_ELEMENTS = 1000;
	public static final String MAX_DRAINED_ELEMENTS_SYS_PARAM_NAME = "max.drained.elements";

	public static final String METRICS_OFF_SYS_PARAM_NAME = "metrics.off";

	public static final String CACHE_PROVIDER_SYS_PARAM_NAME = "cache.provider";

	public static final String JDBC_CLIENT_INFO_PROGRAM_NAME_PARAM_NAME = "jdbc.client.info.program";

	public static final String PRE_CACHE_FETCH_SIZE_PARAM_NAME = "dimension.precache.jdbc.fetch.size";

	public static final String RENAME_ARCHIVED_FILES_PARAM_NAME = "rename.archived.files";

	public static final String PRINT_PROCESSING_STATISTICS_PARAM_NAME = "output.processing.statistics";

	public static final String PRINT_STATISTICS_AVERAGE_PARAM_NAME = "output.final.average.statistics";

	public static final String ENABLE_PER_THREAD_CACHING_PARAM_NAME = "per.thread.caching.enabled";

	/**
	 * When we encounter null to be written to output file what should be actually written
	 */
	public static final String BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME = "bulk.output.file.null.value.string";

	/**
	 * Default value for parameter above
	 */
	public static final String BULK_OUTPUT_FILE_NULL_VALUE_DEFAULT = "";

	public static final String FILE_POLLING_DELAY_MILLIS_PARAM_NAME = "file.polling.delay.millis";

	public static final int FILE_POLLING_DELAY_MILLIS_DEFAULT = 1000;

	public static final String FEED_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS_PARAM_NAME = "feed.file.acceptance.timeout.millis";

	public static final int FEED_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT = 2000;

	public static final String BULK_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS_PARAM_NAME = "bulk.file.acceptance.timeout.millis";

	public static final String BULK_FILE_RECORD_FILE_SUBMISSIONS = "bulk.file.record.submission.attempts";

	public static final int BULK_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT = 2000;

	public static final String JDBC_BULK_LOADING_BATCH_SIZE_PARAM_NAME = "jdbc.bulk.loading.batch.size";

	public static final String JDBC_THREADS_PARTITION_COUNT = "jdbc.threads.partition.count";

	public static final int JDBC_BULK_LOADING_BATCH_SIZE_DEFAULT = 10000;

	public static final String REMOTING_SERVER_PORT_PARAM_NAME = "remoting.server.port";

	public static final int REMOTING_SERVER_PORT_DEFAULT = 21000;

	public static final String EMAIL_HOST_PARAM_NAME = "email.server.host";

	public static final String EMAIL_USERNAME_PARAM_NAME = "email.server.username";

	public static final String EMAIL_PASSWORD_PARAM_NAME = "email.server.password";

	public static final String EMAIL_HOST_PORT_PARAM_NAME = "email.server.port";

	public static final String EMAIL_RECIPIENTS_LIST_PARAM_NAME = "processing.error.email.recipients";

	public static final String THROUGHPUT_TESTING_MODE_PARAM_NAME = "throughput.testing.mode";

	public static final String DISABLE_DIMENSION_PRE_CACHING_PARAM_NAME = "dimension.precaching.disabled";

	public static final String DELETE_BULK_LOADED_FILES_PARAM_NAME = "bulk.delete.files.after.load";

	public static final String BAUK_INSTANCE_ID_PARAM_NAME = "BAUK_INSTANCE_ID";

	// partitioning - multiple instances

	public static final String MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME = "multi.instance.total.partition.count";

	// overriding properties defined in XSD

	public static final String FEED_PROCESSING_THREADS_PARAM_NAME = "etlProcessingThreadCount";

	public static final String BULK_PROCESSING_THREADS_PARAM_NAME = "databaseProcessingThreadCount";

	public static final String SOURCE_DIRECTORY_PARAM_NAME = "sourceDirectory";

	public static final String OUTPUT_DIRECTORY_PARAM_NAME = "bulkOutputDirectory";

	public static final String ARCHIVE_DIRECTORY_PARAM_NAME = "archiveDirectory";

	public static final String ERROR_DIRECTORY_PARAM_NAME = "errorDirectory";

}
