package com.threeglav.bauk;

public interface SystemConfigurationConstants {

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

	public static final String ENABLE_PER_THREAD_CACHING_PARAM_NAME = "per.thread.caching.enabled";

}
