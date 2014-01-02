package com.threeglav.bauk;

public interface SystemConfigurationConstants {

	public static final String DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME = "dimension.local.cache.size";
	public static final int DIMENSION_LOCAL_CACHE_SIZE_DEFAULT = 40000;

	/**
	 * Points to installation folder. Startup scripts will ensure that this is set.
	 */
	public static final String APP_HOME_SYS_PARAM_NAME = "bauk.home";

	/**
	 * Where internal db data (used for housekeeping) will be stored (relative to home folder)
	 */
	public static final String DB_DATA_FOLDER = "/data/db/";

	public static final int SQL_EXECUTION_WARNING_THRESHOLD_MILLIS = 1000 * 10;
	public static final String SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME = "sql.execution.warning.threshold.millis";

	public static final int DEFAULT_READ_WRITE_BUFFER_SIZE_MB = 1;
	public static final String READ_WRITE_BUFFER_SIZE_SYS_PARAM_NAME = "rw.buffer.size.mb";

	public static final int DEFAULT_MAX_DRAINED_ELEMENTS = 1000;
	public static final String MAX_DRAINED_ELEMENTS_SYS_PARAM_NAME = "max.drained.elements";

	public static final String METRICS_OFF_SYS_PARAM_NAME = "metrics.off";

}
