package com.threeglav.sh.bauk.files.bulk;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.dimension.db.DataSourceProvider;
import com.threeglav.sh.bauk.util.InMemoryDbUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class BulkFileSubmissionRecorder {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String SUBMISSION_DB_NAME = "submissions";

	private static final String SELECT_FILE_SQL = "select file_name from submitted_files where file_name = ?";

	private static final String INSERT_FILE_SQL = "insert into submitted_files(file_name) values(?)";

	private static final String DELETE_FILE_SQL = "delete from submitted_files where file_name = ?";

	private final DataSource dataSource;

	private final boolean isDebugEnabled;

	public BulkFileSubmissionRecorder() {
		final String url = InMemoryDbUtil.getJdbcUrl(SUBMISSION_DB_NAME);
		dataSource = DataSourceProvider.getSimpleDataSource(url);
		isDebugEnabled = log.isDebugEnabled();
		this.tryCreatingTables();
	}

	private void tryCreatingTables() {
		try {
			final String createTableSql = "create table submitted_files(file_name varchar);";
			InMemoryDbUtil.executeUpdateStatement(createTableSql, dataSource);
			final String createIndexSql = "create unique index s_f_u_index on submitted_files(file_name);";
			InMemoryDbUtil.executeUpdateStatement(createIndexSql, dataSource);
			log.info("Successfully created tables and indexes for submission tracking");
		} catch (final Exception exc) {
			log.info("Failed creating tables. Most likely already created!", exc);
		}
	}

	boolean wasAlreadySubmitted(final String bulkFileName) {
		if (StringUtil.isEmpty(bulkFileName)) {
			throw new IllegalArgumentException("Bulk file name must not be null or empty");
		}
		final Object res = InMemoryDbUtil.executePreparedStatement(SELECT_FILE_SQL, dataSource, bulkFileName);
		final boolean found = bulkFileName.equals(String.valueOf(res));
		if (!found) {
			if (isDebugEnabled) {
				log.debug("[{}] was not already submitted for bulk loading", bulkFileName);
			}
		} else {
			log.warn("[{}] was already submitted for bulk loading before", bulkFileName);
		}
		return found;
	}

	void recordSubmissionAttempt(final String bulkFileName) {
		if (StringUtil.isEmpty(bulkFileName)) {
			throw new IllegalArgumentException("Bulk file name must not be null or empty");
		}
		if (isDebugEnabled) {
			log.debug("Recording submission of [{}] for bulk loading", bulkFileName);
		}
		InMemoryDbUtil.executePreparedStatement(INSERT_FILE_SQL, dataSource, bulkFileName);
		if (isDebugEnabled) {
			log.debug("Successfully recorded submission of bulk file {}", bulkFileName);
		}
	}

	void deleteLoadedFile(final String bulkFileName) {
		if (StringUtil.isEmpty(bulkFileName)) {
			throw new IllegalArgumentException("Bulk file name must not be null or empty");
		}
		if (isDebugEnabled) {
			log.debug("Deleting already loaded file [{}]", bulkFileName);
		}
		InMemoryDbUtil.executePreparedStatement(DELETE_FILE_SQL, dataSource, bulkFileName);
		if (isDebugEnabled) {
			log.debug("Successfully deleted recording of submission for {}", bulkFileName);
		}
	}

}
