package com.threeglav.bauk.camel.bulk;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.dimension.db.DataSourceProvider;
import com.threeglav.bauk.util.InMemoryDbUtil;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileSubmissionRecorder {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String SUBMISSION_DB_NAME = "submissions";

	private static final String SELECT_FILE_SQL = "select file_name from submitted_files where file_name = ?";

	private static final String INSERT_FILE_SQL = "insert into submitted_files(file_name) values(?)";

	private static final String DELETE_FILE_SQL = "delete from submitted_files where file_name = ?";

	private final DataSource dataSource;

	public BulkFileSubmissionRecorder() {
		final String url = InMemoryDbUtil.getJdbcUrl(SUBMISSION_DB_NAME);
		dataSource = DataSourceProvider.getSimpleDataSource(url);
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

	public boolean wasAlreadySubmitted(final String bulkFileName) {
		if (StringUtil.isEmpty(bulkFileName)) {
			throw new IllegalArgumentException("Bulk file name must not be null or empty");
		}
		final Object res = InMemoryDbUtil.executePreparedStatement(SELECT_FILE_SQL, dataSource, bulkFileName);
		final boolean found = bulkFileName.equals(String.valueOf(res));
		if (!found) {
			log.debug("[{}] was not already submitted for bulk loading", bulkFileName);
		} else {
			log.warn("[{}] was already submitted for bulk loading before", bulkFileName);
		}
		return found;
	}

	public void recordSubmissionAttempt(final String bulkFileName) {
		if (StringUtil.isEmpty(bulkFileName)) {
			throw new IllegalArgumentException("Bulk file name must not be null or empty");
		}
		log.debug("Recording submission of [{}] for bulk loading", bulkFileName);
		InMemoryDbUtil.executePreparedStatement(INSERT_FILE_SQL, dataSource, bulkFileName);
		log.debug("Successfully recorded submission of bulk file {}", bulkFileName);
	}

	public void deleteLoadedFile(final String bulkFileName) {
		if (StringUtil.isEmpty(bulkFileName)) {
			throw new IllegalArgumentException("Bulk file name must not be null or empty");
		}
		log.debug("Deleting already loaded file [{}]", bulkFileName);
		InMemoryDbUtil.executePreparedStatement(DELETE_FILE_SQL, dataSource, bulkFileName);
		log.debug("Successfully deleted recording of submission for {}", bulkFileName);
	}

}
