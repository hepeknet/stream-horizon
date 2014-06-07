package com.threeglav.sh.bauk.files.feed.jdbc;

import java.io.IOException;
import java.util.List;

import javax.sql.DataSource;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.dimension.db.DataSourceProvider;
import com.threeglav.sh.bauk.dimension.db.DbHandler;
import com.threeglav.sh.bauk.dimension.db.SpringJdbcDbHandler;
import com.threeglav.sh.bauk.files.BaukFile;
import com.threeglav.sh.bauk.files.ListBaukFile;
import com.threeglav.sh.bauk.files.feed.FeedFileProcessor;
import com.threeglav.sh.bauk.model.BaukConfiguration;

public class JDBCFeedSourceJob implements Job {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public void execute(final JobExecutionContext context) throws JobExecutionException {
		final JobDataMap jdm = context.getJobDetail().getJobDataMap();
		final String sqlStatement = jdm.getString("sql");
		final String delimiterString = jdm.getString("delimiter");
		final String jdbcUrl = jdm.getString("url");
		final FeedFileProcessor ffp = (FeedFileProcessor) jdm.get("ffp");
		final BaukConfiguration config = (BaukConfiguration) jdm.get("config");
		final DataSource jdbcDataSource = DataSourceProvider.getJdbcFeedSourceDataSource(jdbcUrl);
		final DbHandler dbHandler = new SpringJdbcDbHandler(config);
		final List<List<String>> allRowsAsStrings = dbHandler.selectAllRowsAsStrings(sqlStatement, jdbcDataSource);
		if (allRowsAsStrings == null || allRowsAsStrings.isEmpty()) {
			return;
		}
		final BaukFile bf = new ListBaukFile(allRowsAsStrings, delimiterString);
		bf.setFileNameOnly("feed from jdbc source");
		bf.setFullFilePath("feed from jdbc source");
		try {
			ffp.process(bf);
			log.debug("Successfully processed feed. Source of feed data was {}", sqlStatement);
		} catch (final IOException e) {
			log.error("Exception while processing feed. Source of feed data was {}", sqlStatement);
			log.error("Exception", e);
		}
	}

}
