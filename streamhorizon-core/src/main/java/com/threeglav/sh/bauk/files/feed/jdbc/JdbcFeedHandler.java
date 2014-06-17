package com.threeglav.sh.bauk.files.feed.jdbc;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.ArrayList;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import com.threeglav.sh.bauk.files.feed.AbstractFeedHandler;
import com.threeglav.sh.bauk.files.feed.FeedFileProcessor;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedSource;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;

public class JdbcFeedHandler extends AbstractFeedHandler {

	private final FeedFileProcessor ffp;
	private final String scheduleExpression;
	private final String sqlStatement;
	private final String jdbcUrl;
	private Scheduler scheduler;
	private final String delimiterString;

	public JdbcFeedHandler(final Feed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		ffp = new FeedFileProcessor(factFeed, config, "jdbc-feed");
		final ArrayList<BaukProperty> properties = factFeed.getSource().getProperties();
		sqlStatement = BaukPropertyUtil.getRequiredUniqueProperty(properties, FeedSource.JDBC_FEED_SOURCE_SQL_STATEMENT_PROPERTY_NAME).getValue();
		scheduleExpression = BaukPropertyUtil.getRequiredUniqueProperty(properties, FeedSource.JDBC_FEED_SOURCE_SCHEDULE_PROPERTY_NAME).getValue();
		jdbcUrl = BaukPropertyUtil.getRequiredUniqueProperty(properties, FeedSource.JDBC_FEED_SOURCE_JDBC_URL_PROPERTY_NAME).getValue();
		delimiterString = factFeed.getDelimiterString();
	}

	@Override
	public void init() {
		try {
			final Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.start();
			final JobDetail job = newJob(JDBCFeedSourceJob.class).withIdentity("jdbc_job", "jdbc_jobs").usingJobData("url", jdbcUrl)
					.usingJobData("sql", sqlStatement).usingJobData("delimiter", delimiterString).build();
			job.getJobDataMap().put("ffp", ffp);
			job.getJobDataMap().put("config", config);
			final Trigger trigger = newTrigger().withIdentity("jdbc_schedule_job", "jdbc_feed_input").withSchedule(cronSchedule(scheduleExpression))
					.forJob("jdbc_job", "jdbc_jobs").build();
			scheduler.scheduleJob(job, trigger);
			log.info("Scheduled {} to be executed at {}", sqlStatement, scheduleExpression);
		} catch (final Exception exc) {
			log.error("Exception while starting scheduling for jdbc feed input", exc);
			System.exit(-1);
		}
	}

	@Override
	public void stop() {
		if (scheduler != null) {
			try {
				scheduler.shutdown();
				log.info("Stopped all scheduled jobs!");
			} catch (final SchedulerException e) {
				log.error("Exception while stopping scheduled jobs", e);
			}
		}
		super.stop();
	}

}
