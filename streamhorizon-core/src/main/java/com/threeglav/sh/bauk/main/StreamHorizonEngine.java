package com.threeglav.sh.bauk.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.command.BaukCommandsExecutor;
import com.threeglav.sh.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.sh.bauk.dimension.db.DataSourceProvider;
import com.threeglav.sh.bauk.files.bulk.BulkFilesHandler;
import com.threeglav.sh.bauk.files.feed.FeedFilesHandler;
import com.threeglav.sh.bauk.files.feed.FeedHandler;
import com.threeglav.sh.bauk.files.feed.ThriftFeedHandler;
import com.threeglav.sh.bauk.files.feed.jdbc.JdbcFeedHandler;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedSource;
import com.threeglav.sh.bauk.remoting.RemotingServer;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.CacheUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class StreamHorizonEngine {

	private static final String DEFAULT_CONFIG_FILE_NAME = "engine-config.xml";
	public static final String CONFIG_FILE_PROP_NAME = "bauk.config";

	private static final Logger LOG = LoggerFactory.getLogger(StreamHorizonEngine.class);

	private static long instanceStartTime;
	private static RemotingServer remotingHandler;
	private static final List<FeedHandler> feedHandlers = new LinkedList<>();
	private static final List<BulkFilesHandler> bulkFileHandlers = new LinkedList<>();

	public static void main(final String[] args) throws Exception {
		printRuntimeInfo();
		final long start = System.currentTimeMillis();
		LOG.info("To run in test mode set system parameter {}=true", BaukEngineConfigurationConstants.IDEMPOTENT_FEED_PROCESSING_PARAM_NAME);
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		final BaukConfiguration conf = findConfiguration();
		if (conf != null) {
			ConfigurationProperties.setBaukProperties(conf.getProperties());
			final ConfigurationValidator configValidator = new ConfigurationValidator(conf);
			try {
				configValidator.validate();
			} catch (final Exception exc) {
				BaukUtil.logEngineMessageSync("Error while validating configuration: " + exc.getMessage());
				LOG.error("", exc);
				System.exit(-1);
			}
			remotingHandler = new RemotingServer();
			remotingHandler.start();
			createProcessingRoutes(conf);
			final boolean throughputTestingMode = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);
			if (throughputTestingMode) {
				BaukUtil.logEngineMessageSync("ENGINE IS RUNNING IN THROUGHPUT TESTING MODE! ONE INPUT FEED FILE PER THREAD WILL BE CACHED AND PROCESSED REPEATEDLY!!!");
			}
			instanceStartTime = System.currentTimeMillis();
			final boolean isMultiInstance = ConfigurationProperties.isConfiguredPartitionedMultipleInstances();
			if (isMultiInstance) {
				final int totalPartitionsCount = ConfigurationProperties.getSystemProperty(
						BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, -1);
				final String myUniqueIdentifier = ConfigurationProperties.getBaukInstanceIdentifier();
				BaukUtil.logEngineMessageSync("Configured to run in multi-instance mode of " + totalPartitionsCount
						+ " instances in total. My unique identifier is " + myUniqueIdentifier);
			}
			BaukUtil.logEngineMessageSync("Finished initialization! Started counting uptime");
			startProcessing();
			final long total = System.currentTimeMillis() - start;
			final long totalSec = total / 1000;
			final boolean detectBaukInstances = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.DETECT_OTHER_BAUK_INSTANCES, false);
			if (detectBaukInstances) {
				final int numberOfInstances = HazelcastCacheInstanceManager.getNumberOfBaukInstances();
				BaukUtil.logEngineMessage("Total number of detected running engine instances is " + numberOfInstances);
			}
			BaukUtil.logEngineMessage("Engine started successfully in " + total + "ms (" + totalSec + " seconds). Waiting for feed files...\n\n");
		} else {
			LOG.error(
					"Unable to find valid configuration file! Check your startup scripts and make sure system property {} points to valid feed configuration file. Aborting!",
					CONFIG_FILE_PROP_NAME);
			BaukUtil.logEngineMessage("Unable to find valid configuration file! Check your startup scripts and make sure system property "
					+ CONFIG_FILE_PROP_NAME + " points to valid feed configuration file. Aborting!");
			System.exit(-1);
		}
		// sleep forever
		while (!BaukUtil.shutdownStarted()) {
			Thread.sleep(10000);
		}
	}

	public static long getEngineInstanceStartTime() {
		return instanceStartTime;
	}

	private static void startProcessing() throws Exception {
		int ffhStartedCount = 0;
		for (final FeedHandler ffh : feedHandlers) {
			ffhStartedCount += ffh.start();
		}
		BaukUtil.logEngineMessageSync("Started in total " + ffhStartedCount + " ETL (feed processing) threads!");
		int bfhStartedCount = 0;
		for (final BulkFilesHandler bfh : bulkFileHandlers) {
			bfhStartedCount += bfh.start();
		}
		BaukUtil.logEngineMessageSync("Started in total " + bfhStartedCount + " DB (bulk file processing) threads!");
	}

	private static void createProcessingRoutes(final BaukConfiguration config) throws Exception {
		LOG.debug("Starting processing routes...");
		for (final Feed feed : config.getFeeds()) {
			executeOnStartupCommands(feed, config);
			LOG.trace("Creating processing routes for feed [{}]", feed.getName());
			try {
				final String feedSourceType = feed.getSource().getType();
				FeedHandler feedHandler = null;
				if (FeedSource.FILE_FEED_SOURCE.equalsIgnoreCase(feedSourceType)) {
					feedHandler = new FeedFilesHandler(feed, config);
				} else if (FeedSource.RPC_FEED_SOURCE.equalsIgnoreCase(feedSourceType)) {
					feedHandler = new ThriftFeedHandler(feed, config);
				} else if (FeedSource.JDBC_FEED_SOURCE.equalsIgnoreCase(feedSourceType)) {
					feedHandler = new JdbcFeedHandler(feed, config);
				} else {
					throw new IllegalArgumentException("Unsupported feed source type " + feedSourceType);
				}
				feedHandler.init();
				feedHandlers.add(feedHandler);
				if (feed.isFileTarget()) {
					final BulkFilesHandler bulkFilesHandler = new BulkFilesHandler(feed, config);
					bulkFilesHandler.createFileHandlers();
					bulkFileHandlers.add(bulkFilesHandler);
					LOG.debug("Feed {} is outputing files. Will start bulk file handlers", feed.getName());
				} else {
					LOG.info("Feed {} is not outputing files", feed.getName());
				}
				LOG.debug("Successfully added routes for feed [{}]", feed.getName());
			} catch (final Exception exc) {
				BaukUtil.logEngineMessage(exc.getMessage());
				LOG.error("Exception while starting route. Exiting application!", exc);
				System.exit(-1);
				throw exc;
			}
		}
	}

	private static void executeOnStartupCommands(final Feed feed, final BaukConfiguration config) {
		if (feed.getEvents() != null && feed.getEvents().getOnStartup() != null) {
			try {
				LOG.debug("Executing on-startup commands for feed {}", feed.getName());
				final BaukCommandsExecutor bce = new BaukCommandsExecutor(feed, config, feed.getEvents().getOnStartup());
				bce.executeBaukCommandSequence(null, "On startup commands for feed [" + feed.getName() + "]");
				LOG.debug("Finished executing on startup commands for {}", feed.getName());
			} catch (final Exception exc) {
				LOG.error("Exception while executing on startup commands. Will continue with processing.", exc);
			}
		}
	}

	private static final BaukConfiguration findConfiguration() {
		LOG.info("Trying to find configuration file. First if specified as {} system property and then as {} in classpath", CONFIG_FILE_PROP_NAME,
				DEFAULT_CONFIG_FILE_NAME);
		final String configFile = System.getProperty(CONFIG_FILE_PROP_NAME);
		InputStream is = null;
		if (StringUtil.isEmpty(configFile)) {
			LOG.info("Was not able to find system property {}. Trying to find default configuration {} in classpath", CONFIG_FILE_PROP_NAME,
					DEFAULT_CONFIG_FILE_NAME);
			is = StreamHorizonEngine.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE_NAME);
			if (is == null) {
				LOG.error("Was not able to find file {} in the classpath. Unable to start application", DEFAULT_CONFIG_FILE_NAME);
				return null;
			}
			LOG.info("Found configuration file {} in classpath", DEFAULT_CONFIG_FILE_NAME);
		} else {
			LOG.info("Found system property {}={}. Will try to load it as configuration...", CONFIG_FILE_PROP_NAME, configFile);
			final File cFile = new File(configFile);
			try {
				if (cFile.exists() && cFile.isFile()) {
					LOG.debug("Loading config file [{}] from file system", configFile);
					is = new FileInputStream(configFile);
				} else {
					LOG.debug("Loading config file [{}] from classpath", configFile);
					is = StreamHorizonEngine.class.getClass().getResourceAsStream(configFile);
				}
				LOG.info("Successfully found configuration [{}] on file system", configFile);
			} catch (final FileNotFoundException fnfe) {
				LOG.error("Was not able to find file {}. Use full, absolute path.", configFile);
				return null;
			}
		}
		if (is == null) {
			return null;
		}
		try {
			LOG.debug("Trying to load configuration from xml file");
			final JAXBContext jaxbContext = JAXBContext.newInstance(BaukConfiguration.class);
			final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			final Schema schema = schemaFactory.newSchema(StreamHorizonEngine.class.getResource("/bauk_config.xsd"));
			jaxbUnmarshaller.setSchema(schema);
			final BaukConfiguration config = (BaukConfiguration) jaxbUnmarshaller.unmarshal(is);
			LOG.info("Successfully loaded configuration");
			return config;
		} catch (final Exception exc) {
			LOG.error("Exception while loading configuration", exc);
			exc.printStackTrace();
			return null;
		} finally {
			IOUtils.closeQuietly(is);
		}
	}

	private static final class ShutdownHook extends Thread {
		@Override
		public void run() {
			shutdown();
		}
	}

	public static void shutdown() {
		LOG.debug("Gracefully shutting down engine!");
		BaukUtil.logEngineMessage("Shutting down engine. Waiting to gracefully stop all processing threads...");
		BaukUtil.startShutdown();
		LOG.debug("Caches are down!");
		if (remotingHandler != null) {
			remotingHandler.stop();
		}
		for (final FeedHandler ffh : feedHandlers) {
			ffh.stop();
		}
		LOG.debug("Stopped {} feed file handlers", feedHandlers.size());
		for (final BulkFilesHandler bfh : bulkFileHandlers) {
			bfh.stop();
		}
		LOG.debug("Stopped {} bulk file handlers", bulkFileHandlers.size());
		CacheUtil.getCacheInstanceManager().stop();
		DataSourceProvider.shutdown();
		BaukUtil.logEngineMessageSync("StreamHorizon engine is down!");
		printStatistics();
	}

	private static void printStatistics() {
		final long totalInputFeedFilesProcessed = EngineRegistry.getProcessedFeedFilesCount();
		final long totalInputFeedRowsProcessed = EngineRegistry.getProcessedFeedRowsTotal();
		if (totalInputFeedFilesProcessed > 0 && totalInputFeedRowsProcessed > 0) {
			final long totalUpTimeMillis = System.currentTimeMillis() - instanceStartTime;
			final long totalUpTimeSec = totalUpTimeMillis / 1000;
			final long minutes = totalUpTimeSec / 60;
			final long remainedSeconds = totalUpTimeSec % 60;
			final boolean printFinalAverageStatistics = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.PRINT_STATISTICS_AVERAGE_PARAM_NAME, false);
			BaukUtil.logEngineMessageSync("Uptime of this instance was " + totalUpTimeSec + " seconds (" + minutes + " minutes and "
					+ remainedSeconds + " seconds). In total processed " + totalInputFeedFilesProcessed + " input feed files and "
					+ totalInputFeedRowsProcessed + " rows.");
			if (printFinalAverageStatistics) {
				final long averageFilesPerSecond = totalInputFeedFilesProcessed / totalUpTimeSec;
				final long averageRowsPerSecond = totalInputFeedRowsProcessed / totalUpTimeSec;
				BaukUtil.logEngineMessageSync("On average processed " + averageFilesPerSecond + " files/sec, " + averageRowsPerSecond + " rows/sec.");
			}
		}
	}

	private static void printRuntimeInfo() {
		final String version = ConfigurationProperties.getRunningEngineVersion();
		final String entity = ConfigurationProperties.getLicensedEntity();
		if (!StringUtil.isEmpty(version)) {
			BaukUtil.logEngineMessageSync("\n\nStarting StreamHorizon engine version " + version);
		}
		if (StringUtil.isEmpty(entity) || entity.contains("${lic}")) {
			String notLicensedCopy = "\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			notLicensedCopy += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			notLicensedCopy += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			notLicensedCopy += "+++++ THIS IS UNLICENCED COPY OF STREAMHORIZON PLATFORM +++++\n";
			notLicensedCopy += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			notLicensedCopy += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
			notLicensedCopy += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n\n";
			BaukUtil.logEngineMessageSync(notLicensedCopy);
		} else {
			BaukUtil.logEngineMessageSync("This copy of StreamHorizon is licenced to " + entity);
		}
	}

}
