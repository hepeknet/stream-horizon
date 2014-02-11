package com.threeglav.bauk.main;

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

import com.threeglav.bauk.BaukEngineConfigurationConstants;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.EngineRegistry;
import com.threeglav.bauk.command.BaukCommandsExecutor;
import com.threeglav.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.bauk.files.bulk.BulkFilesHandler;
import com.threeglav.bauk.files.feed.FeedFilesHandler;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.remoting.RemotingServer;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.CacheUtil;
import com.threeglav.bauk.util.StringUtil;

public class BaukApplication {

	private static final String DEFAULT_CONFIG_FILE_NAME = "feedConfig.xml";
	private static final String CONFIG_FILE_PROP_NAME = "bauk.config";

	private static final Logger LOG = LoggerFactory.getLogger(BaukApplication.class);

	private static long instanceStartTime;
	private static RemotingServer remotingHandler;
	private static final List<FeedFilesHandler> feedFileHandlers = new LinkedList<>();
	private static final List<BulkFilesHandler> bulkFileHandlers = new LinkedList<>();

	public static void main(final String[] args) throws Exception {
		BaukUtil.logEngineMessage("Starting Bauk engine");
		final long start = System.currentTimeMillis();
		LOG.info("To run in test mode set system parameter {}=true", BaukEngineConfigurationConstants.IDEMPOTENT_FEED_PROCESSING_PARAM_NAME);
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		final BaukConfiguration conf = findConfiguration();
		if (conf != null) {
			ConfigurationProperties.setBaukProperties(conf.getProperties());
			final ConfigurationValidator configValidator = new ConfigurationValidator(conf);
			configValidator.validate();
			remotingHandler = new RemotingServer();
			remotingHandler.start();
			createProcessingRoutes(conf);
			final boolean throughputTestingMode = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);
			if (throughputTestingMode) {
				BaukUtil.logEngineMessageSync("ENGINE IS RUNNING IN THROUGHPUT TESTING MODE! ONE INPUT FEED FILE PER THREAD WILL BE CACHED AND PROCESSED REPEATEDLY!!!");
			}
			instanceStartTime = System.currentTimeMillis();
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
			BaukUtil.logEngineMessage("Bauk engine started successfully in " + total + "ms (" + totalSec + " seconds). Waiting for feed files...\n\n");
		} else {
			LOG.error(
					"Unable to find valid configuration file! Check your startup scripts and make sure system property {} points to valid feed configuration file. Aborting!",
					CONFIG_FILE_PROP_NAME);
			BaukUtil.logEngineMessage("Unable to find valid configuration file! Check your startup scripts and make sure system property "
					+ CONFIG_FILE_PROP_NAME + " points to valid feed configuration file. Aborting!");
			return;
		}
		// sleep forever
		while (true) {
			Thread.sleep(30000);
		}
	}

	private static void startProcessing() throws Exception {
		int ffhStartedCount = 0;
		for (final FeedFilesHandler ffh : feedFileHandlers) {
			ffhStartedCount += ffh.start();
		}
		BaukUtil.logEngineMessageSync("Started in total " + ffhStartedCount + " feed processing threads!");
		int bfhStartedCount = 0;
		for (final BulkFilesHandler bfh : bulkFileHandlers) {
			bfhStartedCount += bfh.start();
		}
		BaukUtil.logEngineMessageSync("Started in total " + bfhStartedCount + " bulk file processing threads!");
	}

	private static void createProcessingRoutes(final BaukConfiguration config) throws Exception {
		LOG.debug("Starting processing routes...");
		for (final FactFeed feed : config.getFactFeeds()) {
			executeOnStartupCommands(feed, config);
			LOG.trace("Creating routes for feed [{}]", feed.getName());
			try {
				final FeedFilesHandler feedFilesHandler = new FeedFilesHandler(feed, config);
				feedFilesHandler.createFileHandlers();
				feedFileHandlers.add(feedFilesHandler);
				final BulkFilesHandler bulkFilesHandler = new BulkFilesHandler(feed, config);
				bulkFilesHandler.createFileHandlers();
				bulkFileHandlers.add(bulkFilesHandler);
				LOG.debug("Successfully added routes for feed [{}]", feed.getName());
			} catch (final Exception exc) {
				BaukUtil.logEngineMessage(exc.getMessage());
				LOG.error("Exception while starting route. Exiting application!", exc);
				System.exit(-1);
				throw exc;
			}
		}
	}

	private static void executeOnStartupCommands(final FactFeed feed, final BaukConfiguration config) {
		if (feed.getOnStartup() != null) {
			try {
				LOG.debug("Executing on-startup commands for feed {}", feed.getName());
				final BaukCommandsExecutor bce = new BaukCommandsExecutor(feed, config);
				bce.executeBaukCommandSequence(feed.getOnStartup(), null, "On startup commands for feed " + feed.getName());
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
			is = BaukApplication.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE_NAME);
			if (is == null) {
				LOG.error("Was not able to find file {} in the classpath. Unable to start application", DEFAULT_CONFIG_FILE_NAME);
				return null;
			}
			LOG.info("Found configuration file {} in classpath", DEFAULT_CONFIG_FILE_NAME);
		} else {
			LOG.info("Found system property {}={}. Will try to load it as configuration...", CONFIG_FILE_PROP_NAME, configFile);
			try {
				is = new FileInputStream(configFile);
				LOG.info("Successfully found configuration [{}] on file system", configFile);
			} catch (final FileNotFoundException fnfe) {
				LOG.error("Was not able to find file {}. Use full, absolute path.", configFile);
				return null;
			}
		}
		try {
			LOG.debug("Trying to load configuration from xml file");
			final JAXBContext jaxbContext = JAXBContext.newInstance(BaukConfiguration.class);
			final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			final String configFolderPath = ConfigurationProperties.getConfigFolder();
			final File configFolder = new File(configFolderPath);
			final File xsdFile = new File(configFolder, "bauk_config.xsd");
			final Schema schema = schemaFactory.newSchema(xsdFile);
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
			BaukUtil.logEngineMessage("Shutting down engine. Waiting to gracefully stop all processing threads...");
			BaukUtil.startShutdown();
			CacheUtil.getCacheInstanceManager().stop();
			remotingHandler.stop();
			for (final FeedFilesHandler ffh : feedFileHandlers) {
				ffh.stop();
			}
			for (final BulkFilesHandler bfh : bulkFileHandlers) {
				bfh.stop();
			}
			BaukUtil.logEngineMessage("Bauk engine is down!");
			printStatistics();
		}
	}

	private static void printStatistics() {
		final long totalInputFeedFilesProcessed = EngineRegistry.getProcessedFeedFilesCount();
		final long totalInputFeedRowsProcessed = EngineRegistry.getProcessedFeedRowsTotal();
		if (totalInputFeedFilesProcessed > 0 && totalInputFeedRowsProcessed > 0) {
			final long totalUpTimeMillis = System.currentTimeMillis() - instanceStartTime;
			final long totalUpTimeSec = totalUpTimeMillis / 1000;
			final long minutes = totalUpTimeSec / 60;
			final long remainedSeconds = totalUpTimeSec % 60;
			final long averageFilesPerSecond = totalInputFeedFilesProcessed / totalUpTimeSec;
			final long averageRowsPerSecond = totalInputFeedRowsProcessed / totalUpTimeSec;
			BaukUtil.logEngineMessageSync("Uptime of this instance was " + totalUpTimeSec + " seconds (" + minutes + " minutes and "
					+ remainedSeconds + " seconds). In total processed " + totalInputFeedFilesProcessed + " input feed files and "
					+ totalInputFeedRowsProcessed + " rows.");
			BaukUtil.logEngineMessageSync("On average processed " + averageFilesPerSecond + " files/sec, " + averageRowsPerSecond + " rows/sec.");
		} else {
			BaukUtil.logEngineMessageSync("No files were processed or statistics are turned off.");
		}
	}

}
