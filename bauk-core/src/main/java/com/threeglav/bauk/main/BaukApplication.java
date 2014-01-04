package com.threeglav.bauk.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.camel.BulkLoadFileProcessingRoute;
import com.threeglav.bauk.camel.InputFeedFileProcessingRoute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.CacheUtil;
import com.threeglav.bauk.util.StringUtil;

public class BaukApplication {

	private static final String DEFAULT_CONFIG_FILE_NAME = "feedConfig.xml";
	private static final String CONFIG_FILE_PROP_NAME = "bauk.config";

	private static final Logger LOG = LoggerFactory.getLogger(BaukApplication.class);

	private static final CamelContext camelContext = new DefaultCamelContext();

	public static void main(final String[] args) throws Exception {
		BaukUtil.logEngineMessage("Starting engine");
		LOG.info("To run in test mode set system parameter {}=true", SystemConfigurationConstants.BAUK_TEST_MODE_PARAM_NAME);
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		final BaukConfiguration conf = findConfiguration();
		if (conf != null) {
			final ConfigurationValidator configValidator = new ConfigurationValidator(conf);
			configValidator.validate();
			createCamelRoutes(conf);
			final int numberOfInstances = CacheUtil.getNumberOfBaukInstances();
			BaukUtil.logEngineMessage("Total number of detected running engine instances is " + numberOfInstances);
			BaukUtil.logEngineMessage("Engine started successfully. Ready for feed files...");
		} else {
			LOG.error(
					"Unable to find valid configuration file! Check your startup scripts and make sure system property {} points to valid feed configuration file. Aborting!",
					CONFIG_FILE_PROP_NAME);
			return;
		}
		// sleep forever
		while (true) {
			Thread.sleep(30000);
		}
	}

	private static void createCamelRoutes(final BaukConfiguration config) throws Exception {
		LOG.debug("Starting camel routes...");
		for (final FactFeed feed : config.getFactFeeds()) {
			LOG.trace("Creating routes for feed [{}]", feed.getName());
			final InputFeedFileProcessingRoute fpr = new InputFeedFileProcessingRoute(feed, config);
			final BulkLoadFileProcessingRoute blfp = new BulkLoadFileProcessingRoute(feed, config);
			try {
				camelContext.addRoutes(fpr);
				if (blfp.shouldStartRoute()) {
					camelContext.addRoutes(blfp);
				}
				LOG.debug("Successfully added routes for feed [{}]", feed.getName());
			} catch (final Exception exc) {
				LOG.error("Exception while starting route. Exiting application!", exc);
				System.exit(-1);
				throw exc;
			}
		}
		LOG.debug("Starting camel context");
		camelContext.start();
		LOG.debug("Successfully started camel context");
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
			BaukUtil.logEngineMessage("Shutting down engine");
			BaukUtil.startShutdown();
			CacheUtil.shutdownHazelcast();
			try {
				camelContext.stop();
			} catch (final Exception ignored) {
				// ignored
			}
			BaukUtil.logEngineMessage("Engine is down!");
		}
	}

}
