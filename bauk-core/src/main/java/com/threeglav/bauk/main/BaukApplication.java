package com.threeglav.bauk.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.camel.BulkLoadFileProcessingRoute;
import com.threeglav.bauk.camel.InputFeedFileProcessingRoute;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class BaukApplication {

	private static final String DEFAULT_CONFIG_FILE_NAME = "baukConfig.xml";
	private static final String CONFIG_FILE_PROP_NAME = "bauk.config";

	private static final Logger LOG = LoggerFactory.getLogger(BaukApplication.class);

	private static final CamelContext camelContext = new DefaultCamelContext();

	public static void main(final String[] args) throws Exception {
		LOG.info("Starting application");
		final Config conf = findConfiguration();
		if (conf != null) {
			final ConfigurationValidator configValidator = new ConfigurationValidator(conf);
			configValidator.validate();
			createCamelRoutes(conf);
		} else {
			LOG.error("Unable to find valid configuration! Aborting!");
			return;
		}
		// sleep forever
		while (true) {
			Thread.sleep(30000);
		}
	}

	private static void createCamelRoutes(final Config config) throws Exception {
		LOG.debug("Starting camel routes...");
		for (final FactFeed feed : config.getFactFeeds()) {
			LOG.trace("Creating route for {}", feed);
			final InputFeedFileProcessingRoute fpr = new InputFeedFileProcessingRoute(feed, config);
			final BulkLoadFileProcessingRoute blfp = new BulkLoadFileProcessingRoute(feed, config);
			try {
				camelContext.addRoutes(fpr);
				if (blfp.shouldStartRoute()) {
					camelContext.addRoutes(blfp);
				}
				LOG.debug("Successfully added routes for {}", feed);
			} catch (final Exception exc) {
				LOG.error("Exception while starting route. Existing application!", exc);
				System.exit(-1);
				throw exc;
			}
		}
		LOG.debug("Starting camel context");
		camelContext.start();
		LOG.debug("Successfully started camel context");
	}

	private static final Config findConfiguration() {
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
			LOG.info("Found configuration in classpath");
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
			final JAXBContext jaxbContext = JAXBContext.newInstance(Config.class);
			final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			final Config config = (Config) jaxbUnmarshaller.unmarshal(is);
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

}
