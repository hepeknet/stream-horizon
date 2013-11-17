package com.threeglav.bauk.camel;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.dimension.db.SpringJdbcDbHandler;
import com.threeglav.bauk.feed.TextFileReaderComponent;
import com.threeglav.bauk.model.BulkDefinition;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StreamUtil;
import com.threeglav.bauk.util.StringUtil;

class FeedFileProcessor implements Processor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final FactFeed factFeed;
	private final Config config;
	private final Meter inputFeedsProcessed;
	private final Histogram inputFeedProcessingTime;
	private final TextFileReaderComponent textFileReaderComponent;
	private String fileExtension;

	public FeedFileProcessor(final FactFeed factFeed, final Config config, final String fileMask) {
		if (factFeed == null) {
			throw new IllegalArgumentException("Fact feed must not be null");
		}
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		if (StringUtil.isEmpty(fileMask)) {
			throw new IllegalArgumentException("File mask must not be null or empty");
		}
		this.factFeed = factFeed;
		this.config = config;
		this.validate();
		final CacheInstanceManager cacheInstanceManager = new HazelcastCacheInstanceManager();
		final DbHandler dbHandler = new SpringJdbcDbHandler(this.config);
		final String cleanFileMask = StringUtil.replaceAllNonASCII(fileMask);
		this.textFileReaderComponent = new TextFileReaderComponent(this.factFeed, this.config, dbHandler, cacheInstanceManager, cleanFileMask);
		this.inputFeedsProcessed = MetricsUtil.createMeter("Input feeds (" + cleanFileMask + ") - processed files count");
		this.inputFeedProcessingTime = MetricsUtil.createHistogram("Input feeds (" + cleanFileMask + ") - processing time (millis)");
	}

	private void validate() {
		if (StringUtil.isEmpty(this.config.getBulkOutputDirectory())) {
			throw new IllegalStateException("Bulk output directory must not be null or empty!");
		}
		this.fileExtension = BulkDefinition.DEFAULT_BULK_OUTPUT_EXTENSION;
		final BulkDefinition bulkDefinition = this.factFeed.getBulkDefinition();
		if (bulkDefinition != null && !StringUtil.isEmpty(bulkDefinition.getBulkOutputExtension())) {
			this.fileExtension = bulkDefinition.getBulkOutputExtension();
		}
		if (!this.fileExtension.matches("[A-Za-z0-9]+")) {
			throw new IllegalStateException("Bulk file extension must contain only alpha-numerical characters. Currently " + this.fileExtension);
		}
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		InputStream inputStream = exchange.getIn().getBody(InputStream.class);
		final String fullFilePath = (String) exchange.getIn().getHeader("CamelFileName");
		final Long lastModified = (Long) exchange.getIn().getHeader("CamelFileLastModified");
		final Long fileLength = (Long) exchange.getIn().getHeader("CamelFileLength");
		if (inputStream != null) {
			final String lowerCaseFilePath = fullFilePath.toLowerCase();
			if (lowerCaseFilePath.endsWith(".zip")) {
				inputStream = StreamUtil.unzipInputStream(inputStream);
			} else if (lowerCaseFilePath.endsWith(".gz")) {
				inputStream = StreamUtil.ungzipInputStream(inputStream);
			}
			final long start = System.currentTimeMillis();
			this.log.debug("Received filePath={}, lastModified={}, fileLength={}", fullFilePath, lastModified, fileLength);
			final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(exchange);
			this.textFileReaderComponent.process(inputStream, globalAttributes);
			IOUtils.closeQuietly(inputStream);
			final long total = System.currentTimeMillis() - start;
			if (this.inputFeedsProcessed != null) {
				this.inputFeedsProcessed.mark();
			}
			if (this.inputFeedProcessingTime != null) {
				this.inputFeedProcessingTime.update(total);
			}
			this.log.debug("Successfully processed [{}] in {}ms", fullFilePath, total);
		} else {
			this.log.warn("Stream is null - unable to process file");
		}
	}

	private String getOutputFilePath(final String inputFileName) {
		this.log.debug("Will use {} file extension for bulk files", this.fileExtension);
		return this.config.getBulkOutputDirectory() + "/" + this.factFeed.getName() + "_" + StringUtil.getFileNameWithoutExtension(inputFileName)
				+ "." + this.fileExtension;
	}

	private Map<String, String> createImplicitGlobalAttributes(final Exchange exchange) {
		final Map<String, String> attributes = new HashMap<String, String>();
		final String fileNameOnly = (String) exchange.getIn().getHeader("CamelFileNameOnly");
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME, fileNameOnly);
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH, (String) exchange.getIn().getHeader("CamelFileName"));
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP,
				String.valueOf(exchange.getIn().getHeader("CamelFileLastModified")));
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSED_TIMESTAMP, "" + System.currentTimeMillis());
		attributes.put(Constants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH, this.getOutputFilePath(fileNameOnly));
		this.log.debug("Created global attributes {}", attributes);
		return attributes;
	}

}