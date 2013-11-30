package com.threeglav.bauk.camel;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipFile;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.feed.FeedCompletionProcessor;
import com.threeglav.bauk.feed.FeedDataProcessor;
import com.threeglav.bauk.feed.MultiThreadedFeedDataProcessor;
import com.threeglav.bauk.feed.SingleThreadedFeedDataProcessor;
import com.threeglav.bauk.feed.TextFileReaderComponent;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StreamUtil;
import com.threeglav.bauk.util.StringUtil;

class FeedFileProcessor implements Processor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private final Meter inputFeedsProcessed;
	private final Histogram inputFeedProcessingTime;
	private final TextFileReaderComponent textFileReaderComponent;
	private final FeedDataProcessor feedDataProcessor;
	private final FeedCompletionProcessor feedCompletionProcessor;
	private String fileExtension;

	public FeedFileProcessor(final FactFeed factFeed, final BaukConfiguration config, final String fileMask) {
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
		final String cleanFileMask = StringUtil.replaceAllNonASCII(fileMask);
		feedDataProcessor = this.createFeedDataProcessor(cleanFileMask);
		textFileReaderComponent = new TextFileReaderComponent(this.factFeed, this.config, feedDataProcessor, cleanFileMask);
		feedCompletionProcessor = this.createFeedCompletionProcessor();
		inputFeedsProcessed = MetricsUtil.createMeter("Input feeds (" + cleanFileMask + ") - processed files count");
		inputFeedProcessingTime = MetricsUtil.createHistogram("Input feeds (" + cleanFileMask + ") - processing time (millis)");
	}

	private FeedDataProcessor createFeedDataProcessor(final String routeId) {
		int numberOfInputThreads = 1;
		if (factFeed.getThreadPoolSizes() != null) {
			numberOfInputThreads = factFeed.getThreadPoolSizes().getFeedProcessingThreads();
		}
		if (numberOfInputThreads == 1) {
			log.debug("Will use single threaded feed data processing");
			return new SingleThreadedFeedDataProcessor(factFeed, config, routeId);
		} else {
			log.debug("Will use multi threaded feed data processing - thread number {}", numberOfInputThreads);
			return new MultiThreadedFeedDataProcessor(factFeed, config, routeId, numberOfInputThreads);
		}
	}

	private void validate() {
		if (StringUtil.isEmpty(config.getBulkOutputDirectory())) {
			throw new IllegalStateException("Bulk output directory must not be null or empty!");
		}
		fileExtension = BulkLoadDefinition.DEFAULT_BULK_OUTPUT_EXTENSION;
		final BulkLoadDefinition bulkDefinition = factFeed.getBulkLoadDefinition();
		if (bulkDefinition != null) {
			final String bulkLoadOutputExtension = bulkDefinition.getBulkLoadOutputExtension();
			if (bulkDefinition != null && !StringUtil.isEmpty(bulkLoadOutputExtension)) {
				fileExtension = bulkLoadOutputExtension;
			}
			if (!fileExtension.matches("[A-Za-z0-9]+")) {
				throw new IllegalStateException("Bulk file extension must contain only alpha-numerical characters. Currently " + fileExtension);
			}
		}
	}

	private FeedCompletionProcessor createFeedCompletionProcessor() {
		final FeedCompletionProcessor processor = null;
		if (factFeed.getOnFeedProcessingCompletion() != null && !factFeed.getOnFeedProcessingCompletion().isEmpty()) {
			log.debug("Found {} to be executed on feed completion for {}", factFeed.getOnFeedProcessingCompletion(), factFeed.getName());
			return new FeedCompletionProcessor(factFeed, config);
		} else {
			log.debug("Did not find anything to execute on feed completion for feed {}", factFeed.getName());
		}
		return processor;
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		final String fullFileName = (String) exchange.getIn().getHeader("CamelFileName");
		final Long lastModified = (Long) exchange.getIn().getHeader("CamelFileLastModified");
		final Long fileLength = (Long) exchange.getIn().getHeader("CamelFileLength");
		final String lowerCaseFilePath = fullFileName.toLowerCase();
		log.debug("Trying to process {}", lowerCaseFilePath);
		if (lowerCaseFilePath.endsWith(".zip")) {
			final File file = exchange.getIn().getBody(File.class);
			final ZipFile zipFile = new ZipFile(file);
			if (!zipFile.entries().hasMoreElements()) {
				IOUtils.closeQuietly(zipFile);
				throw new IllegalStateException("Did not find any entries inside " + fullFileName);
			}
			if (zipFile.size() > 1) {
				log.error("Will process only one zipped entry inside {} and will skip all others. Found {} entries", fullFileName);
			}
			final InputStream inputStream = zipFile.getInputStream(zipFile.entries().nextElement());
			this.processInputStream(exchange, inputStream, fullFileName, lastModified, fileLength);
			IOUtils.closeQuietly(zipFile);
			IOUtils.closeQuietly(inputStream);
		} else if (lowerCaseFilePath.endsWith(".gz")) {
			final InputStream fileInputStream = exchange.getIn().getBody(InputStream.class);
			final InputStream inputStream = StreamUtil.ungzipInputStream(fileInputStream);
			this.processInputStream(exchange, inputStream, fullFileName, lastModified, fileLength);
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(fileInputStream);
		} else {
			final InputStream inputStream = exchange.getIn().getBody(InputStream.class);
			this.processInputStream(exchange, inputStream, fullFileName, lastModified, fileLength);
			IOUtils.closeQuietly(inputStream);
		}
		log.debug("Successfully processed {}", lowerCaseFilePath);
	}

	private void processStreamWithCompletion(final InputStream inputStream, final Map<String, String> globalAttributes) {
		final Map<String, String> completionAttributes = new HashMap<>();
		try {
			textFileReaderComponent.process(inputStream, globalAttributes);
			// add all success attributes
		} catch (final Exception exc) {
			// add all failure attributes
		}
		feedCompletionProcessor.process(globalAttributes, completionAttributes);
	}

	private void processInputStream(final Exchange exchange, final InputStream inputStream, final String fullFilePath, final Long lastModified,
			final Long fileLength) {
		if (inputStream != null) {
			final long start = System.currentTimeMillis();
			log.debug("Received filePath={}, lastModified={}, fileLength={}", fullFilePath, lastModified, fileLength);
			final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(exchange);
			if (feedCompletionProcessor == null) {
				textFileReaderComponent.process(inputStream, globalAttributes);
			} else {
				this.processStreamWithCompletion(inputStream, globalAttributes);
			}
			IOUtils.closeQuietly(inputStream);
			final long total = System.currentTimeMillis() - start;
			if (inputFeedsProcessed != null) {
				inputFeedsProcessed.mark();
			}
			if (inputFeedProcessingTime != null) {
				inputFeedProcessingTime.update(total);
			}
			log.debug("Successfully processed [{}] in {}ms", fullFilePath, total);
		} else {
			log.warn("Stream is null - unable to process file");
		}
	}

	private String getOutputFilePath(final String inputFileName) {
		log.debug("Will use {} file extension for bulk files", fileExtension);
		return config.getBulkOutputDirectory() + "/" + factFeed.getName() + "_" + StringUtil.getFileNameWithoutExtension(inputFileName) + "."
				+ fileExtension;
	}

	private Map<String, String> createImplicitGlobalAttributes(final Exchange exchange) {
		final Map<String, String> attributes = new HashMap<String, String>();
		final Message exchangeIn = exchange.getIn();
		final String fileNameOnly = (String) exchangeIn.getHeader("CamelFileNameOnly");
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME, fileNameOnly);
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH, (String) exchangeIn.getHeader("CamelFileAbsolutePath"));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP,
				String.valueOf(exchangeIn.getHeader("CamelFileLastModified")));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE, String.valueOf(exchangeIn.getHeader("CamelFileLength")));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSED_TIMESTAMP, "" + System.currentTimeMillis());
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH, this.getOutputFilePath(fileNameOnly));
		log.debug("Created global attributes {}", attributes);
		return attributes;
	}

}