package com.threeglav.sh.bauk.feed.processing;

import java.util.Map;

import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.feed.BulkOutputValuesResolver;
import com.threeglav.sh.bauk.feed.FeedParserComponent;
import com.threeglav.sh.bauk.feed.bulk.writer.FileBulkOutputWriter;
import com.threeglav.sh.bauk.feed.bulk.writer.GzipFileBulkOutputWriter;
import com.threeglav.sh.bauk.feed.bulk.writer.JdbcBulkOutputWriter;
import com.threeglav.sh.bauk.feed.bulk.writer.NullBulkOutputWriter;
import com.threeglav.sh.bauk.feed.bulk.writer.ZipFileBulkOutputWriter;
import com.threeglav.sh.bauk.io.BulkOutputWriter;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BulkLoadDefinition;
import com.threeglav.sh.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.CacheUtil;

public abstract class AbstractFeedDataProcessor extends ConfigAware implements FeedDataProcessor {

	protected final BulkOutputWriter bulkOutputWriter;
	protected final BulkOutputValuesResolver bulkoutputResolver;
	protected final FeedParserComponent feedParserComponent;

	public AbstractFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final BulkLoadDefinition bld = factFeed.getBulkLoadDefinition();
		if (bld == null) {
			throw new IllegalArgumentException("Was not able to find bulk load definition for feed " + factFeed.getName());
		}
		final String outputType = bld.getOutputType();
		if (BulkLoadDefinitionOutputType.FILE.toString().equalsIgnoreCase(outputType)) {
			log.info("Will output bulk output results for feed {} to file", factFeed.getName());
			bulkOutputWriter = new FileBulkOutputWriter(factFeed, config);
		} else if (BulkLoadDefinitionOutputType.NONE.toString().equalsIgnoreCase(outputType)) {
			log.info("Will not output any bulk output results for feed {}", factFeed.getName());
			bulkOutputWriter = new NullBulkOutputWriter();
		} else if (BulkLoadDefinitionOutputType.ZIP.toString().equalsIgnoreCase(outputType)) {
			log.info("Will output bulk output results for feed {} to file using ZIP compression", factFeed.getName());
			bulkOutputWriter = new ZipFileBulkOutputWriter(factFeed, config);
		} else if (BulkLoadDefinitionOutputType.GZ.toString().equalsIgnoreCase(outputType)) {
			log.info("Will output bulk output results for feed {} to file using GZ compression", factFeed.getName());
			bulkOutputWriter = new GzipFileBulkOutputWriter(factFeed, config);
		} else if (BulkLoadDefinitionOutputType.JDBC.toString().equalsIgnoreCase(outputType)) {
			log.info("Will output bulk output results for feed {} to file using JDBC batch inserts", factFeed.getName());
			bulkOutputWriter = new JdbcBulkOutputWriter(factFeed, config);
		} else {
			log.info("Was not able to find built-in bulk output writer for {}. Will try to find it as user-defind plugin", outputType);
			try {
				bulkOutputWriter = BaukUtil.loadWriterByProtocol(outputType);
			} catch (final Exception exc) {
				log.error("Exception while loading bulk output writer for protocol {} from plugins. Details {}", outputType, exc.getMessage());
				log.error("Exception", exc);
				throw new IllegalStateException("Was not able to find bulk output writer type " + outputType
						+ " in built-in or user-defined bulk output writers. Please check your configuration and packaging.");
			}
		}
		bulkoutputResolver = new BulkOutputValuesResolver(factFeed, config, CacheUtil.getCacheInstanceManager());
		feedParserComponent = new FeedParserComponent(factFeed, config);
	}

	@Override
	public void startFeed(final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Starting new feed with global attributes {}", globalAttributes);
		}
		bulkoutputResolver.startFeed(globalAttributes);
		bulkOutputWriter.startWriting(globalAttributes);
	}

	@Override
	public void closeFeed(final int expectedResults, final Map<String, String> globalAttributes, final boolean success) {
		bulkOutputWriter.closeResourcesAfterWriting(globalAttributes, success);
		bulkoutputResolver.closeCurrentFeed();
		if (isDebugEnabled) {
			log.debug("Closed feed.");
		}
	}

}
