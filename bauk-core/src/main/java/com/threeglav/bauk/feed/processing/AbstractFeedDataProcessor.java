package com.threeglav.bauk.feed.processing;

import java.util.Map;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.feed.BulkOutputValuesResolver;
import com.threeglav.bauk.feed.FeedParserComponent;
import com.threeglav.bauk.feed.bulk.writer.BulkOutputWriter;
import com.threeglav.bauk.feed.bulk.writer.FileBulkOutputWriter;
import com.threeglav.bauk.feed.bulk.writer.NIOFileBulkOutputWriter;
import com.threeglav.bauk.feed.bulk.writer.NullBulkOutputWriter;
import com.threeglav.bauk.feed.bulk.writer.ZipFileBulkOutputWriter;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.CacheUtil;

public abstract class AbstractFeedDataProcessor extends ConfigAware implements FeedDataProcessor {

	protected final BulkOutputWriter bulkOutputWriter;
	protected final BulkOutputValuesResolver bulkoutputResolver;
	protected final FeedParserComponent feedParserComponent;
	private final boolean isDebugEnabled;

	public AbstractFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier) {
		super(factFeed, config);
		final BulkLoadDefinition bld = factFeed.getBulkLoadDefinition();
		if (bld == null) {
			throw new IllegalArgumentException("Was not able to find bulk load definition for feed " + factFeed.getName());
		}
		final BulkLoadDefinitionOutputType outputType = bld.getOutputType();
		if (outputType == BulkLoadDefinitionOutputType.FILE) {
			log.info("Will output bulk output results for feed {} to file", factFeed.getName());
			bulkOutputWriter = new FileBulkOutputWriter(factFeed, config);
		} else if (outputType == BulkLoadDefinitionOutputType.NONE) {
			log.info("Will not output any bulk output results for feed {}", factFeed.getName());
			bulkOutputWriter = new NullBulkOutputWriter();
		} else if (outputType == BulkLoadDefinitionOutputType.NIO) {
			log.info("Will output bulk output results for feed {} to file using NIO", factFeed.getName());
			bulkOutputWriter = new NIOFileBulkOutputWriter(factFeed, config);
		} else if (outputType == BulkLoadDefinitionOutputType.ZIP) {
			log.info("Will output bulk output results for feed {} to file using ZIP compression", factFeed.getName());
			bulkOutputWriter = new ZipFileBulkOutputWriter(factFeed, config);
		} else {
			throw new IllegalStateException("Unknown bulk output writer type " + outputType);
		}
		bulkoutputResolver = new BulkOutputValuesResolver(factFeed, config, routeIdentifier, CacheUtil.getCacheInstanceManager());
		feedParserComponent = new FeedParserComponent(factFeed, config, routeIdentifier);
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public void startFeed(final Map<String, String> globalAttributes) {
		if (globalAttributes == null) {
			throw new IllegalArgumentException("Global attributes must not be null");
		}
		if (isDebugEnabled) {
			log.debug("Starting new feed with global attributes {}", globalAttributes);
		}
		bulkoutputResolver.startFeed(globalAttributes);
		bulkOutputWriter.initialize(globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH));
	}

	@Override
	public void closeFeed(final int expectedResults, final Map<String, String> globalAttributes) {
		bulkOutputWriter.closeResources(globalAttributes);
		bulkoutputResolver.closeCurrentFeed();
		if (isDebugEnabled) {
			log.debug("Closed feed. Expected results {}", expectedResults);
		}
	}

}
