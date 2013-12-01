package com.threeglav.bauk.feed;

import java.util.Map;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public abstract class AbstractFeedDataProcessor extends ConfigAware implements FeedDataProcessor {

	protected final BulkFileWriter bulkWriter;
	protected final BulkOutputValuesResolver bulkoutputResolver;
	protected final FeedParserComponent feedParserComponent;

	protected Map<String, String> globalAttributes;
	protected Map<String, String> headerAttributes;

	public AbstractFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier) {
		super(factFeed, config);
		bulkWriter = new BulkFileWriter(factFeed, config);
		bulkoutputResolver = new BulkOutputValuesResolver(factFeed, config, routeIdentifier, new HazelcastCacheInstanceManager());
		feedParserComponent = new FeedParserComponent(factFeed, config, routeIdentifier);
	}

	@Override
	public void startFeed(final Map<String, String> globalAttributes, final Map<String, String> headerAttributes) {
		this.globalAttributes = globalAttributes;
		this.headerAttributes = headerAttributes;
		bulkWriter.startWriting(globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH));
	}

	@Override
	public void closeFeed(final int expectedResults) {
		bulkWriter.closeResources();
		bulkoutputResolver.closeCurrentFeed();
	}

}
