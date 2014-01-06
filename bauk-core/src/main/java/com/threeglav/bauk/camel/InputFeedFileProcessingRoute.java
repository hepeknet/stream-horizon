package com.threeglav.bauk.camel;

import java.util.Random;

import org.apache.camel.ShutdownRunningTask;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.impl.PropertyPlaceholderDelegateRegistry;
import org.apache.camel.model.TryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.ThreadPoolSizes;

public class InputFeedFileProcessingRoute extends RouteBuilder {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private int feedProcessingThreads = ThreadPoolSizes.THREAD_POOL_DEFAULT_SIZE;

	public InputFeedFileProcessingRoute(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		this.validate();
		if (this.factFeed.getThreadPoolSizes() != null) {
			feedProcessingThreads = factFeed.getThreadPoolSizes().getFeedProcessingThreads();
		}
		log.debug("Will use {} threads to process incoming files for {}", feedProcessingThreads, factFeed.getName());
	}

	private void validate() {
		if (factFeed.getFileNameMasks() == null || factFeed.getFileNameMasks().isEmpty()) {
			throw new IllegalArgumentException("Could not find any file masks for " + factFeed.getName() + ". Check your configuration!");
		}
	}

	@Override
	public void configure() throws Exception {
		this.createRouteForAllFileMasks();
	}

	private void bindBean(final String name, final Object bean) {
		final JndiRegistry registry = (JndiRegistry) ((PropertyPlaceholderDelegateRegistry) this.getContext().getRegistry()).getRegistry();
		registry.bind(name, bean);
	}

	private void createRouteForAllFileMasks() {
		for (final String fileMask : factFeed.getFileNameMasks()) {
			log.debug("Creating {} routes for {}", feedProcessingThreads, fileMask);
			for (int i = 0; i < feedProcessingThreads; i++) {
				final FeedFileProcessor feedFileProcessor = new FeedFileProcessor(factFeed, config, fileMask);
				this.createRoute(fileMask, feedFileProcessor, i, feedProcessingThreads);
				log.debug("Created route #{} for {}", i, fileMask);
			}
			log.debug("Created in total {} routes for processing {}", feedProcessingThreads, fileMask);
		}
	}

	private void createRoute(final String fileMask, final FeedFileProcessor feedFileProcessor, final int routeId, final int totalNumber) {
		final int delay = 1000 + routeId * 100;
		final HashedNameFileFilter hnff = new HashedNameFileFilter<>(fileMask, routeId, totalNumber);
		final Random rand = new Random();
		final String filterName = "bauk_filter_" + routeId + "_" + rand.nextInt(100000);
		this.bindBean(filterName, hnff);
		String inputEndpoint = "file://" + config.getSourceDirectory() + "?include=" + fileMask + "&delete=true";
		final boolean isIdempotentFeedProcessing = ConfigurationProperties.getSystemProperty(
				SystemConfigurationConstants.IDEMPOTENT_FEED_PROCESSING_PARAM_NAME, false);
		final boolean renameArchivedFiles = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.RENAME_ARCHIVED_FILES_PARAM_NAME,
				false);
		if (isIdempotentFeedProcessing) {
			inputEndpoint += "&idempotent=true";
		}
		inputEndpoint += "&readLock=changed&initialDelay=" + delay + "&filter=#" + filterName;
		log.debug("Input endpoint is {}", inputEndpoint);
		final TryDefinition td = this.from(inputEndpoint).shutdownRunningTask(ShutdownRunningTask.CompleteCurrentTaskOnly)
				.routeId("InputFeedProcessing (" + fileMask + ")_" + routeId).doTry().process(feedFileProcessor);
		if (renameArchivedFiles) {
			td.setHeader("CamelFileName", this.simple("${file:name.noext}-${date:now:yyyy_MM_dd_HHmmssSSS}.${file:ext}"));
		}
		td.to("file://" + config.getArchiveDirectory() + "/").doCatch(Exception.class).to("file://" + config.getErrorDirectory()).transform()
				.simple("${exception.stacktrace}")
				.setHeader("CamelFileName", this.simple("${file:name.noext}-${date:now:yyyy_MM_dd_HH_mm_ss_SSS}_inputFeed.fail"))
				.to("file://" + config.getErrorDirectory() + "/").end();
	}
}
