package com.threeglav.bauk.camel;

import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.ThreadPoolSizes;

public class InputFeedFileProcessingRoute extends RouteBuilder {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final Config config;
	private int feedProcessingThreads = ThreadPoolSizes.THREAD_POOL_DEFAULT_SIZE;

	public InputFeedFileProcessingRoute(final FactFeed factFeed, final Config config) {
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

	private void createRouteForAllFileMasks() {
		for (final String fileMask : factFeed.getFileNameMasks()) {
			log.debug("Creating route for {}", fileMask);
			this.createRoute(fileMask);
			log.debug("Created route for {}", fileMask);
		}
	}

	private void createRoute(final String fileMask) {
		String inputEndpoint = "file://" + config.getSourceDirectory() + "?move=" + config.getArchiveDirectory()
				+ "/${file:name.noext}-${date:now:yyyy_MM_dd_HHmmssSSS}.${file:ext}&include=" + fileMask;
		inputEndpoint += "&idempotent=true&readLock=changed";
		log.debug("Input endpoint is {}", inputEndpoint);
		this.from(inputEndpoint).routeId("InputFeedProcessing (" + fileMask + ")").doTry().process(new FeedFileProcessor(factFeed, config, fileMask))
				.doCatch(Exception.class).to("file://" + config.getErrorDirectory()).transform().simple("${exception.stacktrace}")
				.setHeader("CamelFileName", this.simple("${file:name.noext}-${date:now:yyyy_MM_dd_HH_mm_ss_SSS}_inputFeed.fail"))
				.to("file://" + config.getErrorDirectory() + "/").end();
	}
}
