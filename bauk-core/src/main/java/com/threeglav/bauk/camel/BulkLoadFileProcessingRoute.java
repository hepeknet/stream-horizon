package com.threeglav.bauk.camel;

import org.apache.camel.ShutdownRunningTask;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.ThreadPoolSizes;

public class BulkLoadFileProcessingRoute extends RouteBuilder {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private int bulkProcessingThreads = ThreadPoolSizes.THREAD_POOL_DEFAULT_SIZE;
	private final MoveFileProcessor moveToErrorFileProcessor;

	public BulkLoadFileProcessingRoute(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		if (factFeed.getThreadPoolSizes() != null) {
			bulkProcessingThreads = factFeed.getThreadPoolSizes().getBulkLoadProcessingThreads();
		}
		log.debug("Will use {} threads to process bulk load files for {}", bulkProcessingThreads, factFeed.getName());
		if (bulkProcessingThreads <= 0) {
			log.info("For feed {} bulk processing set to use non-positive number of threads. Will not be started!", factFeed.getName());
		}
		this.validate();
		moveToErrorFileProcessor = new MoveFileProcessor(config.getErrorDirectory());
	}

	private void validate() {
		if (factFeed.getBulkLoadDefinition() == null && bulkProcessingThreads > 0) {
			throw new IllegalStateException(
					"Was not able to find bulk definition in configuration file but bulk processing threads set to positive value!");
		}
	}

	@Override
	public void configure() throws Exception {
		for (int i = 0; i < bulkProcessingThreads; i++) {
			this.createRoute(i);
		}
		log.debug("Created in total {} bulk processing routes", bulkProcessingThreads);
	}

	private void createRoute(final int routeId) {
		final int initialRouteDelayMillis = 500 + routeId * 100;
		final String fullFileMask = ".*" + factFeed.getBulkLoadDefinition().getBulkLoadOutputExtension();
		log.debug("Will process bulk files in {} with file mask {}", config.getBulkOutputDirectory(), fullFileMask);
		String inputEndpoint = "file://" + config.getBulkOutputDirectory() + "?include=" + fullFileMask + "&initialDelay=" + initialRouteDelayMillis;
		inputEndpoint += "&idempotent=true&readLock=changed&delete=true";
		log.debug("Input endpoint is {}", inputEndpoint);
		this.from(inputEndpoint).routeId("BulkLoadFileProcessing_" + routeId).shutdownRunningTask(ShutdownRunningTask.CompleteCurrentTaskOnly)
				.doTry().process(new BulkFileProcessor(factFeed, config)).doCatch(Exception.class)
				.setHeader("originalFilePath", this.simple("${file:absolute.path}")).process(moveToErrorFileProcessor).end();
	}

	public boolean shouldStartRoute() {
		return bulkProcessingThreads > 0;
	}

}
