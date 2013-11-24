package com.threeglav.bauk.camel;

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

	public BulkLoadFileProcessingRoute(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		this.validate();
		if (factFeed.getThreadPoolSizes() != null) {
			bulkProcessingThreads = factFeed.getThreadPoolSizes().getBulkLoadProcessingThreads();
		}
		log.debug("Will use {} threads to process bulk load files for {}", bulkProcessingThreads, factFeed.getName());
		if (bulkProcessingThreads <= 0) {
			log.info("Bulk processing set to use non-positive number of threads. Will not be started!");
		}
	}

	private void validate() {
		if (factFeed.getBulkLoadDefinition() == null) {
			throw new IllegalStateException("Was not able to find bulk definition in configuration file!");
		}
	}

	@Override
	public void configure() throws Exception {
		this.createRoute();
	}

	private void createRoute() {
		final String fullFileMask = ".*" + factFeed.getBulkLoadDefinition().getBulkLoadOutputExtension();
		log.debug("Will process bulk files in {} with file mask {}", config.getBulkOutputDirectory(), fullFileMask);
		String inputEndpoint = "file://" + config.getBulkOutputDirectory() + "?include=" + fullFileMask;
		inputEndpoint += "&idempotent=true&readLock=changed&delete=true";
		log.debug("Input endpoint is {}", inputEndpoint);
		this.from(inputEndpoint).routeId("BulkLoadFileProcessing").threads(bulkProcessingThreads).doTry()
				.process(new BulkFileProcessor(factFeed, config)).doCatch(Exception.class).to("file://" + config.getErrorDirectory()).transform()
				.simple("${exception.stacktrace}")
				.setHeader("CamelFileName", this.simple("${file:name.noext}-${date:now:yyyy_MM_dd_HH_mm_ss_SSS}_bulkLoad.fail"))
				.to("file://" + config.getBulkOutputDirectory() + "/").end();
	}

	public boolean shouldStartRoute() {
		return bulkProcessingThreads > 0;
	}

}
