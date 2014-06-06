package com.threeglav.sh.bauk.rpc;

import java.io.IOException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.files.feed.FeedFileProcessor;

public class ThriftSHFeedProcessorImpl implements SHFeedProcessor.Iface {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FeedFileProcessor feedProcessor;

	public ThriftSHFeedProcessorImpl(final FeedFileProcessor ffp) {
		if (ffp == null) {
			throw new IllegalArgumentException("Feed processor must not be null");
		}
		feedProcessor = ffp;
	}

	@Override
	public ProcessingResult processFeed(final InputFeed feedToProcess) throws TException {
		RPCBaukFile bf;
		try {
			bf = new RPCBaukFile(feedToProcess);
		} catch (final Exception exc) {
			log.error("Unable to understand feed provided via RPC", exc);
			return ProcessingResult.INVALID_FEED;
		}
		try {
			feedProcessor.process(bf);
			return ProcessingResult.SUCCESS;
		} catch (final IOException e) {
			return ProcessingResult.PROCESSING_ERROR;
		}
	}

}
