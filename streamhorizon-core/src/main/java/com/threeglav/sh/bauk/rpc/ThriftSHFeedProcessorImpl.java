package com.threeglav.sh.bauk.rpc;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftSHFeedProcessorImpl implements SHFeedProcessor.Iface {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public ProcessingResult processFeed(final InputFeed feedToProcess) throws TException {
		return null;
	}

}
