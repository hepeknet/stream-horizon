package com.threeglav.sh.bauk.files.feed;

import java.util.ArrayList;

import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.FeedSource;
import com.threeglav.sh.bauk.rpc.SHFeedProcessor;
import com.threeglav.sh.bauk.rpc.ThriftSHFeedProcessorImpl;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;

public class ThriftFeedHandler extends AbstractFeedHandler {

	private final int portNumber;

	public ThriftFeedHandler(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final ArrayList<BaukProperty> properties = factFeed.getSource().getProperties();
		final String configuredPortNumber = BaukPropertyUtil.getRequiredUniqueProperty(properties,
				FeedSource.RPC_FEED_SOURCE_SERVER_PORT_PROPERTY_NAME).getName();
		portNumber = Integer.valueOf(configuredPortNumber);
	}

	@Override
	public void init() {
		try {
			log.info("Starting non-blocking thrift server on port {}", portNumber);
			final TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(portNumber);
			final SHFeedProcessor.Processor processor = new SHFeedProcessor.Processor(new ThriftSHFeedProcessorImpl());
			final TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
			server.serve();
			log.debug("Successfully started thrift server on port {}", portNumber);
		} catch (final TTransportException e) {
			log.error("Error while starting thrift server on port {}. Details {}", portNumber, e.getMessage());
			log.error("Exception ", e);
			System.exit(-1);
		}
	}

}
