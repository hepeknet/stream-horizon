package com.threeglav.sh.bauk.files.feed;

import java.util.ArrayList;

import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedSource;
import com.threeglav.sh.bauk.rpc.SHFeedProcessor;
import com.threeglav.sh.bauk.rpc.ThriftSHFeedProcessorImpl;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;

public class ThriftFeedHandler extends AbstractFeedHandler {

	private final int portNumber;
	private TServer server;

	public ThriftFeedHandler(final Feed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final ArrayList<BaukProperty> properties = factFeed.getSource().getProperties();
		final String configuredPortNumber = BaukPropertyUtil.getRequiredUniqueProperty(properties,
				FeedSource.RPC_FEED_SOURCE_SERVER_PORT_PROPERTY_NAME).getValue();
		portNumber = Integer.valueOf(configuredPortNumber);
		this.initializeServer();
	}

	private void initializeServer() {
		try {
			log.info("Starting non-blocking thrift server on port {}", portNumber);
			final TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(portNumber);
			final FeedFileProcessor ffp = new FeedFileProcessor(factFeed, config, "rpc-feed");
			final SHFeedProcessor.Processor processor = new SHFeedProcessor.Processor(new ThriftSHFeedProcessorImpl(ffp));
			final TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
			server.serve();
			log.info("Successfully started StreamHorizon thrift server listening on port {}", portNumber);
		} catch (final TTransportException e) {
			log.error("Error while starting thrift server on port {}. Details {}", portNumber, e.getMessage());
			log.error("Exception ", e);
			System.exit(-1);
		}
	}

	@Override
	public void init() {

	}

	@Override
	public int start() {
		return 1;
	}

	@Override
	public void stop() {
		if (server != null) {
			server.stop();
			log.info("Stopped thrift server listening on port {}", portNumber);
		}
	}

}
