package com.threeglav.bauk.remoting;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.util.BaukThreadFactory;

public class RemotingHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final ExecutorService exec = Executors.newSingleThreadExecutor(new BaukThreadFactory("bauk-remoting-threads", "bauk-remoting-thread"));

	private Server server;

	public void start() {
		final int port = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.REMOTING_SERVER_PORT_PARAM_NAME,
				SystemConfigurationConstants.REMOTING_SERVER_PORT_DEFAULT);
		if (port <= 0) {
			return;
		}
		log.debug("Starting jetty server on port {}", port);
		server = new Server(port);
		this.addHandlers();
		exec.submit(new Runnable() {
			@Override
			public void run() {
				try {
					server.start();
					server.join();
				} catch (final Exception exc) {
					throw new RuntimeException("Exception while starting jetty server", exc);
				}
			}
		});
	}

	private void addHandlers() {
		final ContextHandler context = new ContextHandler();
		context.setContextPath("/flushDimensionCache");
		context.setResourceBase(".");
		server.setHandler(context);
		context.setHandler(new FlushDimensionCacheHandler());
	}

	public void stop() {
		log.debug("Stopping server...");
		exec.shutdown();
		if (server == null) {
			return;
		}
		try {
			server.stop();
		} catch (final Exception exc) {
			log.error("Exception while shutting down jetty server", exc);
		}
	}

}
