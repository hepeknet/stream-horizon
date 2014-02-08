package com.threeglav.bauk.remoting;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.remoting.handlers.FlushDimensionCacheRemoteHandler;
import com.threeglav.bauk.remoting.handlers.MonitoringHandler;
import com.threeglav.bauk.util.BaukThreadFactory;

public class RemotingServer {

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
		final QueuedThreadPool p = (QueuedThreadPool) server.getThreadPool();
		p.setMaxThreads(2);
		this.addHandlers();
		exec.submit(new Runnable() {
			@Override
			public void run() {
				try {
					server.start();
					server.join();
				} catch (final Exception exc) {
					log.error("Exception while starting jetty server", exc);
					throw new RuntimeException("Exception while starting jetty server", exc);
				}
			}
		});
	}

	private void addHandlers() {
		final ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setDirectoriesListed(true);
		resourceHandler.setWelcomeFiles(new String[] { "index.html" });
		resourceHandler.setResourceBase(".");

		final ContextHandler staticCtxHandler = new ContextHandler();
		staticCtxHandler.setContextPath("/");
		staticCtxHandler.setResourceBase(ConfigurationProperties.getWebAppsFolder());
		staticCtxHandler.setHandler(new ResourceHandler());

		final ContextHandler flushDimensionCacheCtx = new ContextHandler();
		flushDimensionCacheCtx.setContextPath("/flushDimensionCache");
		flushDimensionCacheCtx.setHandler(new FlushDimensionCacheRemoteHandler());

		final ContextHandler monitoringCtx = new ContextHandler();
		monitoringCtx.setContextPath("/monitor");
		monitoringCtx.setHandler(new MonitoringHandler());

		final ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.addHandler(staticCtxHandler);
		contexts.addHandler(flushDimensionCacheCtx);
		contexts.addHandler(monitoringCtx);

		server.setHandler(contexts);

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
