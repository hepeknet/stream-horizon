package com.threeglav.bauk.remoting;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.EngineRegistry;
import com.threeglav.bauk.events.EngineEvents;
import com.threeglav.bauk.util.StringUtil;

public class FlushDimensionCacheRemoteHandler extends AbstractHandler {

	private static final int MAX_RETRY_COUNT = 10;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public void handle(final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {
		final String dimensionName = request.getParameter("dimension");
		log.debug("Got request to flush cache for dimension {}", dimensionName);
		response.setContentType("text/plain;charset=utf-8");
		baseRequest.setHandled(true);
		if (StringUtil.isEmpty(dimensionName)) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			response.getWriter().println("Dimension not specified!");
		} else {
			final PrintWriter writer = response.getWriter();
			response.setStatus(HttpServletResponse.SC_OK);
			final long start = System.currentTimeMillis();
			final boolean success = this.pauseProcessingFlushAndContinue(dimensionName, writer);
			if (success) {
				response.setStatus(HttpServletResponse.SC_OK);
			} else {
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			}
			if (success) {
				final long total = System.currentTimeMillis() - start;
				writer.println("Paused processing, flushed cache for dimension " + dimensionName
						+ " and instructed processors to continue processing. All executed in " + total + "ms");
			}
		}
	}

	private boolean pauseProcessingFlushAndContinue(final String dimensionName, final PrintWriter pw) {
		EngineEvents.notifyPauseProcessing();
		pw.println("Asked processors to pause processing");
		log.debug("Requested pausing of processing...");
		int currentJobsInProgress = EngineRegistry.CURRENT_IN_PROGRESS_JOBS.get();
		log.debug("Currently in progress have {} jobs", currentJobsInProgress);
		int count = 0;
		while (currentJobsInProgress > 0) {
			if (count > MAX_RETRY_COUNT) {
				pw.println("Could not stop processing in " + count + " attempts. Unable to flush dimension cache.");
				EngineEvents.notifyContinueProcessing();
				return false;
			}
			pw.println("Have " + currentJobsInProgress + " jobs in progress. Have to wait for them to finish...");
			try {
				Thread.sleep(1000);
				count++;
			} catch (final InterruptedException e) {
				// ignore
			}
			currentJobsInProgress = EngineRegistry.CURRENT_IN_PROGRESS_JOBS.get();
		}
		log.debug("Ok to do dimension flush. Currently have {} jobs in progress...", EngineRegistry.CURRENT_IN_PROGRESS_JOBS.get());
		pw.println("All current jobs finished. Flushing caches for dimension " + dimensionName);
		EngineEvents.notifyFlushDimensionCache(dimensionName);
		log.debug("Finished flushing caches for dimension {}", dimensionName);
		pw.println("Caches for dimension " + dimensionName + " have been cleared! Continue processing again...");
		EngineEvents.notifyContinueProcessing();
		log.debug("Notified all processors to continue processing...");
		return true;
	}

}
