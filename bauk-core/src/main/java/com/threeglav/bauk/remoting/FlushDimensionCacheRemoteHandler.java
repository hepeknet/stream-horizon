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
				writer.println("SUMMARY: Paused processing, flushed cache for dimension " + dimensionName
						+ " and then instructed processing threads to continue processing. All executed in " + total + "ms");
			}
		}
	}

	private boolean pauseProcessingFlushAndContinue(final String dimensionName, final PrintWriter pw) {
		pw.println("Trying to flush caches for dimension " + dimensionName + ". Asking all processing threads to pause processing");
		final boolean allThreadsPaused = EngineRegistry.beginProcessingPause();
		if (allThreadsPaused) {
			log.debug("Requested pausing of processing...");
			pw.println("All current processing threads paused their work. Flushing caches for dimension " + dimensionName);
			EngineEvents.notifyFlushDimensionCache(dimensionName);
			log.debug("Finished flushing caches for dimension {}", dimensionName);
			pw.println("Caches for dimension " + dimensionName + " have been cleared! Continued processing again...");
			log.debug("Notified all processors to continue processing...");
			EngineRegistry.endProcessingPause();
			return true;
		} else {
			pw.println("Could not pause all processing threads withing 10 seconds. Unable to flush dimension cache!");
			return false;
		}
	}

}
