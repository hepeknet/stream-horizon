package com.threeglav.bauk.remoting.handlers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.threeglav.bauk.EngineRegistry;

public class MonitoringHandler extends AbstractHandler {

	@Override
	public void handle(final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {
		response.setContentType("text/plain;charset=utf-8");
		baseRequest.setHandled(true);
		String responseMsg = "Input records per second: " + EngineRegistry.getProcessedRowsInTheLastMinute()
				+ ", successfully processed input files " + EngineRegistry.getProcessedFeedFilesCount();
		responseMsg += "\n";
		responseMsg += "Failed input files " + EngineRegistry.getFailedFeedFilesCount() + ", successully bulk loaded files "
				+ EngineRegistry.getSuccessfulBulkFilesCount() + ", failed bulk files " + EngineRegistry.getFailedBulkFilesCount();
		response.getWriter().println(responseMsg);
	}

}
