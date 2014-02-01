package com.threeglav.bauk.monitor.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.threeglav.bauk.EngineRegistry;

public class MonitoringServlet extends HttpServlet {

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		resp.setContentType("text/plain;charset=utf-8");
		String response = "Input records per second: " + EngineRegistry.getProcessedRowsInTheLastMinute() + ", successfully processed input files "
				+ EngineRegistry.getProcessedFeedFilesCount();
		response += "\n";
		response += "Failed input files " + EngineRegistry.getFailedFeedFilesCount() + ", successully bulk loaded files "
				+ EngineRegistry.getSuccessfulBulkFilesCount() + ", failed bulk files " + EngineRegistry.getFailedBulkFilesCount();
		resp.getWriter().write(response);
	}

}
