package com.threeglav.bauk.remoting;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

public class FlushDimensionCacheHandler extends AbstractHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public void handle(final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {
		final String dimensionName = request.getParameter("dimension");
		log.debug("Got request to flush cache for dimension {}", dimensionName);
		response.setContentType("text/html;charset=utf-8");
		baseRequest.setHandled(true);
		if (StringUtil.isEmpty(dimensionName)) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			response.getWriter().println("Dimension not specified!");
		} else {
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().println("Will flush cache for dimension " + dimensionName + " as soon as all current processing is finished!");
		}
	}

}
