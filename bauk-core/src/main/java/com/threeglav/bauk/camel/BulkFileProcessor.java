package com.threeglav.bauk.camel;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.dimension.db.SpringJdbcDbHandler;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileProcessor implements Processor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final FactFeed factFeed;
	private final Config config;
	private final DbHandler dbHandler;
	private final Meter successfullyLoadedBulkFilesMeter;

	public BulkFileProcessor(final FactFeed factFeed, final Config config) {
		this.factFeed = factFeed;
		this.config = config;
		this.dbHandler = new SpringJdbcDbHandler(this.config);
		this.successfullyLoadedBulkFilesMeter = MetricsUtil.createMeter("Successfully loaded bulk files");
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(exchange);
		final String bulkLoadFilePath = globalAttributes.get(Constants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		this.log.debug("Processing {}", bulkLoadFilePath);
		final String insertStatement = this.factFeed.getBulkDefinition().getBulkLoadInsertStatement();
		if (StringUtil.isEmpty(insertStatement)) {
			this.log.info("Could not find insert statement for bulk loading files!");
			return;
		}
		this.log.debug("Insert statement for bulk loading files is {}", insertStatement);
		final String replacedStatement = StringUtil.replaceAllAttributes(insertStatement, globalAttributes, Constants.GLOBAL_ATTRIBUTE_PREFIX);
		this.log.debug("Statement to execute is {}", replacedStatement);
		this.dbHandler.executeInsertStatement(replacedStatement);
		this.log.debug("Successfully executed statement {}", replacedStatement);
		if (this.successfullyLoadedBulkFilesMeter != null) {
			this.successfullyLoadedBulkFilesMeter.mark();
		}
	}

	private Map<String, String> createImplicitGlobalAttributes(final Exchange exchange) {
		final Map<String, String> attributes = new HashMap<String, String>();
		final String fileNameOnly = (String) exchange.getIn().getHeader("CamelFileNameOnly");
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME, fileNameOnly);
		final String inputFileAbsolutePath = (String) exchange.getIn().getHeader("CamelFileAbsolutePath");
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH, StringUtil.fixFilePath(inputFileAbsolutePath));
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_FILE_BULK_FILE_RECEIVED_TIMESTAMP,
				String.valueOf(exchange.getIn().getHeader("CamelFileLastModified")));
		attributes.put(com.threeglav.bauk.Constants.IMPLICIT_ATTRIBUTE_FILE_BULK_FILE_PROCESSED_TIMESTAMP, "" + System.currentTimeMillis());
		this.log.debug("Created global attributes {}", attributes);
		return attributes;
	}

}
