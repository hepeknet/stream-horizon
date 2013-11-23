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
import com.threeglav.bauk.model.OnBulkLoadSuccess;
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
		dbHandler = new SpringJdbcDbHandler(this.config);
		successfullyLoadedBulkFilesMeter = MetricsUtil.createMeter("Successfully loaded bulk files");
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(exchange);
		final String bulkLoadFilePath = globalAttributes.get(Constants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		log.debug("Processing {}", bulkLoadFilePath);
		final String insertStatement = factFeed.getBulkLoadDefinition().getBulkLoadInsertStatement();
		if (StringUtil.isEmpty(insertStatement)) {
			log.info("Could not find insert statement for bulk loading files!");
			return;
		}
		log.debug("Insert statement for bulk loading files is {}", insertStatement);
		final String replacedStatement = StringUtil.replaceAllAttributes(insertStatement, globalAttributes, Constants.GLOBAL_ATTRIBUTE_PREFIX);
		log.debug("Statement to execute is {}", replacedStatement);
		dbHandler.executeInsertOrUpdateStatement(replacedStatement);
		log.debug("Successfully executed statement {}", replacedStatement);
		this.executeOnSuccessBulkLoad(globalAttributes);
		if (successfullyLoadedBulkFilesMeter != null) {
			successfullyLoadedBulkFilesMeter.mark();
		}
	}

	private void executeOnSuccessBulkLoad(final Map<String, String> globalAttributes) {
		final OnBulkLoadSuccess onBulkLoadSuccess = factFeed.getBulkLoadDefinition().getOnBulkLoadSuccess();
		if (onBulkLoadSuccess != null) {
			if (onBulkLoadSuccess.getSqlStatements() != null) {
				log.debug("Will execute on success sql actions. Global attributes {}", globalAttributes);
				for (final String sqlStatement : onBulkLoadSuccess.getSqlStatements()) {
					final String replaced = StringUtil.replaceAllAttributes(sqlStatement, globalAttributes, Constants.GLOBAL_ATTRIBUTE_PREFIX);
					log.debug("Trying to execute statement [{}]", replaced);
					dbHandler.executeInsertOrUpdateStatement(replaced);
					log.debug("Successfully finished execution of [{}]", replaced);
				}
			}
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
		log.debug("Created global attributes {}", attributes);
		return attributes;
	}

}
