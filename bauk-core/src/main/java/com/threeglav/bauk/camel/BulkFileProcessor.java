package com.threeglav.bauk.camel;

import gnu.trove.map.hash.THashMap;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.feed.ConfigAware;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.OnBulkLoadSuccess;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileProcessor extends ConfigAware implements Processor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Meter successfullyLoadedBulkFilesMeter;
	private final String dbStringLiteral;

	public BulkFileProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		successfullyLoadedBulkFilesMeter = MetricsUtil.createMeter("Successfully loaded bulk files");
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(exchange);
		final String bulkLoadFilePath = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		log.debug("Processing {}", bulkLoadFilePath);
		final String insertStatement = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsertStatement();
		if (StringUtil.isEmpty(insertStatement)) {
			log.info("Could not find insert statement for bulk loading files!");
			return;
		}
		log.debug("Insert statement for bulk loading files is {}", insertStatement);
		final String replacedStatement = StringUtil.replaceAllAttributes(insertStatement, globalAttributes, dbStringLiteral);
		log.debug("Statement to execute is {}", replacedStatement);
		this.getDbHandler().executeInsertOrUpdateStatement(replacedStatement);
		log.debug("Successfully executed statement {}", replacedStatement);
		this.executeOnSuccessBulkLoad(globalAttributes);
		if (successfullyLoadedBulkFilesMeter != null) {
			successfullyLoadedBulkFilesMeter.mark();
		}
	}

	private void executeOnSuccessBulkLoad(final Map<String, String> globalAttributes) {
		final OnBulkLoadSuccess onBulkLoadSuccess = this.getFactFeed().getBulkLoadDefinition().getOnBulkLoadSuccess();
		if (onBulkLoadSuccess != null) {
			if (onBulkLoadSuccess.getSqlStatements() != null) {
				log.debug("Will execute on-success sql actions. Global attributes {}", globalAttributes);
				for (final String sqlStatement : onBulkLoadSuccess.getSqlStatements()) {
					final String replaced = StringUtil.replaceAllAttributes(sqlStatement, globalAttributes, dbStringLiteral);
					log.debug("Trying to execute statement [{}]", replaced);
					this.getDbHandler().executeInsertOrUpdateStatement(replaced);
					log.debug("Successfully finished execution of [{}]", replaced);
				}
			}
		}
	}

	private Map<String, String> createImplicitGlobalAttributes(final Exchange exchange) {
		final Map<String, String> attributes = new THashMap<String, String>();
		final String fileNameOnly = (String) exchange.getIn().getHeader("CamelFileNameOnly");
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME, fileNameOnly);
		final String inputFileAbsolutePath = (String) exchange.getIn().getHeader("CamelFileAbsolutePath");
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH, StringUtil.fixFilePath(inputFileAbsolutePath));
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_TIMESTAMP,
				String.valueOf(exchange.getIn().getHeader("CamelFileLastModified")));
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_PROCESSED_TIMESTAMP,
				String.valueOf(System.currentTimeMillis()));
		log.debug("Created global attributes {}", attributes);
		return attributes;
	}

}
