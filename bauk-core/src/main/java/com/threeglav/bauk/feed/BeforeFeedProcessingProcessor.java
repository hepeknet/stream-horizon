package com.threeglav.bauk.feed;

import java.util.HashMap;
import java.util.Map;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.MappedResultsSQLStatement;
import com.threeglav.bauk.model.SqlStatementType;
import com.threeglav.bauk.util.StringUtil;

public class BeforeFeedProcessingProcessor extends ConfigAware {

	private final String statementDescription;
	private final DbHandler databaseHandler;

	public BeforeFeedProcessingProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		statementDescription = "BeforeFeedProcessor for feed " + this.getFactFeed().getName();
		databaseHandler = this.getDbHandler();
	}

	public void processAndGenerateNewAttributes(final Map<String, String> globalAttributes) {
		if (isTraceEnabled) {
			log.trace("Executing before-feed-processing. Global attributes {}", globalAttributes);
		}
		for (final MappedResultsSQLStatement mrss : this.getFactFeed().getBeforeFeedProcessing()) {
			final Map<String, String> attrs = this.processMappedStatement(mrss, globalAttributes);
			log.debug("After executing {} got results {}. Will merge them with global attributes", mrss.getSqlStatement(), attrs);
			if (attrs != null) {
				globalAttributes.putAll(attrs);
			}
		}
		if (isDebugEnabled) {
			log.debug("After before-feed-processing global attributes are {}", globalAttributes);
		}
	}

	private Map<String, String> processMappedStatement(final MappedResultsSQLStatement mrss, final Map<String, String> globalAttrs) {
		final String statement = StringUtil.replaceAllAttributes(mrss.getSqlStatement(), globalAttrs, this.getConfig().getDatabaseStringLiteral(),
				this.getConfig().getDatabaseStringEscapeLiteral());
		if (isDebugEnabled) {
			log.debug("Statement to execute is {}", statement);
		}
		if (mrss.getType() == SqlStatementType.SELECT) {
			return databaseHandler.executeSelectStatement(statement, statementDescription);
		} else if (mrss.getType() == SqlStatementType.INSERT) {
			databaseHandler.executeInsertOrUpdateStatement(statement, statementDescription);
			return null;
		} else if (mrss.getType() == SqlStatementType.INSERT_RETURN_KEY) {
			final Long key = databaseHandler.executeInsertStatementAndReturnKey(statement, statementDescription);
			final Map<String, String> vals = new HashMap<String, String>();
			vals.put("_sk_", String.valueOf(key));
			return vals;
		} else {
			throw new IllegalStateException("Unsupported statement type for " + statementDescription);
		}
	}

}
