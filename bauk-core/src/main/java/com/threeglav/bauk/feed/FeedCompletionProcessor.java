package com.threeglav.bauk.feed;

import java.util.Map;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class FeedCompletionProcessor extends ConfigAware {

	private final String statementDescription;

	public FeedCompletionProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		statementDescription = "FeedCompletionProcessor for " + this.getFactFeed().getName();
	}

	public void process(final Map<String, String> globalAttributes) {
		log.debug("Global attributes {}", globalAttributes);
		for (final String sqlStatement : this.getFactFeed().getAfterFeedProcessingCompletion()) {
			log.debug("About to execute on-completion statement {} for feed {}. First will prepare it...", sqlStatement, this.getFactFeed().getName());
			String stat = sqlStatement;
			stat = StringUtil.replaceAllAttributes(stat, globalAttributes, this.getConfig().getDatabaseStringLiteral(), this.getConfig()
					.getDatabaseStringEscapeLiteral());
			log.debug("Executing on-completions statement {}", stat);
			this.getDbHandler().executeInsertOrUpdateStatement(stat, statementDescription);
			log.debug("Successfully executed on-completion statement {}", stat);
		}
	}

}
