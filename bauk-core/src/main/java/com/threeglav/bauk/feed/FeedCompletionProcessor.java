package com.threeglav.bauk.feed;

import java.util.Map;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class FeedCompletionProcessor extends ConfigAware {

	public FeedCompletionProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
	}

	public void process(final Map<String, String> globalAttributes, final Map<String, String> completionAttributes) {
		log.debug("Global attributes {}, completion attributes {}", globalAttributes, completionAttributes);
		for (final String sqlStatement : this.getFactFeed().getAfterFeedProcessingCompletion()) {
			log.debug("About to execute on-completion statement {} for feed {}. First will prepare it...", sqlStatement, this.getFactFeed().getName());
			String stat = sqlStatement;
			stat = StringUtil.replaceAllAttributes(stat, globalAttributes, this.getConfig().getDatabaseStringLiteral());
			stat = StringUtil.replaceAllAttributes(stat, completionAttributes, this.getConfig().getDatabaseStringLiteral());
			log.debug("Executing on-completions statement {}", stat);
			this.getDbHandler().executeInsertOrUpdateStatement(stat);
			log.debug("Successfully executed {}", stat);
		}
	}

}
