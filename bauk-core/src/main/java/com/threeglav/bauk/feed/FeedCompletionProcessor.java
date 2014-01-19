package com.threeglav.bauk.feed;

import java.util.ArrayList;
import java.util.Map;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.command.BaukCommandsExecutor;
import com.threeglav.bauk.model.BaukCommand;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public final class FeedCompletionProcessor extends ConfigAware {

	private final String statementDescription;
	private BaukCommandsExecutor commandExecutor;

	public FeedCompletionProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final ArrayList<BaukCommand> commands = this.getFactFeed().getAfterFeedProcessingCompletion();
		if (commands != null && !commands.isEmpty()) {
			commandExecutor = new BaukCommandsExecutor(factFeed, config);
		}
		statementDescription = "FeedCompletionProcessor for " + this.getFactFeed().getName();
	}

	public void process(final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Global attributes {}", globalAttributes);
		}
		if (commandExecutor != null) {
			commandExecutor.executeBaukCommandSequence(this.getFactFeed().getAfterFeedProcessingCompletion(), globalAttributes, statementDescription);
		}
	}

}
