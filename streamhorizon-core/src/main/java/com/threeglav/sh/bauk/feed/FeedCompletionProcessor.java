package com.threeglav.sh.bauk.feed;

import java.util.ArrayList;
import java.util.Map;

import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.command.BaukCommandsExecutor;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;

public final class FeedCompletionProcessor extends ConfigAware {

	private final String statementDescription;
	private BaukCommandsExecutor commandExecutor;

	public FeedCompletionProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final ArrayList<BaukCommand> commands = this.getFactFeed().getAfterFeedProcessingCompletion();
		if (commands != null && !commands.isEmpty()) {
			commandExecutor = new BaukCommandsExecutor(factFeed, config, this.getFactFeed().getAfterFeedProcessingCompletion());
		}
		statementDescription = "FeedCompletionProcessor for " + this.getFactFeed().getName();
	}

	public void process(final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Executing feed completion logic. Global attributes {}", globalAttributes);
		}
		if (commandExecutor != null) {
			commandExecutor.executeBaukCommandSequence(globalAttributes, statementDescription);
		}
	}

}
