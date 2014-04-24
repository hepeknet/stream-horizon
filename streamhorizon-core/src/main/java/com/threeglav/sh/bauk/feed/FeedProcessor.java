package com.threeglav.sh.bauk.feed;

import java.util.ArrayList;
import java.util.Map;

import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.command.BaukCommandsExecutor;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.StringUtil;

public final class FeedProcessor extends ConfigAware {

	private final String statementDescription;
	private BaukCommandsExecutor commandExecutor;

	public FeedProcessor(final FactFeed factFeed, final BaukConfiguration config, final String description, final ArrayList<BaukCommand> commands) {
		super(factFeed, config);
		if (StringUtil.isEmpty(description)) {
			throw new IllegalArgumentException("Description must not be null or empty");
		}
		if (commands != null && !commands.isEmpty()) {
			commandExecutor = new BaukCommandsExecutor(factFeed, config, commands);
		}
		statementDescription = description + " for " + this.getFactFeed().getName();
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
