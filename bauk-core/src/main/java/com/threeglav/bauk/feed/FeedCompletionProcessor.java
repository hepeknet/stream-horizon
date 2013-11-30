package com.threeglav.bauk.feed;

import java.util.Map;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public class FeedCompletionProcessor extends ConfigAware {

	public FeedCompletionProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
	}

	public void process(final Map<String, String> globalAttributes, final Map<String, String> completionAttributes) {

	}

}
