package com.threeglav.bauk.feed;

import gnu.trove.map.hash.THashMap;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

public class DefaultFeedFileNameProcessor implements FeedFileNameProcessor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String FEED_FILE_ATTR_PREFIX = "ffn[";

	private static final String DELIMITER = "_";

	public DefaultFeedFileNameProcessor() {
		log.info("Default file name processor. Uses delimiter {} and splits file name into context attributes named {}x]", DELIMITER,
				FEED_FILE_ATTR_PREFIX);
	}

	@Override
	public Map<String, String> parseFeedFileName(final String feedFileName) {
		final Map<String, String> parsedFeedFileNameAttributes = new THashMap<String, String>();
		log.debug("Parsing file name [{}] into attributes", feedFileName);
		if (!StringUtil.isEmpty(feedFileName)) {
			final String[] parsedName = feedFileName.split(DELIMITER);
			if (parsedName != null && parsedName.length > 0) {
				for (int i = 0; i < parsedName.length; i++) {
					parsedFeedFileNameAttributes.put(FEED_FILE_ATTR_PREFIX + i + "]", parsedName[i]);
				}
				log.debug("Attributes parsed from feed file name {} are {}", feedFileName, parsedFeedFileNameAttributes);
			}
		}
		return parsedFeedFileNameAttributes;
	}

	@Override
	public void init(final Map<String, String> engineConfigurationProperties) {
	}

}
