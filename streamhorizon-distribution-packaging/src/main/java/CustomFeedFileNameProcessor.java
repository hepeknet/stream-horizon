import java.util.HashMap;
import java.util.Map;

import com.threeglav.sh.bauk.feed.FeedFileNameProcessor;

/**
 * Example how to write custom feed file name processor
 * 
 */
public class CustomFeedFileNameProcessor implements FeedFileNameProcessor {

	/**
	 * Method for parsing feed file name and providing values directly as attributes in context. Whatever is returned by
	 * this method will be available as context attributes.
	 * 
	 * @param feedFileName
	 *            the name of original feed file
	 * @return map of context attribute names and values
	 */
	@Override
	public Map<String, String> parseFeedFileName(final String feedFileName) {
		final Map<String, String> attributes = new HashMap<String, String>();
		attributes.put("myCustomAttribute", "myCustomValue");
		return attributes;
	}

	/**
	 * Invoked once, after processor has been created. This method is invoked only once and before any processing is
	 * done and should be used to initialize processor.
	 * 
	 * @param engineConfigurationProperties
	 *            configuration properties supplied to engine at startup
	 */
	@Override
	public void init(final Map<String, String> engineConfigurationProperties) {
	}

}