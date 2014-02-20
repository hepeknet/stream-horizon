import java.util.Map;

import com.threeglav.sh.bauk.feed.FeedDataLineProcessor;

public class DataMapper implements FeedDataLineProcessor {

	/**
	 * Invoked once per feed line before that line is passed further for processing (just after line has been read from
	 * file). Must be fast - since it will be invoked once per every line in feed.
	 * <p>
	 * It is very important to keep this method fast because it is executed once per every row in input feed files.
	 * 
	 * @param parsedDataLine
	 *            parsed data row read from file
	 * @param globalAttributes
	 *            global attributes available at the moment when row is being parsed. It is possible to modify these
	 *            attributes in this method.
	 * @return modified data. Must be the same length as passed array
	 */
	public String[] preProcessDataLine(String[] parsedDataLine, Map<String, String> globalAttributes){
		for(int i=0;i<parsedDataLine.length;i++){
			// here we basically replace whatever was in original feed with our custom values
			parsedDataLine[i] = "Some new value";
		}
		globalAttributes.put("myNewAttributeName", "myNewAttributeValue");
		return parsedDataLine;
	}
	
	/**
	 * Invoked once, after processor has been created. This method is invoked only once and before any processing is
	 * done and should be used to initialize processor.
	 * 
	 * @param engineConfigurationProperties
	 *            configuration properties supplied to engine at startup
	 */
	public void init(Map<String, String> engineConfigurationProperties){
		
	}

}