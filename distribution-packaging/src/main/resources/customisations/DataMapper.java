import com.threeglav.bauk.feed.FeedDataLineProcessor;

public class DataMapper implements FeedDataLineProcessor {

	/**
	 * Invoked once per feed line before that line is passed further for processing (just after line has been read from
	 * file). Must be fast - since it will be invoked once per every line in feed.
	 * 
	 * @param parsedDataLine
	 *            data read from file
	 * @return modified data. Must be same length as passed array
	 */
	public String[] preProcessDataLine(String[] parsedDataLine){
		for(int i=0;i<parsedDataLine.length;i++){
			// here we basically replace whatever was in original feed with our custom values
			parsedDataLine[i] = "Some new value";
		}
		return parsedDataLine;
	}

}