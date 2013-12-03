import com.threeglav.bauk.feed.FeedDataLineProcessor;

public class DataMapper implements FeedDataLineProcessor {

	public String[] preProcessDataLine(String[] parsedDataLine){
		for(int i=0;i<parsedDataLine.length;i++){
			parsedDataLine[i] = "Some new value";
		}
		return parsedDataLine;
	}

}