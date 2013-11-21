import com.threeglav.bauk.feed.FeedProcessor;

public class DataMapper implements FeedProcessor {

	public String[] preProcess(String[] parsedDataLine){
		for(int i=0;i<parsedDataLine.length;i++){
			parsedDataLine[i] = "Some new value";
		}
		return parsedDataLine;
	}

}