import com.threeglav.bauk.header.HeaderParser;

import java.util.Map;
import java.util.HashMap;

public class CustomHeaderParser implements HeaderParser {

	public Map<String, String> parseHeader(String headerLine, String[] declaredHeaderAttributeNames, String configuredHeaderStartsWithString,
			String configuredDelimiter){
			
			Map<String, String> headerValues = new HashMap();
			header.values.put("abc", "someCustomValue");
			return headerValues;
			
	}
}