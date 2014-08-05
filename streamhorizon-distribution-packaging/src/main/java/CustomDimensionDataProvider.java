import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import com.threeglav.sh.bauk.dimension.DimensionDataProvider;
import com.threeglav.sh.bauk.dimension.DimensionRecord;

/**
 * Example how to write custom dimension data provider
 * 
 * 
 */
public class CustomDimensionDataProvider implements DimensionDataProvider {

	@Override
	public void init(final Map<String, String> engineConfigurationProperties) {
		// initialize whatever resources (JDBC connection etc)
	}

	@Override
	public Collection<DimensionRecord> getDimensionRecords() {
		// obtain some data from DB, file system etc and return it
		// here we return few mocked records
		final LinkedList<DimensionRecord> records = new LinkedList<>();
		for (int i = 0; i < 10; i++) {
			final DimensionRecord rec = new DimensionRecord();
			rec.setSurrogateKey(i);
			final String[] naturalKeys = new String[i + 1];
			for (int j = 0; j < i + 1; j++) {
				naturalKeys[j] = "naturalKeyValue_" + j;
			}
			rec.setNaturalKeyValues(naturalKeys);
			records.add(rec);
		}
		return records;
	}

}
