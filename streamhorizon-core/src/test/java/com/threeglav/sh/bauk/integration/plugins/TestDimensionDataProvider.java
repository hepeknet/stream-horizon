package com.threeglav.sh.bauk.integration.plugins;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import com.threeglav.sh.bauk.dimension.DimensionDataProvider;
import com.threeglav.sh.bauk.dimension.DimensionRecord;

public class TestDimensionDataProvider implements DimensionDataProvider {

	@Override
	public void init(final Map<String, String> engineConfigurationProperties) {

	}

	@Override
	public Collection<DimensionRecord> getDimensionRecords() {
		final LinkedList<DimensionRecord> records = new LinkedList<>();
		final DimensionRecord dr1 = new DimensionRecord();
		dr1.setSurrogateKey(666);
		dr1.setNaturalKeyValues(new String[] { "ap66", "bp66" });
		records.add(dr1);
		final DimensionRecord dr2 = new DimensionRecord();
		dr2.setSurrogateKey(777);
		dr2.setNaturalKeyValues(new String[] { "ap77", "bp77" });
		records.add(dr2);
		return records;
	}

}
