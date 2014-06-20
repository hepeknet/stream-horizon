package com.threeglav.sh.bauk.feed.bulk.writer;

import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.Feed;

public class FileBulkOutputWriterTest {

	private static final String NEWLINE_STRING = System.getProperty("line.separator");

	@Test
	public void testConcatenationNothingSet() {
		ConfigurationProperties.setBaukProperties(null);
		final Feed ff = Mockito.mock(Feed.class);
		when(ff.getName()).thenReturn("testff1");
		final BaukConfiguration conf = Mockito.mock(BaukConfiguration.class);
		final FileBulkOutputWriter fbowt = new FileBulkOutputWriter(ff, conf);
		final String sb = fbowt.concatenateAllValues(new Object[] { "1", null, "2", null, "3" });
		Assert.assertEquals("1,,2,,3" + NEWLINE_STRING, sb);
	}

	@Test
	public void testConcatenationSetNullValue() {
		final Feed ff = Mockito.mock(Feed.class);
		when(ff.getName()).thenReturn("testff1");
		final BaukConfiguration conf = Mockito.mock(BaukConfiguration.class);
		final ArrayList<BaukProperty> props = new ArrayList<>();
		BaukProperty bp = new BaukProperty();
		bp.setName(BaukEngineConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME);
		bp.setValue("null");
		props.add(bp);
		when(conf.getProperties()).thenReturn(props);
		ConfigurationProperties.setBaukProperties(props);
		final FileBulkOutputWriter fbowt = new FileBulkOutputWriter(ff, conf);
		final String sb = fbowt.concatenateAllValues(new Object[] { "1", null, "2", null, "3" });
		Assert.assertEquals("1,null,2,null,3" + NEWLINE_STRING, sb);
		bp = new BaukProperty();
		bp.setName(BaukEngineConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME);
		bp.setValue("");
		props.clear();
		props.add(bp);
		when(conf.getProperties()).thenReturn(props);
		ConfigurationProperties.setBaukProperties(props);
		final FileBulkOutputWriter fbowt1 = new FileBulkOutputWriter(ff, conf);
		final String sb1 = fbowt1.concatenateAllValues(new Object[] { "1", null, "2", null, "3" });
		Assert.assertEquals("1,,2,,3" + NEWLINE_STRING, sb1);
	}

}
