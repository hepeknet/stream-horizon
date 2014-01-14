package com.threeglav.bauk.feed.bulk.writer;

import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BaukProperty;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.FactFeed;

public class FileBulkOutputWriterTest {

	@Test
	public void testConcatenationNothingSet() {
		ConfigurationProperties.setBaukProperties(null);
		final FactFeed ff = Mockito.mock(FactFeed.class);
		when(ff.getName()).thenReturn("testff1");
		final BulkLoadDefinition bld = Mockito.mock(BulkLoadDefinition.class);
		when(bld.getBulkLoadFileDelimiter()).thenReturn(",");
		when(ff.getBulkLoadDefinition()).thenReturn(bld);
		final BaukConfiguration conf = Mockito.mock(BaukConfiguration.class);
		final FileBulkOutputWriter fbowt = new FileBulkOutputWriter(ff, conf);
		final StringBuilder sb = fbowt.concatenateAllValues(new Object[] { "1", null, "2", null, "3" });
		Assert.assertEquals("1,,2,,3", sb.toString());
	}

	@Test
	public void testConcatenationSetNullValue() {
		final FactFeed ff = Mockito.mock(FactFeed.class);
		when(ff.getName()).thenReturn("testff1");
		final BulkLoadDefinition bld = Mockito.mock(BulkLoadDefinition.class);
		when(bld.getBulkLoadFileDelimiter()).thenReturn(",");
		when(ff.getBulkLoadDefinition()).thenReturn(bld);
		final BaukConfiguration conf = Mockito.mock(BaukConfiguration.class);
		final ArrayList<BaukProperty> props = new ArrayList<>();
		BaukProperty bp = new BaukProperty();
		bp.setName(SystemConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME);
		bp.setValue("null");
		props.add(bp);
		when(conf.getProperties()).thenReturn(props);
		ConfigurationProperties.setBaukProperties(props);
		final FileBulkOutputWriter fbowt = new FileBulkOutputWriter(ff, conf);
		final StringBuilder sb = fbowt.concatenateAllValues(new Object[] { "1", null, "2", null, "3" });
		Assert.assertEquals("1,null,2,null,3", sb.toString());
		bp = new BaukProperty();
		bp.setName(SystemConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME);
		bp.setValue("");
		props.clear();
		props.add(bp);
		when(conf.getProperties()).thenReturn(props);
		ConfigurationProperties.setBaukProperties(props);
		final FileBulkOutputWriter fbowt1 = new FileBulkOutputWriter(ff, conf);
		final StringBuilder sb1 = fbowt1.concatenateAllValues(new Object[] { "1", null, "2", null, "3" });
		Assert.assertEquals("1,,2,,3", sb1.toString());
	}

}
