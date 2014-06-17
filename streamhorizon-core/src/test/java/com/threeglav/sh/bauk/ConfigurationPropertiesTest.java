package com.threeglav.sh.bauk;

import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.ThreadPoolSettings;

public class ConfigurationPropertiesTest {

	@Test
	public void testCurrentInstanceIdentifier() {
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "");
		Assert.assertEquals(-1, ConfigurationProperties.getCurrentBaukInstanceIdentifierAsInteger());
		Assert.assertEquals("", ConfigurationProperties.getBaukInstanceIdentifier());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "ab");
		Assert.assertEquals(-1, ConfigurationProperties.getCurrentBaukInstanceIdentifierAsInteger());
		Assert.assertEquals("ab", ConfigurationProperties.getBaukInstanceIdentifier());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "0");
		Assert.assertEquals(0, ConfigurationProperties.getCurrentBaukInstanceIdentifierAsInteger());
		Assert.assertEquals("0", ConfigurationProperties.getBaukInstanceIdentifier());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "12");
		Assert.assertEquals(12, ConfigurationProperties.getCurrentBaukInstanceIdentifierAsInteger());
		Assert.assertEquals("12", ConfigurationProperties.getBaukInstanceIdentifier());
	}

	@Test
	public void testIsMultiInstance() {
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "");
		Assert.assertFalse(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "a");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "b");
		Assert.assertFalse(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "1");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "b");
		Assert.assertFalse(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "1");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "-1");
		Assert.assertFalse(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "0");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "0");
		Assert.assertFalse(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "1");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "1");
		Assert.assertFalse(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "0");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "1");
		Assert.assertTrue(ConfigurationProperties.isConfiguredPartitionedMultipleInstances());
	}

	@Test
	public void testCalculateMultiInstanceIdentifiers() {
		final Feed ff = Mockito.mock(Feed.class);
		final ThreadPoolSettings tps = Mockito.mock(ThreadPoolSettings.class);
		when(tps.getEtlProcessingThreadCount()).thenReturn(5);
		when(tps.getDatabaseProcessingThreadCount()).thenReturn(6);
		when(ff.getThreadPoolSettings()).thenReturn(tps);

		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "");

		Assert.assertEquals(-1, ConfigurationProperties.calculateMultiInstanceFeedProcessorId(10, ff));
		Assert.assertEquals(-1, ConfigurationProperties.calculateMultiInstanceBulkProcessorId(10, ff));

		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "0");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "10");
		Assert.assertEquals(7, ConfigurationProperties.calculateMultiInstanceFeedProcessorId(7, ff));
		Assert.assertEquals(9, ConfigurationProperties.calculateMultiInstanceBulkProcessorId(9, ff));

		System.setProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "1");
		System.setProperty(BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, "10");
		Assert.assertEquals(8, ConfigurationProperties.calculateMultiInstanceFeedProcessorId(3, ff));
		Assert.assertEquals(8, ConfigurationProperties.calculateMultiInstanceBulkProcessorId(2, ff));
	}

}
