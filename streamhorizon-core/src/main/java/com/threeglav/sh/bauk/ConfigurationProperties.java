package com.threeglav.sh.bauk;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public abstract class ConfigurationProperties {

	private static List<BaukProperty> BAUK_PROPERTIES;

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationProperties.class);

	public static void setBaukProperties(final List<BaukProperty> baukProps) {
		BAUK_PROPERTIES = baukProps;
		BaukUtil.logEngineMessage("Engine properties are " + BAUK_PROPERTIES);
	}

	public static Map<String, String> getEngineConfigurationProperties() {
		final Map<String, String> engineConfigProperties = new THashMap<>();
		if (BAUK_PROPERTIES != null) {
			for (final BaukProperty bp : BAUK_PROPERTIES) {
				final String propName = bp.getName();
				final String propValue = bp.getValue();
				if (!StringUtil.isEmpty(propName) && !StringUtil.isEmpty(propValue)) {
					engineConfigProperties.put(propName, propValue);
				}
			}
		}
		return engineConfigProperties;
	}

	private static String getBaukProperty(final String propName) {
		if (StringUtil.isEmpty(propName)) {
			throw new IllegalArgumentException("Property name must not be null or empty!");
		}
		String val = null;
		if (BAUK_PROPERTIES != null) {
			for (final BaukProperty bp : BAUK_PROPERTIES) {
				if (propName.equalsIgnoreCase(bp.getName())) {
					val = bp.getValue();
					LOG.debug("Value for [{}] is [{}]", propName, val);
					break;
				}
			}
		}
		return val;
	}

	public static boolean getSystemProperty(final String systemPropertyName, final boolean defaultValue) {
		if (StringUtil.isEmpty(systemPropertyName)) {
			throw new IllegalArgumentException("System property name must not be null or empty");
		}
		final String baukPropValue = getPropertyByName(systemPropertyName);
		if (!StringUtil.isEmpty(baukPropValue)) {
			return Boolean.valueOf(baukPropValue);
		} else {
			LOG.info("Did not find set value for system property {}. Will use default value {}", systemPropertyName, defaultValue);
		}
		return defaultValue;
	}

	public static float getSystemProperty(final String systemPropertyName, final float defaultValue) {
		if (StringUtil.isEmpty(systemPropertyName)) {
			throw new IllegalArgumentException("System property name must not be null or empty");
		}
		final String baukPropValue = getPropertyByName(systemPropertyName);
		if (!StringUtil.isEmpty(baukPropValue)) {
			LOG.debug("Found {}={}. Will try to convert it to integer", systemPropertyName, baukPropValue);
			try {
				final float val = Float.parseFloat(baukPropValue);
				LOG.debug("Will use set value {}={}", systemPropertyName, val);
				return val;
			} catch (final NumberFormatException nfe) {
				LOG.error("Exception while converting {} to float value", baukPropValue);
			}
		} else {
			LOG.info("Did not find set value for system property {}. Will use default value {}", systemPropertyName, defaultValue);
		}
		return defaultValue;
	}

	public static int getSystemProperty(final String systemPropertyName, final int defaultValue) {
		if (StringUtil.isEmpty(systemPropertyName)) {
			throw new IllegalArgumentException("System property name must not be null or empty");
		}
		final String baukPropValue = getPropertyByName(systemPropertyName);
		if (!StringUtil.isEmpty(baukPropValue)) {
			LOG.debug("Found {}={}. Will try to convert it to integer value", systemPropertyName, baukPropValue);
			try {
				final int val = Integer.parseInt(baukPropValue);
				LOG.debug("Will use set value {}={}", systemPropertyName, val);
				return val;
			} catch (final NumberFormatException nfe) {
				LOG.error("Exception while converting {} to integer", baukPropValue);
			}
		} else {
			LOG.info("Did not find set value for system property {}. Will use default value {}", systemPropertyName, defaultValue);
		}
		return defaultValue;
	}

	private static String getPropertyByName(final String propertyName) {
		String baukPropValue = System.getProperty(propertyName);
		if (StringUtil.isEmpty(baukPropValue)) {
			baukPropValue = getBaukProperty(propertyName);
		}
		return baukPropValue;
	}

	public static String getSystemProperty(final String systemPropertyName, final String defaultValue) {
		if (StringUtil.isEmpty(systemPropertyName)) {
			throw new IllegalArgumentException("System property name must not be null or empty");
		}
		final String baukPropValue = getPropertyByName(systemPropertyName);
		if (!StringUtil.isEmpty(baukPropValue)) {
			return baukPropValue;
		} else {
			LOG.info("Did not find set value for system property {}. Will use default value {}", systemPropertyName, defaultValue);
		}
		return defaultValue;
	}

	public static String getApplicationHome() {
		final String home = System.getProperty(BaukEngineConfigurationConstants.APP_HOME_SYS_PARAM_NAME);
		LOG.debug("Application home is {}", home);
		return home;
	}

	public static String getConfigFolder() {
		return getApplicationHome() + BaukEngineConfigurationConstants.CONFIG_FOLDER_NAME;
	}

	public static String getPluginFolder() {
		return getApplicationHome() + BaukEngineConfigurationConstants.PLUGINS_FOLDER_NAME;
	}

	public static String getWebAppsFolder() {
		return getApplicationHome() + BaukEngineConfigurationConstants.WEB_APPS_FOLDER_NAME;
	}

	public static String getBaukInstanceIdentifier() {
		return System.getProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME);
	}

	public static String getDbDataFolder() {
		final String fullFolderPath = getApplicationHome() + BaukEngineConfigurationConstants.DB_DATA_FOLDER + "instance_"
				+ getBaukInstanceIdentifier() + "/";
		final File dir = new File(fullFolderPath);
		dir.mkdirs();
		if (!dir.exists() || !dir.isDirectory() || !dir.canRead() || !dir.canExecute()) {
			throw new IllegalStateException("Unable to find readable folder [" + fullFolderPath + "]");
		}
		return fullFolderPath;
	}

	public static boolean isConfiguredPartitionedMultipleInstances() {
		final int totalPartitionsCount = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, -1);
		if (totalPartitionsCount > 0) {
			final String currentBaukInstanceIdentifier = ConfigurationProperties.getBaukInstanceIdentifier();
			if (!StringUtil.isEmpty(currentBaukInstanceIdentifier)) {
				int currentBaukInstance = -1;
				try {
					currentBaukInstance = Integer.parseInt(currentBaukInstanceIdentifier);
				} catch (final Exception exc) {
					currentBaukInstance = -1;
					// ignored
				}
				if (currentBaukInstance >= 0 && currentBaukInstance < totalPartitionsCount) {
					LOG.info("Configured to partition work among multiple instances. Current bauk instance id {}, total instances {}",
							currentBaukInstance, totalPartitionsCount);
					return true;
				}
			}
		}
		LOG.info("Not configured to partition work among multiple instances.");
		return false;
	}

}
