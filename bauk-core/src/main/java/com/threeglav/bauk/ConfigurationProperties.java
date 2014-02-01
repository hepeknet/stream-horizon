package com.threeglav.bauk;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.model.BaukProperty;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.StringUtil;

public abstract class ConfigurationProperties {

	private static List<BaukProperty> BAUK_PROPERTIES;

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationProperties.class);

	public static void setBaukProperties(final List<BaukProperty> baukProps) {
		BAUK_PROPERTIES = baukProps;
		BaukUtil.logEngineMessage("Bauk properties are " + BAUK_PROPERTIES);
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
			throw new IllegalArgumentException("Prop name must not be null or empty!");
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
		String baukPropValue = getBaukProperty(systemPropertyName);
		if (baukPropValue == null) {
			baukPropValue = System.getProperty(systemPropertyName);
		}
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
		String baukPropValue = getBaukProperty(systemPropertyName);
		if (baukPropValue == null) {
			baukPropValue = System.getProperty(systemPropertyName);
		}
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
		String baukPropValue = getBaukProperty(systemPropertyName);
		if (baukPropValue == null) {
			baukPropValue = System.getProperty(systemPropertyName);
		}
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

	public static String getSystemProperty(final String systemPropertyName, final String defaultValue) {
		if (StringUtil.isEmpty(systemPropertyName)) {
			throw new IllegalArgumentException("System property name must not be null or empty");
		}
		String baukPropValue = getBaukProperty(systemPropertyName);
		if (baukPropValue == null) {
			baukPropValue = System.getProperty(systemPropertyName);
		}
		if (!StringUtil.isEmpty(baukPropValue)) {
			return baukPropValue;
		} else {
			LOG.info("Did not find set value for system property {}. Will use default value {}", systemPropertyName, defaultValue);
		}
		return defaultValue;
	}

	public static String getApplicationHome() {
		final String home = System.getProperty(SystemConfigurationConstants.APP_HOME_SYS_PARAM_NAME);
		LOG.debug("Application home is {}", home);
		return home;
	}

	public static String getConfigFolder() {
		return getApplicationHome() + SystemConfigurationConstants.CONFIG_FOLDER_NAME;
	}

	public static String getPluginFolder() {
		return getApplicationHome() + SystemConfigurationConstants.PLUGINS_FOLDER_NAME;
	}

	public static String getWebAppsFolder() {
		return getApplicationHome() + SystemConfigurationConstants.WEB_APPS_FOLDER_NAME;
	}

	public static String getDbDataFolder() {
		final String fullFolderPath = getApplicationHome() + SystemConfigurationConstants.DB_DATA_FOLDER;
		final File dir = new File(fullFolderPath);
		if (!dir.exists() || !dir.isDirectory() || !dir.canRead() || !dir.canExecute()) {
			throw new IllegalStateException("Unable to find readable folder [" + fullFolderPath + "]");
		}
		return fullFolderPath;
	}

}
