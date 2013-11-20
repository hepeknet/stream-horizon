package com.threeglav.bauk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

public abstract class ConfigurationProperties {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationProperties.class);

	public static int getSystemProperty(final String systemPropertyName, final int defaultValue) {
		if (StringUtil.isEmpty(systemPropertyName)) {
			throw new IllegalArgumentException("System property name must not be null or empty");
		}
		final String sysPropValue = System.getProperty(systemPropertyName);
		if (!StringUtil.isEmpty(sysPropValue)) {
			LOG.debug("Found {}={}. Will try to convert it to integer", systemPropertyName, sysPropValue);
			try {
				final int val = Integer.parseInt(sysPropValue);
				LOG.debug("Will use set value {}={}", systemPropertyName, val);
				return val;
			} catch (final NumberFormatException nfe) {
				LOG.error("Exception while converting {} to integer", sysPropValue);
			}
		} else {
			LOG.info("Did not find set value for system property {}. Will use default value {}", systemPropertyName, defaultValue);
		}
		return defaultValue;
	}

	public static String getApplicationHome() {
		final String home = System.getProperty(Constants.APP_HOME_SYS_PARAM_NAME);
		LOG.debug("Application home is {}", home);
		return home;
	}

}
