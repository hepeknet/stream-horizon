package com.threeglav.sh.bauk.dynamic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.util.FileUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class CustomProcessorResolver<T> {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fullClassName;
	private final Class<T> expectedType;

	public CustomProcessorResolver(final String fullClassName, final Class<T> expectedType) {
		if (StringUtil.isEmpty(fullClassName)) {
			throw new IllegalArgumentException("Full class name must not be null or empty");
		}
		if (expectedType == null) {
			throw new IllegalArgumentException("Expected type must not be null or empty");
		}
		this.fullClassName = fullClassName;
		this.expectedType = expectedType;
	}

	public T resolveInstance() {
		final ClassResolver<T> classResolver = new ClassResolver<>(fullClassName, expectedType);
		T inst = classResolver.createInstanceFromClasspath();
		if (inst == null) {
			final String simpleClassName = StringUtil.getSimpleClassName(fullClassName);
			final String appHome = ConfigurationProperties.getApplicationHomeDirectory();
			if (StringUtil.isEmpty(appHome)) {
				log.error("Was not able to resolve application home. Please set {} system property",
						BaukEngineConfigurationConstants.APP_HOME_SYS_PARAM_NAME);
				return null;
			}
			final String fullCustomProcessorFilePath = ConfigurationProperties.getPluginFolder() + simpleClassName + ".java";
			log.debug("Will try to find plugin class {} at path {}", fullClassName, fullCustomProcessorFilePath);
			final String fileText = FileUtil.getFileAsText(fullCustomProcessorFilePath);
			if (!StringUtil.isEmpty(fileText)) {
				inst = classResolver.createInstanceFromSource(fileText);
			} else {
				log.warn("Was not able to find text in file {}", fullCustomProcessorFilePath);
			}
		}
		return inst;
	}
}
