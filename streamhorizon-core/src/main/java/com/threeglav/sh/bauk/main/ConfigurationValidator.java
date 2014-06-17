package com.threeglav.sh.bauk.main;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedSource;
import com.threeglav.sh.bauk.model.FeedType;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.StringUtil;

class ConfigurationValidator {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final BaukConfiguration config;

	public ConfigurationValidator(final BaukConfiguration config) {
		this.config = config;
	}

	void validate() throws Exception {
		if (StringUtil.isEmpty(config.getDatabaseStringLiteral())) {
			throw new IllegalStateException("Unable to find non-null, non-empty database string literal!");
		}
		if (StringUtil.isEmpty(config.getDatabaseStringEscapeLiteral())) {
			throw new IllegalStateException("Unable to find non-null, non-empty database string escape literal!");
		}
		for (final Feed ff : config.getFeeds()) {
			this.validateFactFeed(ff);
		}
		this.checkValidityOfAttributes();
		final String baukInstanceIdentifier = ConfigurationProperties.getBaukInstanceIdentifier();
		if (StringUtil.isEmpty(baukInstanceIdentifier)) {
			log.warn(
					"Could not find uniquely set system property {}. Every bauk instance should have uniquely set integer identifier (starting from 0)",
					BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME);
		} else {
			try {
				final int baukInstanceId = Integer.parseInt(baukInstanceIdentifier);
				if (baukInstanceId < 0) {
					log.warn(
							"Every bauk instance should have uniquely set non-negative integer identifier (starting from 0). Currently set value is [{}] is not valid",
							baukInstanceId);
				} else {
					log.info("Unique bauk instance identifier is {}", baukInstanceId);
				}
			} catch (final Exception exc) {
				log.warn(
						"Every bauk instance should have uniquely set non-negative integer identifier (starting from 0). Currently set value is [{}] - it could not be converted into integer",
						baukInstanceIdentifier);
			}
		}
		this.validateSizeOfJdbcConnectionPool();
	}

	private void validateSizeOfJdbcConnectionPool() {
		final int maxConnections = config.getConnectionProperties().getJdbcPoolSize();
		int minRequired = 0;
		final int safetyNet = 10;
		minRequired += safetyNet;
		for (final Feed ff : config.getFeeds()) {
			final int minRequiredPerFeed = ff.getMinimumRequiredJdbcConnections();
			minRequired += minRequiredPerFeed;
		}
		if (maxConnections < minRequired) {
			throw new IllegalStateException("JDBC pool size is too small. At least " + minRequired
					+ " connections are needed for engine to function properly! Your current settings allow maximum " + maxConnections
					+ " to be created!");
		}
	}

	private String validateFeedSource(final Feed ff) {
		if (ff.getSource() == null) {
			throw new IllegalStateException("Feed source is not properly configured!");
		}
		final String sourceType = ff.getSource().getType();
		if (StringUtil.isEmpty(sourceType)) {
			throw new IllegalStateException("Feed source type can not be null or empty!");
		}
		final ArrayList<BaukProperty> properties = ff.getSource().getProperties();
		if (FeedSource.FILE_FEED_SOURCE.equalsIgnoreCase(sourceType)) {
			final String configuredSourceDirectory = BaukPropertyUtil.getRequiredUniqueProperty(properties,
					FeedSource.FILE_FEED_SOURCE_DIRECTORY_PATH_PROPERTY_NAME).getValue();
			final String sourceDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME,
					configuredSourceDirectory);
			if (StringUtil.isEmpty(sourceDirectory)) {
				throw new IllegalStateException("For file feed source " + FeedSource.FILE_FEED_SOURCE_DIRECTORY_PATH_PROPERTY_NAME
						+ " must be defined!");
			}
			final Collection<String> fileNameMasks = BaukPropertyUtil.getAllPropertyValuesByName(ff.getSource().getProperties(),
					FeedSource.FILE_FEED_SOURCE_FILE_NAME_MASK_PROPERTY_NAME);
			if (fileNameMasks == null || fileNameMasks.isEmpty()) {
				throw new IllegalStateException("For file feed source at least one value for "
						+ FeedSource.FILE_FEED_SOURCE_FILE_NAME_MASK_PROPERTY_NAME + " must be defined!");
			}
			return configuredSourceDirectory;
		} else if (FeedSource.RPC_FEED_SOURCE.equalsIgnoreCase(sourceType)) {
			final String configuredPortNumber = BaukPropertyUtil.getRequiredUniqueProperty(properties,
					FeedSource.RPC_FEED_SOURCE_SERVER_PORT_PROPERTY_NAME).getValue();
			if (StringUtil.isEmpty(configuredPortNumber)) {
				throw new IllegalStateException("When using " + FeedSource.RPC_FEED_SOURCE + " feed source you must specify "
						+ FeedSource.RPC_FEED_SOURCE_SERVER_PORT_PROPERTY_NAME + " property!");
			}
			try {
				final int portNum = Integer.parseInt(configuredPortNumber);
				this.validatePort(portNum);
				log.info("Feed source is {} and will use server port {}", FeedSource.RPC_FEED_SOURCE, portNum);

			} catch (final Exception exc) {
				throw new IllegalStateException("Exception while converting port value [" + configuredPortNumber + "] to integer value!");
			}
		} else if (FeedSource.JDBC_FEED_SOURCE.equalsIgnoreCase(sourceType)) {
			final String configuredSql = BaukPropertyUtil.getRequiredUniqueProperty(properties,
					FeedSource.JDBC_FEED_SOURCE_SQL_STATEMENT_PROPERTY_NAME).getValue();
			if (StringUtil.isEmpty(configuredSql)) {
				throw new IllegalStateException("For jdbc feed source " + FeedSource.JDBC_FEED_SOURCE_SQL_STATEMENT_PROPERTY_NAME
						+ " must not be null or empty string!");
			}
			final String configuredSchedule = BaukPropertyUtil.getRequiredUniqueProperty(properties,
					FeedSource.JDBC_FEED_SOURCE_SCHEDULE_PROPERTY_NAME).getValue();
			if (StringUtil.isEmpty(configuredSchedule)) {
				throw new IllegalStateException("For jdbc feed source " + FeedSource.JDBC_FEED_SOURCE_SCHEDULE_PROPERTY_NAME
						+ " must not be null or empty string!");
			}
			final String jdbcUrl = BaukPropertyUtil.getRequiredUniqueProperty(properties, FeedSource.JDBC_FEED_SOURCE_JDBC_URL_PROPERTY_NAME)
					.getValue();
			if (StringUtil.isEmpty(jdbcUrl)) {
				throw new IllegalStateException("For jdbc feed source " + FeedSource.JDBC_FEED_SOURCE_JDBC_URL_PROPERTY_NAME
						+ " must not be null or empty string!");
			}
		}
		return null;
	}

	private void validatePort(final int portNum) {
		if (portNum < 1024 || portNum > 65535) {
			throw new IllegalArgumentException("Port number must be in valid range [1024-65535]. You provided value " + portNum);
		}
	}

	private void validateFeedTarget(final Feed ff) {
		if (ff.getTarget() == null) {
			throw new IllegalArgumentException("Feed target must not be null for " + ff.getName());
		}
	}

	private void validateFactFeed(final Feed ff) throws Exception {
		final String sourceDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME,
				this.validateFeedSource(ff));
		if (sourceDirectory != null) { // only if specified, otherwise no need for input directory
			final boolean sourceOk = this.getOrCreateDirectory(sourceDirectory, false);
			if (!sourceOk) {
				throw new IllegalStateException("Was not able to find folder where input feeds will be stored for feed " + ff.getName()
						+ "! Aborting!");
			}
		}
		this.validateFeedTarget(ff);
		final String archiveDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.ARCHIVE_DIRECTORY_PARAM_NAME,
				ff.getArchiveDirectory());
		final boolean archiveOk = this.getOrCreateDirectory(archiveDirectory, true);
		if (!archiveOk) {
			log.warn("Was not able to find directory for storing archives for feed {}. This feature will be disabled!", ff.getName());
		}
		final String errorDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.ERROR_DIRECTORY_PARAM_NAME,
				ff.getErrorDirectory());
		final boolean errorOk = this.getOrCreateDirectory(errorDirectory, true);
		if (!errorOk) {
			throw new IllegalStateException("Was not able to find folder for storing corrupted/invalid data for feed " + ff.getName() + "! Aborting!");
		}
		final String bulkOutDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.OUTPUT_DIRECTORY_PARAM_NAME,
				ff.getBulkOutputDirectory());
		final boolean bulkOutOk = this.getOrCreateDirectory(bulkOutDirectory, true);
		if (!bulkOutOk) {
			throw new IllegalStateException("Was not able to find folder for storing bulk output data for feed " + ff.getName() + "! Aborting!");
		}
		if (!StringUtil.isEmpty(ff.getData().getEachLineStartsWithCharacter())) {
			log.warn(
					"Configuration for feed {} requires every data line to start with [{}]. This is mandatory for correct data interpretation of input feeds!",
					ff.getName(), ff.getData().getEachLineStartsWithCharacter());
		}
		if (ff.getType() == FeedType.REPETITIVE && ff.getRepetitionCount() <= 0) {
			throw new IllegalStateException("Feed " + ff.getName() + " is marked as " + FeedType.REPETITIVE
					+ " but repetition count is not positive integer value!");
		}
	}

	private void checkValidityOfAttributes() {
		log.info("Checking validity of declared attributes in configuration");
		final List<String> implicitlyDeclaredAttributes = this.getImplicitDeclaredAttributes();
		log.info("Implicitly available attributes are {}", implicitlyDeclaredAttributes);
		final Set<String> attributesUsedInConfiguration = this.getAllUsedAttributes();
		final Set<String> attributesDeclaredInConfiguration = this.getAllDeclaredAttributes();
		final int sizeUsed = attributesUsedInConfiguration.size();
		final int sizeDeclared = attributesDeclaredInConfiguration.size();
		if (sizeUsed > sizeDeclared) {
			log.warn("There are more used attributes {} than declared attributes {}. Some attributes might not be declared properly!",
					attributesUsedInConfiguration, attributesDeclaredInConfiguration);
		}
		final Set<String> notDeclaredAttributes = new HashSet<>();
		for (final String used : attributesUsedInConfiguration) {
			if (!attributesDeclaredInConfiguration.contains(used) && !implicitlyDeclaredAttributes.contains(used)) {
				notDeclaredAttributes.add(used);
			}
		}
		if (!notDeclaredAttributes.isEmpty()) {
			log.warn(
					"{} of used attributes have not been declared anywhere! Those attributes are {}. This might cause problems! Make sure that there are provided values for all these attributes!",
					notDeclaredAttributes.size(), notDeclaredAttributes);
		} else {
			log.info("Looks like all used attributes have been properly declared!");
		}
	}

	private Set<String> getAllUsedAttributes() {
		final Set<String> attrs = new HashSet<>();
		if (config.getDimensions() != null) {
			for (final Dimension dim : config.getDimensions()) {
				attrs.addAll(this.getUsedDimensionAttributes(dim));
			}
		}
		if (config.getFeeds() != null) {
			for (final Feed ff : config.getFeeds()) {
				attrs.addAll(this.getUsedFeedAttributes(ff));
			}
		}
		log.debug("All used attributes are {}", attrs);
		return attrs;
	}

	private Set<String> getAllDeclaredAttributes() {
		final Set<String> attrs = new HashSet<>();
		if (config.getFeeds() != null) {
			for (final Feed ff : config.getFeeds()) {
				final List<String> feedAttrs = this.getDeclaredFeedAttributes(ff);
				if (!feedAttrs.isEmpty()) {
					log.debug("For feed [{}] found declared attributes {}", ff.getName(), feedAttrs);
					final Set<String> uniqueFeedAttrs = new HashSet<>(feedAttrs);
					if (uniqueFeedAttrs.size() != feedAttrs.size()) {
						final List<String> duplicates = new LinkedList<>();
						duplicates.addAll(feedAttrs);
						for (final String uniq : uniqueFeedAttrs) {
							duplicates.remove(uniq);
						}
						log.warn("Feed [{}] declares {} attributes but only {} are unique. Duplicate attribute(s): {}", ff.getName(),
								feedAttrs.size(), uniqueFeedAttrs.size(), duplicates);
					}
					final int currentSize = attrs.size();
					attrs.addAll(uniqueFeedAttrs);
					final int newSize = attrs.size();
					if (newSize != currentSize + uniqueFeedAttrs.size()) {
						log.warn("Some of the attributes declared by feed [{}] have been declared somewhere else. All declared attributes are {}",
								ff.getName(), attrs);
					}
				}
			}
		}
		return attrs;
	}

	private Set<String> getUsedFeedAttributes(final Feed ff) {
		final Set<String> attrs = new HashSet<>();
		if (ff.getEvents() != null && ff.getEvents().getBeforeFeedProcessing() != null) {
			for (final BaukCommand mrss : ff.getEvents().getBeforeFeedProcessing()) {
				final String stat = mrss.getCommand();
				if (!StringUtil.isEmpty(stat)) {
					final Set<String> used = StringUtil.collectAllAttributesFromString(stat);
					if (used != null) {
						attrs.addAll(used);
					}
				}
			}
		}
		if (ff.getEvents() != null && ff.getEvents().getAfterFeedProcessingCompletion() != null) {
			for (final BaukCommand bc : ff.getEvents().getAfterFeedProcessingCompletion()) {
				if (!StringUtil.isEmpty(bc.getCommand())) {
					final Set<String> used = StringUtil.collectAllAttributesFromString(bc.getCommand());
					if (used != null) {
						attrs.addAll(used);
					}
				}
			}
		}
		if (ff.getBulkLoadDefinition() != null && ff.getBulkLoadDefinition().getBulkLoadInsert() != null) {
			for (final BaukCommand bc : ff.getBulkLoadDefinition().getBulkLoadInsert()) {
				if (!StringUtil.isEmpty(bc.getCommand())) {
					final Set<String> used = StringUtil.collectAllAttributesFromString(bc.getCommand());
					if (used != null) {
						attrs.addAll(used);
					}
				}
			}
			if (ff.getBulkLoadDefinition().getAfterBulkLoadSuccess() != null) {
				for (final BaukCommand bc : ff.getBulkLoadDefinition().getAfterBulkLoadSuccess()) {
					if (!StringUtil.isEmpty(bc.getCommand())) {
						final Set<String> used = StringUtil.collectAllAttributesFromString(bc.getCommand());
						if (used != null) {
							attrs.addAll(used);
						}
					}
				}
			}
		}
		log.info("Feed [{}] uses in total {} attributes and those are {}", ff.getName(), attrs.size(), attrs);
		return attrs;
	}

	private Set<String> getUsedDimensionAttributes(final Dimension dim) {
		final Set<String> attrs = new HashSet<>();
		if (dim.getSqlStatements() != null) {
			if (!StringUtil.isEmpty(dim.getSqlStatements().getInsertSingle())) {
				final Set<String> used = StringUtil.collectAllAttributesFromString(dim.getSqlStatements().getInsertSingle());
				if (used != null) {
					attrs.addAll(used);
				}
			}
			if (!StringUtil.isEmpty(dim.getSqlStatements().getPreCacheRecords())) {
				final Set<String> used = StringUtil.collectAllAttributesFromString(dim.getSqlStatements().getPreCacheRecords());
				if (used != null) {
					attrs.addAll(used);
				}
			}
			if (!StringUtil.isEmpty(dim.getSqlStatements().getSelectRecordIdentifier())) {
				final Set<String> used = StringUtil.collectAllAttributesFromString(dim.getSqlStatements().getSelectRecordIdentifier());
				if (used != null) {
					attrs.addAll(used);
				}
			}
		}
		log.info("Dimension [{}] uses attributes {}", dim.getName(), attrs);
		return attrs;
	}

	private List<String> getDeclaredFeedAttributes(final Feed ff) {
		final List<String> attrs = new LinkedList<>();
		if (ff.getHeader() != null && ff.getHeader().getAttributes() != null) {
			final String[] attrNames = AttributeParsingUtil.getAttributeNames(ff.getHeader().getAttributes());
			if (attrNames != null) {
				for (int i = 0; i < attrNames.length; i++) {
					attrs.add(attrNames[i]);
				}
			}
		}
		if (ff.getData() != null && ff.getData().getAttributes() != null) {
			final String[] attrNames = AttributeParsingUtil.getAttributeNames(ff.getData().getAttributes());
			if (attrNames != null) {
				for (int i = 0; i < attrNames.length; i++) {
					attrs.add(attrNames[i]);
				}
			}
		}
		return attrs;
	}

	private List<String> getImplicitDeclaredAttributes() {
		final List<String> attrs = new LinkedList<>();
		attrs.add(BaukConstants.ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_START_TIME);
		attrs.add(BaukConstants.ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_IDENTIFIER);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_ID);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_PARTITIONED_ID);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_MULTI_INSTANCE_BULK_PROCESSOR_ID);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_MULTI_INSTANCE_FEED_PROCESSOR_ID);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_DATE_TIME);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_FINISHED_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_DATE_TIME);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE);

		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		attrs.add(BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_FOR_PROCESSING_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_STARTED_PROCESSING_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_JDBC_STARTED_PROCESSING_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_JDBC_FINISHED_PROCESSING_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FINISHED_PROCESSING_TIMESTAMP);
		attrs.add(BaukConstants.BULK_COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG);
		attrs.add(BaukConstants.BULK_COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION);

		attrs.add(BaukConstants.COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG);
		attrs.add(BaukConstants.COMPLETION_ATTRIBUTE_NUMBER_OF_ROWS_IN_FEED);
		attrs.add(BaukConstants.COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION);
		final StringBuilder sb = new StringBuilder("\n\n========================================================");
		sb.append("\n\n");
		sb.append("Implicitly available attributes are added by engine during processing and can be used in configuration.");
		sb.append("\n");
		sb.append("Attributes named feed* are related to input feed processing.");
		sb.append("\n");
		sb.append("Attributes named bulk* are related to bulk data processing (file or jdbc)");
		sb.append("\n\n");
		sb.append("All implicitly available attributes are:");
		sb.append("\n\n");
		sb.append(attrs);
		sb.append("\n\n");
		sb.append("========================================================");
		sb.append("\n\n");
		BaukUtil.logEngineMessage(sb.toString());
		return attrs;
	}

	private boolean getOrCreateDirectory(final String directory, final boolean shouldWrite) throws Exception {
		if (StringUtil.isEmpty(directory)) {
			return false;
		}
		log.debug("Checking if [{}] is readable directory", directory);
		final File dir = new File(directory);
		if (dir.exists() && dir.canRead() && dir.isDirectory()) {
			if (shouldWrite) {
				return dir.canWrite();
			} else {
				return true;
			}
		} else {
			log.warn("Was not able to find [{}]. Will try to create it!", directory);
			return dir.mkdirs();
		}
	}

}
