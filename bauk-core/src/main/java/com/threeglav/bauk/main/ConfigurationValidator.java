package com.threeglav.bauk.main;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
import com.threeglav.bauk.model.MappedResultsSQLStatement;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.StringUtil;

public class ConfigurationValidator {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final BaukConfiguration config;

	public ConfigurationValidator(final BaukConfiguration config) {
		this.config = config;
	}

	public void validate() throws Exception {
		final boolean sourceOk = this.getOrCreateDirectory(config.getSourceDirectory(), false);
		if (!sourceOk) {
			throw new IllegalStateException("Was not able to find folder where input feeds will be stored! Aborting!");
		}
		final boolean archiveOk = this.getOrCreateDirectory(config.getArchiveDirectory(), true);
		if (!archiveOk) {
			log.warn("Was not able to find directory for storing archives. This feature will be disabled!");
		}
		final boolean errorOk = this.getOrCreateDirectory(config.getErrorDirectory(), true);
		if (!errorOk) {
			throw new IllegalStateException("Was not able to find folder for storing corrupted/invalid data! Aborting!");
		}
		final boolean bulkOutOk = this.getOrCreateDirectory(config.getBulkOutputDirectory(), true);
		if (!bulkOutOk) {
			throw new IllegalStateException("Was not able to find folder for storing bulk output data! Aborting!");
		}
		if (StringUtil.isEmpty(config.getDatabaseStringLiteral())) {
			throw new IllegalStateException("Unable to find non-null, non-empty database string literal!");
		}
		for (final FactFeed ff : config.getFactFeeds()) {
			this.validateFactFeed(ff);
		}
		this.checkValidityOfAttributes();
	}

	private void validateFactFeed(final FactFeed ff) {
		if (!StringUtil.isEmpty(ff.getData().getEachLineStartsWithCharacter())) {
			log.warn("Configuration for feed {} requires every data line to start with [{}]. This is mandatory for correct data interpretation!",
					ff.getName(), ff.getData().getEachLineStartsWithCharacter());
		}
		if (ff.getType() == FactFeedType.REPETITIVE && ff.getRepetitionCount() <= 0) {
			throw new IllegalStateException("Feed " + ff.getName() + " is marked as " + FactFeedType.REPETITIVE
					+ " but repetition count is not positive integer value!");
		}
	}

	private void checkValidityOfAttributes() {
		log.info("Checking validity of declared attributes in configuration");
		final List<String> implicitlyDeclaredAttributes = this.getImplicitDeclaredAttributes();
		log.info("Implicit attributes are {}", implicitlyDeclaredAttributes);
		final Set<String> attributesUsedInConfiguration = this.getAllUsedAttributes();
		final Set<String> attributesDeclaredInConfiguration = this.getAllDeclaredAttributes();
		final int sizeUsed = attributesUsedInConfiguration.size();
		final int sizeDeclared = attributesDeclaredInConfiguration.size();
		if (sizeUsed > sizeDeclared) {
			log.warn("There are more used attributes {} than declared attributes {}. Some attributes might not be declared properly!");
		}
		final Set<String> notAvailable = new HashSet<>();
		for (final String used : attributesUsedInConfiguration) {
			if (!attributesDeclaredInConfiguration.contains(used)) {
				notAvailable.add(used);
			}
		}
		if (!notAvailable.isEmpty()) {
			log.warn(
					"Used attributes {} have not been declared anywhere! This might cause problems! Make sure that someone does provide values for these attributes!",
					notAvailable);
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
		if (config.getFactFeeds() != null) {
			for (final FactFeed ff : config.getFactFeeds()) {
				attrs.addAll(this.getUsedFeedAttributes(ff));
			}
		}
		log.debug("All used attributes are {}", attrs);
		return attrs;
	}

	private Set<String> getAllDeclaredAttributes() {
		final Set<String> attrs = new HashSet<>();
		if (config.getDimensions() != null) {
			for (final Dimension dim : config.getDimensions()) {
				final List<String> dimAttrs = this.getDeclaredDimensionAttributes(dim);
				if (!dimAttrs.isEmpty()) {
					log.info("Dimension {} declared attributes {}", dim.getName(), dimAttrs);
				}
				final int currentSize = attrs.size();
				attrs.addAll(dimAttrs);
				final int newSize = attrs.size();
				if (newSize != currentSize + dimAttrs.size()) {
					log.warn("Looks like someone else declared attribute {}. This might cause unpredictable behaviour.", dimAttrs);
				}
			}
		}
		if (config.getFactFeeds() != null) {
			for (final FactFeed ff : config.getFactFeeds()) {
				final List<String> feedAttrs = this.getDeclaredFeedAttributes(ff);
				if (!feedAttrs.isEmpty()) {
					log.debug("For feed {} found declared attributes {}", ff.getName(), feedAttrs);
					final Set<String> uniqueFeedAttrs = new HashSet<>(feedAttrs);
					if (uniqueFeedAttrs.size() != feedAttrs.size()) {
						final List<String> duplicates = new LinkedList<>();
						duplicates.addAll(feedAttrs);
						for (final String uniq : uniqueFeedAttrs) {
							duplicates.remove(uniq);
						}
						log.warn("Feed {} declares {} attributes but only {} are unique. Duplicate attributes are {}", ff.getName(),
								feedAttrs.size(), uniqueFeedAttrs.size(), duplicates);
					}
					final int currentSize = attrs.size();
					attrs.addAll(uniqueFeedAttrs);
					final int newSize = attrs.size();
					if (newSize != currentSize + uniqueFeedAttrs.size()) {
						log.warn("Some of the attributes declared by feed {} have been declared somewhere else. All declared attributes are {}",
								ff.getName(), attrs);
					}
				}
			}
		}
		return attrs;
	}

	private Set<String> getUsedFeedAttributes(final FactFeed ff) {
		final Set<String> attrs = new HashSet<>();
		if (ff.getBeforeFeedProcessing() != null) {
			for (final MappedResultsSQLStatement mrss : ff.getBeforeFeedProcessing()) {
				final String stat = mrss.getSqlStatement();
				if (!StringUtil.isEmpty(stat)) {
					final Set<String> used = StringUtil.collectAllAttributesFromString(stat);
					if (used != null) {
						attrs.addAll(used);
					}
				}
			}
		}
		if (ff.getAfterFeedProcessingCompletion() != null) {
			for (final String stat : ff.getAfterFeedProcessingCompletion()) {
				if (!StringUtil.isEmpty(stat)) {
					final Set<String> used = StringUtil.collectAllAttributesFromString(stat);
					if (used != null) {
						attrs.addAll(used);
					}
				}
			}
		}
		if (ff.getBulkLoadDefinition() != null) {
			final String insert = ff.getBulkLoadDefinition().getBulkLoadInsertStatement();
			if (!StringUtil.isEmpty(insert)) {
				final Set<String> used = StringUtil.collectAllAttributesFromString(insert);
				if (used != null) {
					attrs.addAll(used);
				}
			}
			if (ff.getBulkLoadDefinition().getOnBulkLoadSuccess() != null) {
				for (final String stat : ff.getBulkLoadDefinition().getOnBulkLoadSuccess().getSqlStatements()) {
					if (!StringUtil.isEmpty(stat)) {
						final Set<String> used = StringUtil.collectAllAttributesFromString(stat);
						if (used != null) {
							attrs.addAll(used);
						}
					}
				}
			}
		}
		log.info("Feed {} uses attributes {}", ff.getName(), attrs);
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
			if (!StringUtil.isEmpty(dim.getSqlStatements().getPreCacheKeys())) {
				final Set<String> used = StringUtil.collectAllAttributesFromString(dim.getSqlStatements().getPreCacheKeys());
				if (used != null) {
					attrs.addAll(used);
				}
			}
			if (!StringUtil.isEmpty(dim.getSqlStatements().getSelectSurrogateKey())) {
				final Set<String> used = StringUtil.collectAllAttributesFromString(dim.getSqlStatements().getSelectSurrogateKey());
				if (used != null) {
					attrs.addAll(used);
				}
			}
		}
		log.info("Dimension {} uses attributes {}", dim.getName(), attrs);
		return attrs;
	}

	private List<String> getDeclaredDimensionAttributes(final Dimension dim) {
		final List<String> attrs = new LinkedList<>();
		if (!StringUtil.isEmpty(dim.getCacheKeyPerFeedInto())) {
			attrs.add(dim.getCacheKeyPerFeedInto());
		}
		return attrs;
	}

	private List<String> getDeclaredFeedAttributes(final FactFeed ff) {
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
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE);

		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_TIMESTAMP);
		attrs.add(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_PROCESSED_TIMESTAMP);

		attrs.add(BaukConstants.COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG);
		attrs.add(BaukConstants.COMPLETION_ATTRIBUTE_NUMBER_OF_ROWS_IN_FEED);
		attrs.add(BaukConstants.COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION);
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
			log.warn("Was not able to find {}. Will try to create it!", directory);
			return dir.mkdirs();
		}
	}

}
