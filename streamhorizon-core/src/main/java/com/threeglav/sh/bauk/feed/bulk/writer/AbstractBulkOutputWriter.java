package com.threeglav.sh.bauk.feed.bulk.writer;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.io.BulkOutputWriter;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;
import com.threeglav.sh.bauk.util.FileUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public abstract class AbstractBulkOutputWriter extends ConfigAware implements BulkOutputWriter {

	protected final String TEMPORARY_FILE_EXTENSION = ".shTmp";

	private final boolean performFileRenameOperation;
	protected final String bulkOutputFileDelimiter;
	protected final boolean isSingleCharacterDelimiter;
	protected final char singleCharacterDelimiter;
	protected final boolean isDebugEnabled;
	protected final int bufferSize;
	private final String nullReplacementString;
	protected String finalBulkOutputFilePath;
	protected String temporaryBulkOutputFilePath;
	private String currentThreadName;
	private final StringBuilder reusedForPerformance = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);

	public AbstractBulkOutputWriter(final Feed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		isDebugEnabled = log.isDebugEnabled();
		this.validate();
		bulkOutputFileDelimiter = this.getOutputFileDelimiter();
		log.debug("For feed {} will use [{}] as delimiter for bulk output file", this.getFactFeed().getName(), bulkOutputFileDelimiter);
		final String outputFileNamePattern = this.getOutputFileRenamePattern();
		if (StringUtil.isEmpty(outputFileNamePattern)) {
			performFileRenameOperation = false;
		} else {
			performFileRenameOperation = true;
		}
		if (bulkOutputFileDelimiter.length() == 1) {
			isSingleCharacterDelimiter = true;
			singleCharacterDelimiter = bulkOutputFileDelimiter.charAt(0);
			log.debug("Will use single character delimiter [{}] for {}", singleCharacterDelimiter, this.getFactFeed().getName());
		} else {
			isSingleCharacterDelimiter = false;
			singleCharacterDelimiter = Character.MIN_VALUE;
		}
		bufferSize = (int) (ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				BaukEngineConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE);
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("Writer buffer size must not be <= 0");
		}
		nullReplacementString = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME,
				BaukEngineConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_DEFAULT);
		log.info("Write buffer size is {} bytes", bufferSize);
		log.info("Newline string will be {}", BaukConstants.NEWLINE_STRING);
	}

	private String getOutputFileDelimiter() {
		String delimiter = BaukEngineConfigurationConstants.DEFAULT_BULK_OUTPUT_VALUE_DELIMITER;
		if (this.getFactFeed().isFileTarget()) {
			final BaukProperty bp = BaukPropertyUtil.getUniquePropertyIfExists(this.getFactFeed().getTarget().getProperties(),
					BaukEngineConfigurationConstants.BULK_OUTPUT_FILE_DELIMITER_PROP_NAME);
			if (bp != null) {
				delimiter = bp.getValue();
			}
		}
		return delimiter;
	}

	private String getOutputFileRenamePattern() {
		String renamePattern = null;
		if (this.getFactFeed().isFileTarget()) {
			final BaukProperty bp = BaukPropertyUtil.getUniquePropertyIfExists(this.getFactFeed().getTarget().getProperties(),
					BaukEngineConfigurationConstants.OUTPUT_FILE_RENAME_PATTERN_PROP_NAME);
			if (bp != null) {
				renamePattern = bp.getValue();
			}
		}
		return renamePattern;
	}

	protected void renameTemporaryBulkOutputFile() {
		final File temp = new File(temporaryBulkOutputFilePath);
		final Path originalPath = temp.toPath();
		final File finalFile = new File(finalBulkOutputFilePath);
		final Path destinationPath = finalFile.toPath();
		FileUtil.moveFile(originalPath, destinationPath);
		log.debug("Renamed temporary file [{}] to [{}]", temporaryBulkOutputFilePath, finalBulkOutputFilePath);
	}

	protected void deleteTemporaryBulkOutputFile() {
		if (temporaryBulkOutputFilePath != null) {
			final File temp = new File(temporaryBulkOutputFilePath);
			if (isDebugEnabled) {
				log.debug("Deleting temporary bulk output file {}", temporaryBulkOutputFilePath);
			}
			temp.delete();
		}
	}

	private void validate() {
		final String outputFileNamePattern = this.getOutputFileRenamePattern();
		if (this.getFactFeed().getTarget() != null && this.getFactFeed().getTarget().getType().equals(BulkLoadDefinitionOutputType.NONE.toString())
				&& !StringUtil.isEmpty(outputFileNamePattern)) {
			throw new IllegalStateException("Fact feed " + this.getFactFeed().getName() + " can not have output none and rename pattern!");
		}
		if (StringUtil.isEmpty(this.getOutputFileDelimiter())) {
			throw new IllegalStateException("Could not find bulk load file value delimiter string for feed " + this.getFactFeed().getName() + "!");
		}
		if (isDebugEnabled) {
			log.debug("Bulk output file for fact feed {} will be renamed (after writing all data to it) according to pattern {}", this.getFactFeed()
					.getName(), outputFileNamePattern);
		}
	}

	protected void renameOutputFile(final String originalFileName, final Map<String, String> globalAttributes) {
		if (performFileRenameOperation) {
			if (StringUtil.isEmpty(originalFileName)) {
				throw new IllegalArgumentException("Original file must not be null or empty");
			}
			final String outputFileNamePattern = this.getOutputFileRenamePattern();
			final File originalFile = new File(originalFileName);
			if (isDebugEnabled) {
				log.debug("Renaming file {} according to pattern {} using attributes {}", originalFile.getAbsolutePath(), outputFileNamePattern,
						globalAttributes);
			}
			final String replacedAttributes = StringUtil.replaceAllAttributes(outputFileNamePattern, globalAttributes, this.getConfig()
					.getDatabaseStringLiteral(), this.getConfig().getDatabaseStringEscapeLiteral());
			if (isDebugEnabled) {
				log.debug("File {} will be renamed to {}", originalFile.getAbsolutePath(), replacedAttributes);
			}
			final File renamedFile = new File(originalFile.getParentFile(), replacedAttributes);
			try {
				FileUtil.moveFile(originalFile.toPath(), renamedFile.toPath());
				if (isDebugEnabled) {
					log.debug("Successfully renamed file {} to {}", originalFile.getAbsolutePath(), renamedFile.getAbsolutePath());
				}
			} catch (final Exception ie) {
				log.error("Was not able to rename file {} to {}", temporaryBulkOutputFilePath, finalBulkOutputFilePath);
				log.error("Exception", ie);
			}
		}
	}

	protected String concatenateAllValues(final Object[] resolvedData) {
		reusedForPerformance.setLength(0);
		for (int i = 0; i < resolvedData.length; i++) {
			if (i != 0) {
				if (isSingleCharacterDelimiter) {
					reusedForPerformance.append(singleCharacterDelimiter);
				} else {
					reusedForPerformance.append(bulkOutputFileDelimiter);
				}
			}
			if (resolvedData[i] == null) {
				reusedForPerformance.append(nullReplacementString);
			} else {
				reusedForPerformance.append(resolvedData[i]);
			}
		}
		reusedForPerformance.append(BaukConstants.NEWLINE_STRING);
		return reusedForPerformance.toString();
	}

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
		final String bulkOutputPath = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH);
		this.initialize(bulkOutputPath);
	}

	protected void initialize(final String bulkOutputPath) {

	}

	protected final String getCurrentThreadName() {
		if (currentThreadName == null) {
			currentThreadName = Thread.currentThread().getName();
		}
		return currentThreadName;
	}

	@Override
	public boolean understandsURI(final String protocol) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void init(final Map<String, String> engineProperties) {
		throw new UnsupportedOperationException("Not supported");
	}

}
