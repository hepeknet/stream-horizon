package com.threeglav.bauk.feed.bulk.writer;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.FileUtil;
import com.threeglav.bauk.util.StringUtil;

public abstract class AbstractBulkOutputWriter extends ConfigAware implements BulkOutputWriter {

	protected final String NEWLINE_STRING = "\n";

	protected final String TEMPORARY_FILE_EXTENSION = ".baukTmp";

	private final boolean performFileRenameOperation;
	protected final String bulkOutputFileDelimiter;
	protected final boolean isSingleCharacterDelimiter;
	protected final char singleCharacterDelimiter;
	protected final boolean isDebugEnabled;
	protected final int bufferSize;
	private final String nullReplacementString;
	protected String finalBulkOutputFilePath;
	protected String temporaryBulkOutputFilePath;

	public AbstractBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		isDebugEnabled = log.isDebugEnabled();
		this.validate();
		bulkOutputFileDelimiter = this.getFactFeed().getBulkLoadDefinition().getBulkLoadFileDelimiter();
		log.debug("For feed {} will use [{}] as delimiter for bulk output file", this.getFactFeed().getName(), bulkOutputFileDelimiter);
		final String outputFileNamePattern = this.getFactFeed().getBulkLoadDefinition().getOutputFileNamePattern();
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
		bufferSize = (int) (ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE);
		nullReplacementString = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_PARAM_NAME,
				SystemConfigurationConstants.BULK_OUTPUT_FILE_NULL_VALUE_DEFAULT);
		log.info("Write buffer size is {} bytes", bufferSize);
	}

	protected void renameTemporaryBulkOutputFile() {
		final File temp = new File(temporaryBulkOutputFilePath);
		final Path originalPath = temp.toPath();
		final File finalFile = new File(finalBulkOutputFilePath);
		final Path destinationPath = finalFile.toPath();
		FileUtil.moveFile(originalPath, destinationPath);
	}

	private void validate() {
		final String outputFileNamePattern = this.getFactFeed().getBulkLoadDefinition().getOutputFileNamePattern();
		if (this.getFactFeed().getBulkLoadDefinition().getOutputType() == BulkLoadDefinitionOutputType.NONE
				&& !StringUtil.isEmpty(outputFileNamePattern)) {
			throw new IllegalStateException("Fact feed " + this.getFactFeed().getName() + " can not have output none and rename pattern!");
		}
		if (StringUtil.isEmpty(this.getFactFeed().getBulkLoadDefinition().getBulkLoadFileDelimiter())) {
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
			final String outputFileNamePattern = this.getFactFeed().getBulkLoadDefinition().getOutputFileNamePattern();
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

	protected StringBuilder concatenateAllValues(final Object[] resolvedData) {
		final StringBuilder sb = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		for (int i = 0; i < resolvedData.length; i++) {
			if (i != 0) {
				if (isSingleCharacterDelimiter) {
					sb.append(singleCharacterDelimiter);
				} else {
					sb.append(bulkOutputFileDelimiter);
				}
			}
			if (resolvedData[i] == null) {
				sb.append(nullReplacementString);
			} else {
				sb.append(resolvedData[i]);
			}
		}
		return sb;
	}

}
