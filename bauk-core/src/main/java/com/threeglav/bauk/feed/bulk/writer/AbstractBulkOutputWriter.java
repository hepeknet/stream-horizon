package com.threeglav.bauk.feed.bulk.writer;

import java.io.File;
import java.util.Map;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public abstract class AbstractBulkOutputWriter extends ConfigAware implements BulkOutputWriter {

	private final boolean performFileRenameOperation;

	public AbstractBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		this.validate();
		final String outputFileNamePattern = this.getFactFeed().getBulkLoadDefinition().getOutputFileNamePattern();
		if (StringUtil.isEmpty(outputFileNamePattern)) {
			performFileRenameOperation = false;
		} else {
			performFileRenameOperation = true;
		}
	}

	private void validate() {
		final String outputFileNamePattern = this.getFactFeed().getBulkLoadDefinition().getOutputFileNamePattern();
		if (this.getFactFeed().getBulkLoadDefinition().getOutputType() == BulkLoadDefinitionOutputType.NONE
				&& !StringUtil.isEmpty(outputFileNamePattern)) {
			throw new IllegalStateException("Fact feed " + this.getFactFeed().getName() + " can not have output none and rename pattern!");
		}
		log.debug("Bulk output file for fact feed {} will be renamed (after writing all data to it) according to pattern {}", this.getFactFeed()
				.getName(), outputFileNamePattern);
	}

	protected void renameOutputFile(final String originalFileName, final Map<String, String> globalAttributes) {
		if (performFileRenameOperation) {
			if (StringUtil.isEmpty(originalFileName)) {
				throw new IllegalArgumentException("Original file must not be null or empty");
			}
			final String outputFileNamePattern = this.getFactFeed().getBulkLoadDefinition().getOutputFileNamePattern();
			if (StringUtil.isEmpty(outputFileNamePattern)) {
				return;
			}
			final File originalFile = new File(originalFileName);
			log.debug("Renaming file {} according to pattern {} using attributes {}", originalFile.getAbsolutePath(), outputFileNamePattern,
					globalAttributes);
			final String replacedAttributes = StringUtil.replaceAllAttributes(outputFileNamePattern, globalAttributes, this.getConfig()
					.getDatabaseStringLiteral());
			log.debug("File {} will be renamed to {}", originalFile.getAbsolutePath(), replacedAttributes);
			final File renamedFile = new File(originalFile.getParentFile(), replacedAttributes);
			originalFile.renameTo(renamedFile);
			log.debug("Successfully renamed file {} to {}", originalFile.getAbsolutePath(), renamedFile.getAbsolutePath());
		}
	}

}
