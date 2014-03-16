package com.threeglav.sh.bauk.feed.bulk.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.CommandType;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.StringUtil;

public class NamedPipeBulkOutputWriter extends AbstractBulkOutputWriter {

	private static final String DEFAULT_PIPE_DIRECTORY = "/var/streamhorizon";

	private BufferedWriter writer;
	private String threadName;
	private final String bulkReadCommand;
	private String preparedBulkReadCommand;
	private Process bulkLoadingProcess;

	public NamedPipeBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		this.validate();
		bulkReadCommand = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert().get(0).getCommand();
		if (StringUtil.isEmpty(bulkReadCommand)) {
			throw new IllegalArgumentException("Bulk insert command must not be null or empty string");
		}
		if (!bulkReadCommand.contains(BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER)) {
			throw new IllegalArgumentException("Bulk insert command must use named pipe attribute " + BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER);
		}
	}

	private void validate() {
		if (this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert() == null
				|| this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert().size() != 1) {
			throw new IllegalStateException("Exactly one bulk insert statement must be specified when using named pipe output");
		}
		final BaukCommand cmd = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert().get(0);
		if (cmd.getType() != CommandType.SHELL) {
			throw new IllegalArgumentException("When using named pipe output bulk insert command must be of type " + CommandType.SHELL);
		}
	}

	@Override
	public void doWriteOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		try {
			final String str = this.concatenateAllValues(resolvedData);
			writer.write(str);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
			throw new IllegalStateException(exc);
		}
	}

	private void createNamedPipe() {
		final String fileName = this.getBulkFilePipeName();
		final File f = new File(fileName);
		final String command = "mkfifo " + fileName;
		if (!f.exists()) {
			log.debug("Named pipe [{}] does not exist. Will try to create it", fileName);
			try {
				Runtime.getRuntime().exec(command);
				log.debug("Successfully created named pipe {}", fileName);
			} catch (final IOException e) {
				log.error("Exception while creating named pipe {}. Details {}", fileName, e.getMessage());
				throw new IllegalStateException("Was not able to create named pipe " + fileName + ". Executed command was [" + command + "]", e);
			}
		}
		if (!f.exists()) {
			throw new IllegalStateException("Was not able to find named pipe " + fileName);
		}
		preparedBulkReadCommand = bulkReadCommand.replace(BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER, fileName);
		if (isDebugEnabled) {
			log.debug("Prepared bulk command to read from named pipe is [{}]", preparedBulkReadCommand);
		}
	}

	private String getBulkFilePipeName() {
		return DEFAULT_PIPE_DIRECTORY + "/bulkFile" + threadName + ".pipe";
	}

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
		if (threadName == null) {
			threadName = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID);
			this.createNamedPipe();
		}
		final String fileName = this.getBulkFilePipeName();
		final File f = new File(fileName);
		if (!f.exists()) {
			throw new IllegalArgumentException("Unable to find named pipe [" + fileName + "]! Check directory permissions!");
		}
		if (isDebugEnabled) {
			log.debug("Named pipe {} exists", fileName);
		}
		try {
			if (isDebugEnabled) {
				log.debug("Executing [{}] in separate process", preparedBulkReadCommand);
			}
			bulkLoadingProcess = Runtime.getRuntime().exec(preparedBulkReadCommand);
			if (isDebugEnabled) {
				log.debug("Successfully executed [{}] in separate process", preparedBulkReadCommand);
			}
		} catch (final IOException e) {
			log.error("Exception while executing [{}] in separate process. Details {}", preparedBulkReadCommand, e.getMessage());
			throw new IllegalStateException(e);
		}
		try {
			writer = new BufferedWriter(new FileWriter(f));
			if (isDebugEnabled) {
				log.debug("Successfully created writer to named pipe {}", fileName);
			}
		} catch (final IOException e) {
			log.error("Error while creating writer to named pipe " + fileName + ".", e);
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void closeResourcesAfterWriting(final Map<String, String> globalAttributes, final boolean success) {
		IOUtils.closeQuietly(writer);
		writer = null;
		final boolean bulkLoadingSuccess = this.wasBulkLoadingSuccessful();
		if (!bulkLoadingSuccess) {
			throw new IllegalStateException("Bulk loading through named pipe was not successful. Command " + preparedBulkReadCommand
					+ " did not execute successfully!");
		}
	}

	private boolean wasBulkLoadingSuccessful() {
		try {
			if (bulkLoadingProcess != null) {
				final int res = bulkLoadingProcess.waitFor();
				final boolean success = (res == 0);
				if (!success) {
					log.warn("Command [{}] returned exit code {} - will report this as unsuccessful bulk loading", preparedBulkReadCommand, res);
				} else {
					if (isDebugEnabled) {
						log.debug("Command [{}] returned exit code 0. Bulk loading was successful", preparedBulkReadCommand);
					}
				}
				return success;
			} else {
				log.error("Process is null");
				return false;
			}
		} catch (final InterruptedException e) {
			log.error("Exception while checking if bulk loading was successful", e);
			throw new IllegalStateException(e);
		}
	}

}
