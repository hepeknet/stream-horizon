package com.threeglav.sh.bauk.feed.bulk.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.CommandType;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

public final class NamedPipeBulkOutputWriter extends AbstractBulkOutputWriter {

	private static boolean namedPipesDeleted = false;

	private BufferedWriter writer;
	private String threadName;
	private final String bulkReadCommand;
	private String preparedBulkReadCommand;
	private Process bulkLoadingProcess;
	private final StatefulAttributeReplacer bulkReadCommandReplacer;

	public NamedPipeBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		this.validate();
		bulkReadCommand = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert().get(0).getCommand();
		if (StringUtil.isEmpty(bulkReadCommand)) {
			throw new IllegalArgumentException("Bulk insert command must not be null or empty string");
		}
		if (!bulkReadCommand.contains(BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER
				+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END)) {
			throw new IllegalArgumentException("Bulk insert command must use named pipe attribute " + BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER);
		}
		bulkReadCommandReplacer = new StatefulAttributeReplacer(bulkReadCommand, config.getDatabaseStringLiteral(),
				config.getDatabaseStringEscapeLiteral());
		this.deleteAllNamedPipes();
		final boolean isWindows = BaukUtil.isWindowsPlatform();
		if (isWindows) {
			log.error("Named pipes feature is not available on windows platform! Files will not be processed correctly!");
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

	private void deleteAllNamedPipes() {
		synchronized (NamedPipeBulkOutputWriter.class) {
			if (!namedPipesDeleted) {
				final String directory = this.getBulkFilePipeFolder();
				log.debug("Deleting all named pipes in [{}]", directory);
				final File dir = new File(directory);
				final File[] files = dir.listFiles();
				if (files != null) {
					int counter = 0;
					for (final File f : files) {
						if (f.getName().endsWith("pipe")) {
							log.debug("Deleting file {}", f.getName());
							final boolean deleted = f.delete();
							if (!deleted) {
								log.warn("Was not able to delete named pipe file {}. Please check permissions!", f.getName());
							} else {
								log.debug("Successfully deleted named pipe file {}", f.getName());
								counter++;
							}
						}
					}
					log.debug("Successfully deleted {} named pipe files", counter);
				}
				namedPipesDeleted = true;
			}
		}
	}

	private void createNamedPipe(final Map<String, String> globalAttributes) {
		final String fileName = this.getBulkFilePipeName();
		final File f = new File(fileName);
		final String command = "mkfifo " + fileName;
		if (!f.exists()) {
			log.debug("Named pipe [{}] does not exist. Will try to create it", fileName);
			try {
				final Process proc = Runtime.getRuntime().exec(command);
				try {
					final int returnedCode = proc.waitFor();
					log.debug("Successfully created named pipe {}. Returned code was {}", fileName, returnedCode);
				} catch (final InterruptedException e) {
					log.error("Exception while waiting for pipe to be created", e);
				}
			} catch (final IOException e) {
				log.error("Exception while creating named pipe {}. Details {}", fileName, e.getMessage());
				throw new IllegalStateException("Was not able to create named pipe " + fileName + ". Executed command was [" + command + "]", e);
			}
		}
		if (!f.exists()) {
			throw new IllegalStateException("Was not able to find named pipe " + fileName);
		}
		final Map<String, String> global = new HashMap<>();
		global.putAll(globalAttributes);
		global.put(BaukConstants.BULK_FILE_NAMED_PIPE_PLACEHOLDER, fileName);
		preparedBulkReadCommand = bulkReadCommandReplacer.replaceAttributes(global);
		if (isDebugEnabled) {
			log.debug("Prepared bulk command to read from named pipe is [{}]", preparedBulkReadCommand);
		}
	}

	private String getBulkFilePipeFolder() {
		final String dataDirectory = ConfigurationProperties.getDbDataFolder();
		final String directory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.NAMED_PIPE_LOCATION_DIRECTORY_PATH,
				dataDirectory);
		final File dir = new File(directory);
		if (!dir.exists() || !dir.isDirectory()) {
			throw new IllegalStateException("Unable to find directory [" + directory + "] to create named pipes!");
		}
		return directory;
	}

	private String getBulkFilePipeName() {
		final String directory = this.getBulkFilePipeFolder();
		return directory + "/bulkFile" + threadName + ".pipe";
	}

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
		if (threadName == null) {
			threadName = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID);
			this.createNamedPipe(globalAttributes);
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
				log.debug("Executing command [{}] in separate process", preparedBulkReadCommand);
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
