package com.threeglav.sh.bauk.command;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.dimension.db.DbHandler;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.CommandType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;

public final class BaukCommandsExecutor extends ConfigAware {

	private DbHandler databaseHandler;
	private final StatefulAttributeReplacer[] replacers;
	private final ArrayList<BaukCommand> commands;
	private boolean hasSqlStatementsToExecute;

	public BaukCommandsExecutor(final Feed factFeed, final BaukConfiguration config, final ArrayList<BaukCommand> commands) {
		super(factFeed, config);
		if (commands == null) {
			throw new IllegalArgumentException("Commands must not be null");
		}
		this.commands = commands;
		replacers = new StatefulAttributeReplacer[commands.size()];
		for (int i = 0; i < commands.size(); i++) {
			final BaukCommand cmd = commands.get(i);
			replacers[i] = new StatefulAttributeReplacer(cmd.getCommand(), config.getDatabaseStringLiteral(), config.getDatabaseStringEscapeLiteral());
			if (cmd.getType() == CommandType.SQL) {
				hasSqlStatementsToExecute = true;
			}
		}
		if (hasSqlStatementsToExecute) {
			databaseHandler = this.getDbHandler();
		}
	}

	private boolean executeShellCommand(final String command, final Map<String, String> attributes) {
		if (isDebugEnabled) {
			log.debug("Executing shell command [{}]", command);
		}
		final StringBuffer output = new StringBuffer();
		int outputResult = -1;
		try {
			final Process p = Runtime.getRuntime().exec(command);
			outputResult = p.waitFor();
			final BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			if (isDebugEnabled) {
				log.debug("Execution of [{}] finished with exit value [{}] and data [{}]", command, p.exitValue(), output.toString());
			}
		} catch (final Exception e) {
			log.error("Command that was executed was {}. All available context attributes used for replacement are {}", command, attributes);
			log.error("Exception while executing shell command", e);
		}
		return outputResult == 0;
	}

	public void executeBaukCommandSequence(final Map<String, String> attributes, final String description) {
		int counter = 0;
		for (final BaukCommand bc : commands) {
			final StatefulAttributeReplacer replacer = replacers[counter++];
			final String replacedCommand = replacer.replaceAttributes(attributes);
			if (bc.getType() == CommandType.SHELL) {
				this.executeShellCommand(replacedCommand, attributes);
			} else if (bc.getType() == CommandType.SQL) {
				if (isDebugEnabled) {
					log.debug("Executing {} as part of {}", replacedCommand, description);
				}
				databaseHandler.executeInsertOrUpdateStatement(replacedCommand, description);
			}
		}
	}

}
