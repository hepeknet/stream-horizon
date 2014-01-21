package com.threeglav.bauk.command;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.model.BaukCommand;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.CommandType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class BaukCommandsExecutor extends ConfigAware {

	public BaukCommandsExecutor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
	}

	private String executeShellCommand(final String command) {
		log.debug("Executing shell command [{}]", command);
		final StringBuffer output = new StringBuffer();
		try {
			final Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
			final BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			log.debug("Execution of [{}] finished with exit value [{}] and data [{}]", command, p.exitValue(), output.toString());
		} catch (final Exception e) {
			log.error("Exception while executing shell command", e);
		}
		return output.toString();
	}

	public void executeBaukCommandSequence(final ArrayList<BaukCommand> commands, final Map<String, String> attributes, final String description) {
		if (commands == null || commands.isEmpty()) {
			log.debug("No commands provided - nothing to execute");
			return;
		}
		log.debug("About to execute following {}", commands);
		for (final BaukCommand bc : commands) {
			if (bc.getType() == CommandType.SHELL) {
				this.executeShellCommand(bc.getCommand());
			} else if (bc.getType() == CommandType.SQL) {
				final String statement = bc.getCommand();
				String stat = statement;
				stat = StringUtil.replaceAllAttributes(stat, attributes, this.getConfig().getDatabaseStringLiteral(), this.getConfig()
						.getDatabaseStringEscapeLiteral());
				if (isDebugEnabled) {
					log.debug("Executing {} as part of {}", stat, description);
				}
				this.getDbHandler().executeInsertOrUpdateStatement(stat, description);
			}
		}
	}

}
