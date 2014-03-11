package com.threeglav.sh.bauk.command;

import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.CommandType;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.BaukUtil;

public class BaukCommandsExecutorTest {

	@Test
	public void testWindowsNoReplacement() throws Exception {
		final boolean isWindows = BaukUtil.isWindowsPlatform();
		final FactFeed ff = Mockito.mock(FactFeed.class);
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		when(config.getDatabaseStringLiteral()).thenReturn("'");
		when(config.getDatabaseStringEscapeLiteral()).thenReturn("''");
		final ArrayList<BaukCommand> commands = new ArrayList<>();
		final String newFileName = UUID.randomUUID().toString();
		final String newFileExtension = "bauk";
		final File tempDir = new File(System.getProperty("java.io.tmpdir"));
		final File nonExistingFile = new File(tempDir, newFileName + "." + newFileExtension);
		Assert.assertFalse(nonExistingFile.exists());
		if (isWindows) {
			final File f = this.createFileWithText("echo. 2>%1.%2", ".bat");
			Assert.assertTrue(f.exists());
			final BaukCommand cmd = new BaukCommand();
			cmd.setType(CommandType.SHELL);
			cmd.setCommand(f.getAbsolutePath() + " " + tempDir.getAbsolutePath() + "/" + newFileName + " " + newFileExtension);
			commands.add(cmd);
			final BaukCommandsExecutor bce = new BaukCommandsExecutor(ff, config, commands);
			bce.executeBaukCommandSequence(new HashMap<String, String>(), "desc");
			Assert.assertTrue(nonExistingFile.exists());
			nonExistingFile.delete();
			f.delete();
		}
	}

	@Test
	public void testWindowsReplacement() throws Exception {
		final boolean isWindows = BaukUtil.isWindowsPlatform();
		final FactFeed ff = Mockito.mock(FactFeed.class);
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		when(config.getDatabaseStringLiteral()).thenReturn("'");
		when(config.getDatabaseStringEscapeLiteral()).thenReturn("''");
		final ArrayList<BaukCommand> commands = new ArrayList<>();
		final String newFileName = UUID.randomUUID().toString();
		final String newFileExtension = "baukAttr";
		final File tempDir = new File(System.getProperty("java.io.tmpdir"));
		final File nonExistingFile = new File(tempDir, newFileName + "." + newFileExtension);
		Assert.assertFalse(nonExistingFile.exists());
		if (isWindows) {
			final File f = this.createFileWithText("echo. 2>%1.%2", ".bat");
			Assert.assertTrue(f.exists());
			final BaukCommand cmd = new BaukCommand();
			cmd.setType(CommandType.SHELL);
			cmd.setCommand(f.getAbsolutePath() + " " + tempDir.getAbsolutePath() + "/" + newFileName + " ${ext}");
			commands.add(cmd);
			final BaukCommandsExecutor bce = new BaukCommandsExecutor(ff, config, commands);
			final Map<String, String> attrs = new HashMap<String, String>();
			attrs.put("ext", newFileExtension);
			bce.executeBaukCommandSequence(attrs, "desc");
			Assert.assertTrue(nonExistingFile.exists());
			nonExistingFile.delete();
			f.delete();
		}
	}

	private File createFileWithText(final String text, final String sufix) throws Exception {
		final Path batchFile = Files.createTempFile("bauk_exec", sufix);
		final File f = batchFile.toFile();
		f.deleteOnExit();
		final BufferedWriter bw = new BufferedWriter(new FileWriter(f));
		bw.write(text);
		bw.close();
		return f;
	}

}
