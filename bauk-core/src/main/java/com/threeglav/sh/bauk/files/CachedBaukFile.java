package com.threeglav.sh.bauk.files;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.apache.commons.io.IOUtils;

public class CachedBaukFile extends BaukFile {

	private byte[] fileContent;
	private final BaukFile originalFile;

	public CachedBaukFile(final BaukFile originalFile) {
		if (originalFile == null) {
			throw new IllegalArgumentException("Original file must not be null");
		}
		this.originalFile = originalFile;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		if (fileContent == null) {
			final InputStream originalInputStream = originalFile.getInputStream();
			fileContent = IOUtils.toByteArray(originalInputStream);
			IOUtils.closeQuietly(originalInputStream);
		}
		return new ByteArrayInputStream(fileContent);
	}

	@Override
	public String getFullFilePath() {
		return originalFile.getFullFilePath();
	}

	@Override
	public String getFileNameOnly() {
		return originalFile.getFileNameOnly();
	}

	@Override
	public long getSize() {
		return originalFile.getSize();
	}

	@Override
	public long getLastModifiedTime() {
		return originalFile.getLastModifiedTime();
	}

	@Override
	public Path getPath() {
		return originalFile.getPath();
	}

	@Override
	public File asFile() {
		return originalFile.asFile();
	}

	@Override
	public void delete() throws IOException {
		originalFile.delete();
	}

}
