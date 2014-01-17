package com.threeglav.bauk.files;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BaukFile {

	private String fullFilePath;
	private String fileNameOnly;
	private long size;
	private long lastModifiedTime;
	private Path path;

	public String getFullFilePath() {
		return fullFilePath;
	}

	public void setFullFilePath(final String fullFilePath) {
		this.fullFilePath = fullFilePath;
	}

	public String getFileNameOnly() {
		return fileNameOnly;
	}

	public void setFileNameOnly(final String fileNameOnly) {
		this.fileNameOnly = fileNameOnly;
	}

	public long getSize() {
		return size;
	}

	public void setSize(final long size) {
		this.size = size;
	}

	public long getLastModifiedTime() {
		return lastModifiedTime;
	}

	public void setLastModifiedTime(final long lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	public void setPath(final Path path) {
		this.path = path;
	}

	public Path getPath() {
		return path;
	}

	public File asFile() {
		return new File(fullFilePath);
	}

	public void delete() throws IOException {
		Files.delete(path);
	}

}
