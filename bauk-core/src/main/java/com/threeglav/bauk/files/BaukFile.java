package com.threeglav.bauk.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StreamUtil;

public class BaukFile {

	private static final Logger LOG = LoggerFactory.getLogger(BaukFile.class);

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

	public InputStream getInputStream() throws IOException {
		final String fullFileName = this.getFullFilePath();
		final String lowerCaseFilePath = fullFileName.toLowerCase();
		if (lowerCaseFilePath.endsWith(".zip")) {
			final ZipFile zipFile = new ZipFile(this.asFile());
			if (!zipFile.entries().hasMoreElements()) {
				IOUtils.closeQuietly(zipFile);
				throw new IllegalStateException("Did not find any zip entries inside " + fullFileName);
			}
			if (zipFile.size() > 1) {
				LOG.error("Will process only one zipped entry inside {} and will skip all others. Found {} entries", fullFileName, zipFile.size());
			}
			final InputStream inputStream = zipFile.getInputStream(zipFile.entries().nextElement());
			return inputStream;
		} else if (lowerCaseFilePath.endsWith(".gz")) {
			final InputStream fileInputStream = new FileInputStream(this.asFile());
			final InputStream inputStream = StreamUtil.ungzipInputStream(fileInputStream);
			return inputStream;
		} else {
			final InputStream inputStream = new FileInputStream(this.asFile());
			return inputStream;
		}
	}

}
