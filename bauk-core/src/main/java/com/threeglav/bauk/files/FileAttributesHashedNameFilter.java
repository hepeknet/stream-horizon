package com.threeglav.bauk.files;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

public final class FileAttributesHashedNameFilter implements DirectoryStream.Filter<Path> {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fileMask;
	private final int orderNum;
	private final int totalNumberOfFilters;
	private final boolean isDebugEnabled;
	private final long fileAcceptanceTimeoutMillis;
	private final Pattern pattern;

	public FileAttributesHashedNameFilter(final String fileMask, final int orderNum, final int totalNumberOfFilters,
			final int fileAcceptanceTimeoutMillis) {
		if (StringUtil.isEmpty(fileMask)) {
			throw new IllegalArgumentException("File mask must not be null or empty");
		}
		if (orderNum >= totalNumberOfFilters) {
			throw new IllegalArgumentException("Order must be lower than " + totalNumberOfFilters + ", currently it is " + orderNum);
		}
		this.fileMask = fileMask;
		pattern = Pattern.compile(fileMask);
		this.orderNum = orderNum;
		this.totalNumberOfFilters = totalNumberOfFilters;
		this.fileAcceptanceTimeoutMillis = fileAcceptanceTimeoutMillis;
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public boolean accept(final Path path) throws IOException {
		final String fileName = path.getFileName().toString();
		final Matcher m = pattern.matcher(fileName);
		if (!m.matches()) {
			if (isDebugEnabled) {
				log.debug("{} does not match pattern {}. Skipping", fileName, fileMask);
			}
			return false;
		}
		final BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
		if (bfa.isDirectory()) {
			log.debug("{} is directory. Skipping", fileName);
			return false;
		}
		if (fileAcceptanceTimeoutMillis > 0) {
			final long lastModifiedFile = bfa.lastModifiedTime().toMillis();
			final long fileCreated = bfa.creationTime().toMillis();
			final long now = System.currentTimeMillis();
			if (now - lastModifiedFile < fileAcceptanceTimeoutMillis) {
				log.info("File {} was modified within last {}ms and will not accept to process it", fileName, fileAcceptanceTimeoutMillis);
				return false;
			}
			if (now - fileCreated < fileAcceptanceTimeoutMillis) {
				log.info("File {} was created within last {}ms and will not accept to process it", fileName, fileAcceptanceTimeoutMillis);
				return false;
			}
		}
		if (isDebugEnabled) {
			log.debug("{} matches pattern {}. Checking if this thread should process it!", fileName, fileMask);
		}
		final String fullFileName = path.toString();
		int hash = fullFileName.hashCode();
		final long fileSize = bfa.size();
		final int additionalSeed = (int) (fileSize / 17);
		hash = hash + additionalSeed * 11;
		if (hash < 0) {
			hash = -hash;
		}
		if (isDebugEnabled) {
			log.debug("Hash for {} is {}", fileName, hash);
		}
		final int res = hash % totalNumberOfFilters;
		final boolean hashesMatch = (res == orderNum);
		if (isDebugEnabled) {
			log.debug("Checking if ({} % {} = {}) == {}", new Object[] { hash, totalNumberOfFilters, res, orderNum });
		}
		if (!hashesMatch) {
			if (isDebugEnabled) {
				log.debug("Hash for {} does not match {}. Skipping", fileName, orderNum);
			}
			return false;
		}
		if (isDebugEnabled) {
			log.debug("Will accept to process {}", fileName);
		}
		return true;
	}

}
