package com.threeglav.bauk.files;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

public final class FileAttributesHashedNameFilter implements FileFilter {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fileMask;
	private final int orderNum;
	private final int totalNumberOfFilters;
	private final boolean isDebugEnabled;
	private final long createdTimeDifferenceMillis = 4000;
	private final long lastModifiedDifferenceMillis = 2000;

	public FileAttributesHashedNameFilter(final String fileMask, final int orderNum, final int totalNumberOfFilters) {
		if (StringUtil.isEmpty(fileMask)) {
			throw new IllegalArgumentException("File mask must not be null or empty");
		}
		if (orderNum >= totalNumberOfFilters) {
			throw new IllegalArgumentException("Order must be lower than " + totalNumberOfFilters + ", currently it is " + orderNum);
		}
		this.fileMask = fileMask;
		this.orderNum = orderNum;
		this.totalNumberOfFilters = totalNumberOfFilters;
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public boolean accept(final File file) {
		final String fileName = file.getName();
		if (file.isDirectory()) {
			log.debug("{} is directory. Skipping", fileName);
			return false;
		}
		if (!fileName.matches(fileMask)) {
			if (isDebugEnabled) {
				log.debug("{} does not match pattern {}. Skipping", fileName, fileMask);
			}
			return false;
		}
		try {
			final BasicFileAttributes bfa = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
			final long lastModifiedFile = bfa.lastModifiedTime().toMillis();
			final long fileCreated = bfa.creationTime().toMillis();
			final long now = System.currentTimeMillis();
			if (now - lastModifiedFile < lastModifiedDifferenceMillis) {
				log.info("File {} was modified within last {}ms and will not accept to process it", fileName, lastModifiedDifferenceMillis);
				return false;
			} else if (now - fileCreated < createdTimeDifferenceMillis) {
				log.info("File {} was created within last {}ms and will not accept to process it", fileName, createdTimeDifferenceMillis);
				return false;
			}
		} catch (final IOException ie) {
			log.error("Was not able to get file attributes for {}. Details {}", fileName, ie.getMessage());
		}
		if (isDebugEnabled) {
			log.debug("{} matches pattern {}. Checking if this thread should process it!", fileName, fileMask);
		}
		final String fullFileName = file.getAbsolutePath();
		int hash = fullFileName.hashCode();
		final long fileSize = file.length();
		final int additionalSeed = (int) (file.lastModified() / 31 + fileSize / 17);
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
