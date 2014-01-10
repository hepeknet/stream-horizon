package com.threeglav.bauk.camel;

import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.BaukUtil;

public class HashedNameFileFilter<T> implements GenericFileFilter<T> {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fileMask;
	private final int orderNum;
	private final int totalNumberOfFilters;
	private final boolean isDebugEnabled;

	public HashedNameFileFilter(final String fileMask, final int orderNum, final int totalNumberOfFilters) {
		this.fileMask = fileMask;
		this.orderNum = orderNum;
		this.totalNumberOfFilters = totalNumberOfFilters;
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public boolean accept(final GenericFile<T> file) {
		final boolean shutdownStarted = BaukUtil.shutdownStarted();
		if (shutdownStarted) {
			log.warn("Shutdown started. Will not accept any more files for processing!");
			return false;
		}
		final String fileName = file.getFileNameOnly();
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
		if (isDebugEnabled) {
			log.debug("{} matches pattern {}. Checking if this thread should process it!", fileName, fileMask);
		}
		final String fullFileName = file.getAbsoluteFilePath();
		int hash = fullFileName.hashCode();
		final long lastModifiedFile = file.getLastModified();
		final long fileSize = file.getFileLength();
		final int additionalSeed = (int) (lastModifiedFile / 31 + fileSize / 17);
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
