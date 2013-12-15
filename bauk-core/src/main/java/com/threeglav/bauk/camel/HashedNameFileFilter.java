package com.threeglav.bauk.camel;

import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashedNameFileFilter<T> implements GenericFileFilter<T> {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fileMask;
	private final int orderNum;
	private final int totalNumberOfFilters;

	public HashedNameFileFilter(final String fileMask, final int orderNum, final int totalNumberOfFilters) {
		this.fileMask = fileMask;
		this.orderNum = orderNum;
		this.totalNumberOfFilters = totalNumberOfFilters;
	}

	@Override
	public boolean accept(final GenericFile<T> file) {
		final String fileName = file.getFileNameOnly();
		if (file.isDirectory()) {
			log.debug("{} is directory. Skipping", fileName);
			return false;
		}
		if (!fileName.matches(fileMask)) {
			log.debug("{} does not match pattern {}. Skipping", fileName, fileMask);
		}
		log.debug("{} matches pattern {}", fileName, fileMask);
		int hash = fileName.hashCode();
		if (hash < 0) {
			hash = -hash;
		}
		log.debug("Hash for {} is {}", fileName, hash);
		final int res = hash % totalNumberOfFilters;
		final boolean hashesMatch = res == orderNum;
		log.debug("Checking if ({} % {} = {}) == {}", new Object[] { hash, totalNumberOfFilters, res, orderNum });
		if (!hashesMatch) {
			log.debug("Hash for {} does not match {}. Skipping", fileName, orderNum);
			return false;
		}
		log.debug("Will accept to process {}", fileName);
		return true;
	}

}
