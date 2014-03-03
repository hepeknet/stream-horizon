package com.threeglav.sh.bauk.files;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.util.StringUtil;

public final class FileAttributesHashedNameFilter implements DirectoryStream.Filter<Path> {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String fileMask;
	private final int myThreadProcessingIdentifier;
	private final int totalNumberOfFileProcessingThreads;
	private final boolean isDebugEnabled;
	private final long fileAcceptanceTimeoutMillis;
	private final Pattern pattern;
	private final boolean partitionedMultiInstanceProcessing;
	private int totalMultipleInstances;
	private int currentInstanceIdentifier;

	public FileAttributesHashedNameFilter(final String fileMask, final int orderNum, final int totalNumberOfThreads,
			final int fileAcceptanceTimeoutMillis) {
		if (StringUtil.isEmpty(fileMask)) {
			throw new IllegalArgumentException("File mask must not be null or empty");
		}
		if (orderNum >= totalNumberOfThreads) {
			throw new IllegalArgumentException("Order must be lower than " + totalNumberOfThreads + ", currently it is " + orderNum);
		}
		this.fileMask = fileMask;
		pattern = Pattern.compile(fileMask);
		myThreadProcessingIdentifier = orderNum;
		totalNumberOfFileProcessingThreads = totalNumberOfThreads;
		this.fileAcceptanceTimeoutMillis = fileAcceptanceTimeoutMillis;
		isDebugEnabled = log.isDebugEnabled();
		partitionedMultiInstanceProcessing = ConfigurationProperties.isConfiguredPartitionedMultipleInstances();
		if (partitionedMultiInstanceProcessing) {
			totalMultipleInstances = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.MULTI_INSTANCE_PARTITION_COUNT_PARAM_NAME, -1);
			currentInstanceIdentifier = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, -1);
		}
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
		if (partitionedMultiInstanceProcessing) {
			if (isDebugEnabled) {
				log.debug("Multi instance partitioning enabled. Will check if I should process file {}", fileName);
			}
			int fileNameHash = fileName.hashCode();
			if (fileNameHash < 0) {
				fileNameHash = -fileNameHash;
			}
			final int hashMod = fileNameHash % totalMultipleInstances;
			if (hashMod == currentInstanceIdentifier) {
				log.info("File {} belongs to me", fileName);
			} else {
				if (isDebugEnabled) {
					log.debug("File {} does not belong to me. Some other instance will process it. Module of hash is {} my instance number is {}",
							fileName, hashMod, currentInstanceIdentifier);
				}
				return false;
			}
		}
		BasicFileAttributes bfa = null;
		try {
			bfa = Files.readAttributes(path, BasicFileAttributes.class);
		} catch (final NoSuchFileException nsfe) {
			// ignore, probably someone else took this file to process it
			return false;
		}
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
		final int res = hash % totalNumberOfFileProcessingThreads;
		final boolean hashesMatch = (res == myThreadProcessingIdentifier);
		if (isDebugEnabled) {
			log.debug("Checking if ({} % {} = {}) == {}",
					new Object[] { hash, totalNumberOfFileProcessingThreads, res, myThreadProcessingIdentifier });
		}
		if (!hashesMatch) {
			if (isDebugEnabled) {
				log.debug("Hash for {} does not match {}. Skipping", fileName, myThreadProcessingIdentifier);
			}
			return false;
		}
		if (isDebugEnabled) {
			log.debug("Will accept to process {}", fileName);
		}
		return true;
	}

}
