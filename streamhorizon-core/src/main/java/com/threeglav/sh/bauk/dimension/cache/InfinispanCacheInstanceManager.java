package com.threeglav.sh.bauk.dimension.cache;

import java.io.File;
import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.util.BaukUtil;

public class InfinispanCacheInstanceManager implements CacheInstanceManager {

	private static final Logger LOG = LoggerFactory.getLogger(InfinispanCacheInstanceManager.class);

	private static final String INFINISPAN_XML_CONFIG_FILE_PATH = ConfigurationProperties.getConfigFolder() + "sh-infinispan-config.xml";

	private static EmbeddedCacheManager manager;

	private synchronized static EmbeddedCacheManager getManager() {
		if (manager == null) {
			try {
				final File f = new File(INFINISPAN_XML_CONFIG_FILE_PATH);
				if (!(f.exists() && f.isFile())) {
					LOG.warn(
							"Was not able to find infinispan configuration file [{}]. Will use default configuration! This might cause performance issues!",
							f.getAbsolutePath());
					manager = new DefaultCacheManager();
				} else {
					manager = new DefaultCacheManager(INFINISPAN_XML_CONFIG_FILE_PATH);
				}
			} catch (final IOException ie) {
				BaukUtil.logEngineMessage("Exception while loading infinispan configuration " + ie.getMessage());
				System.exit(-1);
			}
		}
		return manager;
	}

	@Override
	public CacheInstance getCacheInstance(final String regionName) {
		final Cache<String, Integer> c = getManager().getCache(regionName);
		return new InfinispanCacheInstance(c);
	}

	@Override
	public void stop() {
		if (manager != null) {
			manager.stop();
		}
	}

}
