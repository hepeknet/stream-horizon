package com.threeglav.bauk.dimension.cache;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.util.BaukUtil;

public class InfinispanCacheInstanceManager implements CacheInstanceManager {

	private static final String INFINISPAN_XML_CONFIG_FILE_PATH = ConfigurationProperties.getConfigFolder() + "bauk-infinispan-config.xml";

	private static EmbeddedCacheManager manager;

	private synchronized static EmbeddedCacheManager getManager() {
		if (manager == null) {
			try {
				manager = new DefaultCacheManager(INFINISPAN_XML_CONFIG_FILE_PATH);
			} catch (final IOException ie) {
				BaukUtil.logEngineMessage("Exception while loading infinispan configuration " + ie.getMessage());
				System.exit(-1);
			}
		}
		return manager;
	}

	@Override
	public CacheInstance getCacheInstance(final String regionName) {
		final Cache<String, String> c = getManager().getCache(regionName);
		return new InfinispanCacheInstance(c);
	}

	@Override
	public void stop() {
		if (manager != null) {
			manager.stop();
		}
	}

}
