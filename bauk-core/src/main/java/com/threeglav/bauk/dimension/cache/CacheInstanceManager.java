package com.threeglav.bauk.dimension.cache;

public interface CacheInstanceManager {

	public CacheInstance getCacheInstance(String regionName);

	public void stop();

}
