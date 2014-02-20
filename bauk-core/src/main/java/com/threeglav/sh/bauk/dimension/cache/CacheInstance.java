package com.threeglav.sh.bauk.dimension.cache;

import java.util.Map;

public interface CacheInstance {

	public Integer getSurrogateKey(String naturalKey);

	public void put(String naturalKey, Integer surrogateKey);

	public void putAll(Map<String, Integer> values);

	public void clear();

}
