package com.threeglav.bauk.dimension.cache;

public interface CacheInstance {

	public Integer getSurrogateKey(String naturalKey);

	public void put(String naturalKey, Integer surrogateKey);

}
