package com.threeglav.bauk.dimension.cache;

public interface CacheInstance {

	public String getSurrogateKey(String naturalKey);

	public void put(String naturalKey, String surrogateKey);

}
