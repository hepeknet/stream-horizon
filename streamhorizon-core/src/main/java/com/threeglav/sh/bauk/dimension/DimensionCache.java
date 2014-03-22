package com.threeglav.sh.bauk.dimension;

import java.util.List;
import java.util.Observable;

public interface DimensionCache {

	public abstract Integer getSurrogateKeyFromCache(String cacheKey);

	public abstract int putAllInCache(List<DimensionKeysPair> values);

	public abstract void putInCache(String cacheKey, int cachedValue);

	public abstract void removeFromCache(String cacheKey);

	public abstract void update(Observable o, Object arg);

}