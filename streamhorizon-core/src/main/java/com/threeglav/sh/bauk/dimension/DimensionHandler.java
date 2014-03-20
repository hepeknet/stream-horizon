package com.threeglav.sh.bauk.dimension;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;
import com.threeglav.sh.bauk.model.Dimension;

public interface DimensionHandler extends BulkLoadOutputValueHandler {

	public static final int NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION = -1;

	public Dimension getDimension();

	public int[] getMappedColumnPositionsInFeed();

}
