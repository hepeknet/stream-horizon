package com.threeglav.sh.bauk.dimension;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;
import com.threeglav.sh.bauk.model.Dimension;

public interface DimensionHandler extends BulkLoadOutputValueHandler {

	public Dimension getDimension();

}
