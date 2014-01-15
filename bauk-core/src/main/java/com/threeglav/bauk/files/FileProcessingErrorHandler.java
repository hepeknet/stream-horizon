package com.threeglav.bauk.files;

import java.io.File;
import java.io.IOException;

public interface FileProcessingErrorHandler {

	void handleError(File f, Exception exc) throws IOException;

}
