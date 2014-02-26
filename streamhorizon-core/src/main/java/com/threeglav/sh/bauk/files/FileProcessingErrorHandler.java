package com.threeglav.sh.bauk.files;

import java.nio.file.Path;

public interface FileProcessingErrorHandler {

	void handleError(Path path, Exception exc);

}
