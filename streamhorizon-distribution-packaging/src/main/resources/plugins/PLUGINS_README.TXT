StreamHorizon dynamic plugins should be placed into this directory. 

Requirements:

1) JDK 1.7+ must be installed on the system and JAVA_HOME must point to installation directory
2) Plugin must be valid Java source files, with valid packages, imports etc.
3) Classpath must contain all imports found in any of the plugin source files
4) Plugin must not be copied to folder hierarchy reflecting their package structure. Just leave them in the root of this directory. You still can declare Java classes to be part of package. StreamHorizon will take care of everything else.
5) Plugin are referenced from engine-config.xml by their fully qualified Java name (including package name - if used)
6) Plugins can be compiled and placed in one of jar files in the classpath (recommedation is to use $ENGINE_HOME/ext-lib/), in which case they will be resolved from there instead of compiled from source code. If they are not compiled
then engine will try to compile them directly from Java source files.