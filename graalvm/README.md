This example builds Debezium embedded Oracle connector using native-image utility from graalvm. The resulting output is a native shared library (libebeziumshared.so) that contains startEngine() and stopEngine() API to start or stop Oracle replication. The Debezium configurations are hardcoded in this example. This native shared library can then be linked by a native C application or even a PostgreSQL extension and utilized to access Debezium resources without the need for Java or JVM. This is a feasibility study and POC and not yet meant for production.

### download graalvm community jdk
I am using version 21.0.2 community version for x86-64 platform:
```
wget https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-21.0.2/graalvm-community-jdk-21.0.2_linux-x64_bin.tar.gz
tar xzvf graalvm-community-jdk-21.0.2_linux-x64_bin.tar.gz

export JAVA_HOME=${PWD}/graalvm-community-openjdk-21.0.2+13.1
export GRAALVM_HOME=$JAVA_HOME
export PATH=${JAVA_HOME}/bin:${PATH}
```

Make sure there is no conflicting java versions in PATH.

### Build the Java application into a shaded .jar
```
mvn clean package
```

### Build the native library based on the shaded .jar
```
native-image \
  --no-fallback \
  --shared \
  --enable-native-access=all-unnamed \
  -H:+UnlockExperimentalVMOptions \
  -H:Name=libdebeziumshared \
  -H:Class=io.debezium.nativebridge.DebeziumSharedLib \
  --initialize-at-run-time=java.sql.DriverManager,oracle.jdbc.OracleDriver \
  --initialize-at-build-time=\
org.slf4j.LoggerFactory,\
org.slf4j.impl.StaticLoggerBinder,\
org.apache.log4j.Logger,\
org.apache.log4j.Category,\
org.apache.log4j.LogManager,\
org.apache.log4j.spi.RootLogger,\
org.apache.log4j.helpers.LogLog,\
org.apache.log4j.helpers.Loader,\
org.apache.log4j.Layout,\
org.slf4j.impl.Reload4jLoggerFactory,\
org.slf4j.impl.Reload4jLoggerAdapter,\
java.beans.Introspector,\
java.beans.ThreadGroupContext,\
com.sun.beans.TypeResolver,\
com.sun.beans.introspect.MethodInfo,\
com.sun.beans.introspect.ClassInfo \
  -H:ReflectionConfigurationFiles=reflect-config.json \
  -H:IncludeResources=io/debezium/connector/oracle/build.version \
  -H:IncludeResources=log4j.properties \
  -cp target/debezium-native-shared-1.0-SNAPSHOT-all.jar
```

Please note that this build is memory intensive and will take some time. I suggest assigning 6 ~ 8 GB of memory for this build process. Insufficient memory would cause the build to be killed by OS.

### Build C app that links to libdebeziumshared.so
```
gcc -o testapp testapp.c -L. -Wl,-rpath=. -ldebeziumshared -ldl -lpthread -lz
```

### Run the app to observe change events
the example starts debezium engine and shuts it down after a set time. 
```
./testapp
```
