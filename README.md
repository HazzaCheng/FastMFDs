# FastMFDs

FastMFDs is an efficient and scalable functional dependency discovery algorithm on distributed data-parallel platforms. In addition, FastMFDs is built on the widely-used distributed data-parallel platform Apache Spark.

# Environment

- Apache Spark: Spark 2.10.4
- Apache HDFS: FastMFDs uses HDFS as the distributed file system. The HDFS version is 2.6.5.
- Java: The JDK version is 1.8
- Scala: The Scala SDK version is 2.10.4

# Compile

FastMFDs is built using Apache Maven.
To build FastMFDs, Run `mvn scala:compile compile package` in the root directory.

# Run

The entry of FastMFDs is defined in the scala class `com.hazzacheng.FD.Main`.

Run FastMFDs with the following command:
```
    spark-submit \
        -v \
        --master [SPARK_MASTER_ADDRESS] \
        --name "FastMFDs" \
        --class "com.hazzacheng.AR.Main" \
        --executor-cores 4 \
        --executor-memory 20G \
        --driver-memory 20G \
        FD.jar \
        <Input path> \
        <Output path> \
        <Temporary path> \
```

The run command contains the following parameters:

- `<Input path>`: The input data path on HDFS or local file system.
- `<Output path>`: The output data path on HDFS or local file system.
- `<Temporary path>`: The tempoaray data path on HDFS or local file system.
