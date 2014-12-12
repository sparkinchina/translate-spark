# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and structured
data processing, MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>

Spark是一个快速的、通用的大数据集群计算系统。
它提供了高层API接口(支持Scala，Java及Python)以及一个优化过的支持图计算和数据处理的引擎。
同时它也一些高级工具也提供了支持，包括对SQL及结构化数据处理的Spark SQL， 机器学习的MLLib，
图像处理的GraphX和实时流处理的Spark Streaming

## Online Documentation (在线文档)

You can find the latest Spark documentation, including a programming
guide, on the project webpage at <http://spark.apache.org/documentation.html>.
This README file only contains basic setup instructions.

在Spark的项目主页上，你可以找到最新的Spark文档，其中还包括一个编程指南。
该 README 文档仅仅包含了基础的安装信息

## Building Spark (创建Spark)

Spark is built on Scala 2.10. To build Spark and its example programs, run:

    ./sbt/sbt assembly

(You do not need to do this if you downloaded a pre-built package.)

Spark是基于Scala 2.10创建的。要创建Spark及其示例程序，运行如下命令：
  ./sbt/sbt assembly
(如果你下载了一个预编译包，你完全没有必要这么做)
(--鉴于国内的网络情况，强烈建议大家下载预编译包---译者注)

## Interactive Scala Shell (交互式 Scala Shell)

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

使用Spark最简单的方法是Scala Shell：
   ./bin/spark-shell
试试下面的命令，其执行结果应该是1000：
    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell (交互式 Python Shell)

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark
    
And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs (示例程序)

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL, 
"yarn-cluster" or "yarn-client" to run on YARN, and "local" to run 
locally with one thread, or "local[N]" to run locally with N threads. You 
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

## A Note About Hadoop Versions (关于Hadoop版本)

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting `-Dhadoop.version` when building Spark.

For Apache Hadoop versions 1.x, Cloudera CDH MRv1, and other Hadoop
versions without YARN, use:

    # Apache Hadoop 1.2.1
    $ sbt/sbt -Dhadoop.version=1.2.1 assembly

    # Cloudera CDH 4.2.0 with MapReduce v1
    $ sbt/sbt -Dhadoop.version=2.0.0-mr1-cdh4.2.0 assembly

For Apache Hadoop 2.2.X, 2.1.X, 2.0.X, 0.23.x, Cloudera CDH MRv2, and other Hadoop versions
with YARN, also set `-Pyarn`:

    # Apache Hadoop 2.0.5-alpha
    $ sbt/sbt -Dhadoop.version=2.0.5-alpha -Pyarn assembly

    # Cloudera CDH 4.2.0 with MapReduce v2
    $ sbt/sbt -Dhadoop.version=2.0.0-cdh4.2.0 -Pyarn assembly

    # Apache Hadoop 2.2.X and newer
    $ sbt/sbt -Dhadoop.version=2.2.0 -Pyarn assembly

When developing a Spark application, specify the Hadoop version by adding the
"hadoop-client" artifact to your project's dependencies. For example, if you're
using Hadoop 1.2.1 and build your application using SBT, add this entry to
`libraryDependencies`:

    "org.apache.hadoop" % "hadoop-client" % "1.2.1"

If your project is built with Maven, add this to your POM file's `<dependencies>` section:

    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>1.2.1</version>
    </dependency>


## A Note About Thrift JDBC server and CLI for Spark SQL (Spark SQL中关于 Thrift JDBC server and CLI)

Spark SQL supports Thrift JDBC server and CLI.
See sql-programming-guide.md for more information about using the JDBC server and CLI.
You can use those features by setting `-Phive` when building Spark as follows.

    $ sbt/sbt -Phive  assembly

## Configuration (配置)

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

 如何配置Spark，请参考线上文档配置指南

## Contributing to Spark (为Spark做贡献)

Contributions via GitHub pull requests are gladly accepted from their original
author. Along with any pull requests, please state that the contribution is
your original work and that you license the work to the project under the
project's open source license. Whether or not you state this explicitly, by
submitting any copyrighted material via pull request, email, or other means
you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.

项目开发者非常乐意接收通过GitHub 发送的贡献请求。请在提交的请求中注明系本人首创，并且您
遵从该项目的开源协议。不管你是否有明确的版权声明（通过....
