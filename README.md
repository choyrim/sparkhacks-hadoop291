# Workarounds for hadoop-aws 2.9.1

This package provides two workarounds for hadoop-aws 2.9.1:
- support for assumed role credential provider
- fix for not respecting s3 bucket encryption settings on copy.
 [HADOOP-16794](https://issues.apache.org/jira/browse/HADOOP-16794)

## Install/setup

You will need to add this jar to your spark classpath.
One way is through the `spark.jars` property. For example,

```properties
spark.jars = path/to/sparkhacks-hadoop291-1.0-SNAPSHOT.jar
```

## Workaround 1

This package provides a port of the `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider` from the
`hadoop-aws` 3.1.2 branch.

https://github.com/apache/hadoop/blob/rel/release-3.1.2/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/auth/AssumedRoleCredentialProvider.java

hadoop-aws 3.1 has support for role assumption as described in the following documentation

https://hadoop.apache.org/docs/r3.1.0/hadoop-aws/tools/hadoop-aws/assumed_roles.html

However, for earlier version, developers could only use the TemporaryAWSCredentialProvider.

###Usage

Typical usage involves setting the hadoop properties as described in the 3.1 documentation.
For use in spark configuration, you will need to prefix the properties below with `spark.hadoop`.

```properties
fs.s3a.aws.credentials.provider = sparkhacks.AssumedRoleCredentialProviderForHadoop291
fs.s3a.assumed.role.arn = <my-target-role-arn>
fs.s3a.assumed.role.credentials.provider = com.amazonaws.auth.InstanceProfileCredentialsProvider
fs.s3a.assumed.role.session.name = my-spark-cluster
fs.s3a.assumed.role.session.duration = 1h
```

## Workaround 2

This package provides a port of the fix for bug
[HADOOP-16794](https://issues.apache.org/jira/browse/HADOOP-16794)
which affects most spark output to s3.
The final rename/move operation will ignore bucket default encryption settings.

### Usage

To use, you must override the default s3 scheme implementations.

```properties
fs.s3a.impl = sparkhacks.S3AFileSystem2ForHadoop291
fs.s3n.impl = sparkhacks.S3AFileSystem2ForHadoop291
fs.s3.impl = sparkhacks.S3AFileSystem2ForHadoop291
```

For pyspark, I have found it necessary to set the hadoop properties after the spark session is created.
Using spark configuration (`spark.hadoop.fs.*` properties) does not appear to work.

```python
for scheme in ["s3a", "s3n", "s3"]:
    spark.sparkContext._jsc.hadoopConfiguration().set(
        f"fs.{scheme}.impl",
        "sparkhacks.S3AFileSystem2ForHadoop291")
```