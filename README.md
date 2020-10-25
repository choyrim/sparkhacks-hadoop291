# S3 Workarounds for Spark on hadoop-aws 2.9.1

This package provides the following workarounds for hadoop-aws 2.9.1:
- support for assumed role credential provider
- fix for not respecting s3 bucket encryption settings on copy.
 [HADOOP-16794](https://issues.apache.org/jira/browse/HADOOP-16794)

## Install/setup

You will need to add this jar to your spark classpath.
One way is through the `spark.jars` property. For example,

```properties
spark.jars = path/to/sparkhacks-hadoop291-1.0-SNAPSHOT.jar
```

## Workaround 1 - Assume Role

To access an s3 bucket securely, the spark worker needs to assume an IAM role.
In the past, this was done through the `TemporaryAWSCredentialsProvider` which cannot refresh.
The `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider` can be refreshed but
was not implemented until hadoop-aws 3.1.
To support earlier versions, I've ported the class `AssumedRoleCredentialProvider` from the
`hadoop-aws` 3.1 branch and tested with the 2.9.1 version.

Since the code is mostly the same, the documentation for 3.1 applies

https://hadoop.apache.org/docs/r3.1.0/hadoop-aws/tools/hadoop-aws/assumed_roles.html

The main difference is that the credential_provider clas is
 `sparkhacks.AssumedRoleCredentialProviderForHadoop291` instead.

### Usage

Typical usage involves setting the hadoop properties as described in the 3.1 documentation.
For use in spark configuration, you will need to prefix the properties below with `spark.hadoop`.
In the following example, we will assume the role starting from the EC2 instance profile.

```properties
fs.s3a.aws.credentials.provider = sparkhacks.AssumedRoleCredentialProviderForHadoop291
fs.s3a.assumed.role.arn = <my-target-role-arn>
# start from EC2 instance profile IAM role.
fs.s3a.assumed.role.credentials.provider = com.amazonaws.auth.InstanceProfileCredentialsProvider
fs.s3a.assumed.role.session.name = my-spark-cluster
fs.s3a.assumed.role.session.duration = 1h
```

### Usage Per-Bucket

You can configure different roles per bucket (overriding the default).
You put "`bucket.<my-bucket-name>`" after the "`fs.s3a`" prefix.
For example, if your bucket name is my.bucket, then you could set a per-bucket configuration

```properties
fs.s3a.bucket.my.bucket.aws.credentials.provider = sparkhacks.AssumedRoleCredentialProviderForHadoop291
fs.s3a.bucket.my.bucket.assumed.role.arn = <my-target-role-arn>
fs.s3a.bucket.my.bucket.assumed.role.credentials.provider = com.amazonaws.auth.InstanceProfileCredentialsProvider
fs.s3a.bucket.my.bucket.assumed.role.session.name = my-spark-cluster
fs.s3a.bucket.my.bucket.assumed.role.session.duration = 1h
```

## Workaround 2 - Default Bucket Encryption Ignored

Writing a dataframe out to s3 from spark without explicitly specifying encryption will appear to ignore
 the default encryption settings of the s3 bucket. Although the _SUCCESS file will be encrypted using the
 default bucket encryption settings, the data files will be encrypted with the default key for the aws s3 service
 instead of the configured encryption.
This is due to a bug in the copy/rename function of the underlying hadoop-aws library -
[HADOOP-16794](https://issues.apache.org/jira/browse/HADOOP-16794).
Since the last step in committing the results of a write in spark involves renaming/moving data files,
the final output data files will ignore the default encryptions settings configured
for the bucket.

To remedy this bug, this package provides a port of the bug fix from branch 3.3.0 and tested on 2.9.1 and 3.2.1.

### Usage

To use, you must override the default s3 scheme implementations.

```properties
fs.s3a.impl = sparkhacks.S3AFileSystem2ForHadoop291
fs.s3n.impl = sparkhacks.S3AFileSystem2ForHadoop291
fs.s3.impl = sparkhacks.S3AFileSystem2ForHadoop291
```

For pyspark, I have found it necessary to set the hadoop properties after the spark session is created.
Setting spark configuration (`spark.hadoop.fs.*` properties) does not appear to work.

```python
for scheme in ["s3a", "s3n", "s3"]:
    spark.sparkContext._jsc.hadoopConfiguration().set(
        f"fs.{scheme}.impl",
        "sparkhacks.S3AFileSystem2ForHadoop291")
```
