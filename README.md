# flume-kinesis

Amazon Kinesis Source and Sink for Apache Flume

This was originally forked from: https://github.com/pdeyhim/flume-kinesis.  This version is updated
for newer version of the AWS API.

## Building and installation

```
mvn compile assembly:single
cp target/*.jar FLUME_HOME_DIR/lib
```

## Configuration

Check the examples under `conf/` for specific examples.  All values without defaults are required.

### Kinesis Source Options

|Name|Default|Description|
-------|-----------|-------------|
|kinesisEndpoint|https://kinesis.us-east-1.amazonaws.com|endpoint to access kinesis|
|accessKey|null|AWS Access Key ID|
|secretAccessKey|null|AWS Secret Access Key|
|kinesisStreamName|null|name of Kinesis stream|
|kinesisApplicationName|null|name of Kinesis application|
|initialPosition|TRIM_HORIZON|strategy to set the initial iterator position|

### Kinesis Sink Options

|Name|Default|Description|
-------|-----------|-------------|
|kinesisEndpoint|https://kinesis.us-east-1.amazonaws.com|endpoint to access kinesis|
|accessKey|null|AWS Access Key ID|
|secretAccessKey|null|AWS Secret Access Key|
|streamName|null|name of Kinesis stream|
|kinesisPartitions|1|number of Kinesis partitions|
|batchSize|100|max number of events to send per API call to Kinesis.  Must be between 1 and 500.|

