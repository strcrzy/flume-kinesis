package com.amazonaws.services.kinesis.flume;

/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

public class KinesisSink extends AbstractSink implements Configurable {
  
  private static final Log LOG = LogFactory.getLog(KinesisSink.class);
  private String accessKey;
  private String accessSecretKey;
  private String streamName;
  static AmazonKinesisClient kinesisClient;
  private String numberOfPartitions;
  private String kinesisEndpoint;
  private long batchSize;
  
  @Override
  public void configure(Context context) {

    this.accessKey = context.getString("accessKey", "defaultValue");
    this.accessSecretKey = context.getString("accessSecretKey", "defaultValue");
    this.numberOfPartitions = context.getString("kinesisPartitions", "1");
    this.streamName = context.getString("streamName", "defaultValue");
    LOG.info(streamName);
    this.kinesisEndpoint = context.getString("kinesisEndpoint","https://kinesis.us-east-1.amazonaws.com");
    this.batchSize = context.getInteger("batchSize", 100);
    if (streamName.equals("defaultValue") || accessKey.equals("defaultValue")|| this.accessSecretKey.equals("defaultValue") || this.streamName.equals("defaultValue")){
      LOG.info("One Of The Required Config Param Is Missing: Accesseky,AccessSecretKey,StreamName");
      throw new Error();
    }

    if (batchSize < 1 || batchSize > 500) {
      LOG.error("Batch size must be between 1 and 500");
      throw new IllegalArgumentException("Batch size must be between 1 and 500");
    }
  }

  @Override
  public void start() {
    kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(this.accessKey,this.accessSecretKey));
    kinesisClient.setEndpoint(kinesisEndpoint);
  }

  @Override
  public void stop () {
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    // Start transaction
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    List<PutRecordsRequestEntry> records = Lists.newArrayList();
    txn.begin();
    try {
      int txnEventCount = 0;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = ch.take();
        if (event == null) {
          break;
        }

        int partitionKey=new Random().nextInt(( Integer.valueOf(numberOfPartitions)- 1) + 1) + 1;
        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        entry.setData(ByteBuffer.wrap(event.getBody()));
        entry.setPartitionKey("partitionKey_"+partitionKey);
        records.add(entry);
      }

      if (txnEventCount > 0) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName( this.streamName);
        putRecordsRequest.setRecords(records);
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
      }

      txn.commit();
      status = Status.READY;
    } catch (Throwable t) {
      txn.rollback();
      LOG.error("Failed to commit transaction. Transaction rolled back. ", t);
      status = Status.BACKOFF;
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error)t;
      } else {
        throw new EventDeliveryException(t);
      }
    } finally {
      txn.close();
    }
    return status;
  }
}
