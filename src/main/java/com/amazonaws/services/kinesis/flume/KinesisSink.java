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
import java.util.ArrayList;

import com.google.common.base.Preconditions;
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
import org.apache.flume.instrumentation.SinkCounter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

public class KinesisSink extends AbstractSink implements Configurable {
  
  private static final Log LOG = LogFactory.getLog(KinesisSink.class);

  private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";
  private static final int DEFAULT_PARTITION_SIZE = 1;
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_MAX_ATTEMPTS = 100;

  private SinkCounter sinkCounter;

  static AmazonKinesisClient kinesisClient;
  private String accessKey;
  private String accessSecretKey;
  private String streamName;
  private String kinesisEndpoint;
  private int numberOfPartitions;
  private int batchSize;
  private int maxAttempts;
  
  @Override
  public void configure(Context context) {
    this.kinesisEndpoint = context.getString("kinesisEndpoint","https://kinesis.us-east-1.amazonaws.com");
    this.accessKey = Preconditions.checkNotNull(
        context.getString("accessKey"), "accessKey is required");
    this.accessSecretKey = Preconditions.checkNotNull(
        context.getString("accessSecretKey"), "accessSecretKey is required");
    this.streamName = Preconditions.checkNotNull(
        context.getString("streamName"), "streamName is required");

    this.numberOfPartitions = context.getInteger("kinesisPartitions", DEFAULT_PARTITION_SIZE);
    Preconditions.checkArgument(numberOfPartitions > 0,
        "numberOfPartitions must be greater than 0");

    this.batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);
    Preconditions.checkArgument(batchSize > 0 && batchSize <= 500,
        "batchSize must be between 1 and 500");

    this.maxAttempts = context.getInteger("maxAttempts", DEFAULT_MAX_ATTEMPTS);
    Preconditions.checkArgument(maxAttempts > 0,
        "maxAttempts must be greater than 0");

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public void start() {
    kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(this.accessKey,this.accessSecretKey));
    kinesisClient.setEndpoint(kinesisEndpoint);
    sinkCounter.start();
  }

  @Override
  public void stop () {
    sinkCounter.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    List<PutRecordsRequestEntry> putRecordsRequestEntryList = Lists.newArrayList();
    txn.begin();
    try {
      int txnEventCount = 0;
      int attemptCount = 1;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = ch.take();
        if (event == null) {
          break;
        }

        int partitionKey=new Random().nextInt(( numberOfPartitions - 1) + 1) + 1;
        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        entry.setData(ByteBuffer.wrap(event.getBody()));
        entry.setPartitionKey("partitionKey_"+partitionKey);
        putRecordsRequestEntryList.add(entry);
      }

      if (txnEventCount > 0) {
        if (txnEventCount == batchSize) {
          sinkCounter.incrementBatchCompleteCount();

        } else {
          sinkCounter.incrementBatchUnderflowCount();
        }

        sinkCounter.addToEventDrainAttemptCount(txnEventCount);
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName( this.streamName);
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

        while (putRecordsResult.getFailedRecordCount() > 0 && attemptCount < maxAttempts) {
          LOG.warn("Failed to sink " + putRecordsResult.getFailedRecordCount() + " records on attempt " + attemptCount + " of " + maxAttempts);
          final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
          final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
          for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
            final PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList.get(i);
            final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
            if (putRecordsResultEntry.getErrorCode() != null) {
              failedRecordsList.add(putRecordRequestEntry);
            }
          }
          putRecordsRequestEntryList = failedRecordsList;
          putRecordsRequest.setRecords(putRecordsRequestEntryList);
          putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
          attemptCount++;
        }

        if (putRecordsResult.getFailedRecordCount() > 0) {
          LOG.error("Failed to sink " + putRecordsResult.getFailedRecordCount() + " records after " + attemptCount + " out of " + maxAttempts + " attempt(s)");
          sinkCounter.incrementConnectionFailedCount();
        }

        int successfulRecords = txnEventCount = putRecordsResult.getFailedRecordCount();
        sinkCounter.addToEventDrainSuccessCount(successfulRecords);
      } else {
        sinkCounter.incrementBatchEmptyCount();
      }

      txn.commit();
      status = txnEventCount == 0 ? Status.BACKOFF : Status.READY;
    } catch (Throwable t) {
      txn.rollback();
      LOG.error("Failed to commit transaction. Transaction rolled back. ", t);
      status = Status.BACKOFF;
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
