package com.amazonaws.services.kinesis.flume;

/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Modifications: Copyright 2015 Sharethrough, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.RecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
 
public class KinesisSource extends AbstractSource implements Configurable, PollableSource {

  static AmazonKinesisClient kinesisClient;
  private static final Log LOG = LogFactory.getLog(KinesisSource.class);
  Worker worker;

  // Initial position in the stream when the application starts up for the first time.
  // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
  private InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON;
  private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";

  private KinesisClientLibConfiguration kinesisClientLibConfiguration;
  private String accessKeyId;
  private String secretAccessKey;
  private String streamName;
  private String applicationName;
  private String endpoint;
  private String initialPosition;

  @Override
  public void configure(Context context) {
    this.endpoint = context.getString("endpoint", DEFAULT_KINESIS_ENDPOINT);
    this.streamName = Preconditions.checkNotNull(
        context.getString("streamName"), "streamName is required");
    this.applicationName = Preconditions.checkNotNull(
        context.getString("applicationName"), "applicationName is required");

    this.initialPosition = context.getString("initialPosition", "TRIM_HORIZON");
    String workerId=null;

    if (this.initialPosition.equals("LATEST")){
      DEFAULT_INITIAL_POSITION=InitialPositionInStream.LATEST;
    }

    try {
      workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    LOG.info("Using workerId: " + workerId);

    kinesisClientLibConfiguration = new KinesisClientLibConfiguration(this.applicationName, this.streamName,
        new DefaultAWSCredentialsProviderChain(), workerId).
        withKinesisEndpoint(this.endpoint).
        withInitialPositionInStream(DEFAULT_INITIAL_POSITION);

  }

  @Override
  public void start() {
    IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory(getChannelProcessor());
    worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        System.out.println("Shutting down Kinesis client thread...");
        worker.shutdown();
      }
    });

    worker.run();
  }

  @Override
  public void stop () {
  }

  @Override
  public Status process() throws EventDeliveryException {
    return null;
  }
}
