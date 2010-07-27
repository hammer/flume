/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.handlers.debug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.CappedExponentialBackoff;
import com.cloudera.util.CumulativeCappedExponentialBackoff;
import com.cloudera.util.MultipleIOException;
import com.google.common.base.Preconditions;

/**
 * This sink decorator attempts to retry opening for up to max millis. This is
 * useful for starting several nodes simultaneously in a situation where a
 * server may be slower to start than a client that wants to make a connection.
 */
public class InsistentOpenDecorator<S extends EventSink> extends
    EventSinkDecorator<S> implements Reportable {
  final static Logger LOG = Logger.getLogger(InsistentOpenDecorator.class);
  final BackoffPolicy backoff;

  // attribute names
  final public static String A_INITIALSLEEP = "intialSleep";
  final public static String A_MAXSLEEP = "maxSleep";
  final public static String A_ATTEMPTS = "openAttempts";
  final public static String A_REQUESTS = "openRequests";
  final public static String A_SUCCESSES = "openSuccessses";
  final public static String A_RETRIES = "openRetries";
  final public static String A_GIVEUPS = "openGiveups";

  long openRequests; // # of times open was called
  long openAttempts; // # of of times
  long openSuccesses; // # of times we successfully opened
  long openRetries; // # of times we tried to reopen
  long openGiveups; // # of times we gave up on waiting

  public InsistentOpenDecorator(S s, BackoffPolicy backoff) {
    super(s);
    this.backoff = backoff;
    this.openSuccesses = 0;
    this.openRetries = 0;
  }

  /**
   * Creates a deco that has subsink s, and after failure initially waits for
   * 'initial' ms, exponentially backs off an individual sleep upto 'sleepCap'
   * ms, and fails after total backoff time has reached 'cumulativeCap' ms.
   */
  public InsistentOpenDecorator(S s, long sleepCap, long initial,
      long cumulativeCap) {
    super(s);
    this.backoff = new CumulativeCappedExponentialBackoff(initial, sleepCap,
        cumulativeCap);

    this.openSuccesses = 0;
    this.openRetries = 0;
  }

  /**
   * Creates a deco that has subsink s, and after failure initially waits for
   * 'initial' ms, exponentially backs off an individual sleep upto 'sleepCap'
   * ms. This has no cumulative cap and will never give up.
   */
  public InsistentOpenDecorator(S s, long sleepCap, long initial) {
    super(s);
    this.backoff = new CappedExponentialBackoff(initial, sleepCap);
    this.openSuccesses = 0;
    this.openRetries = 0;
  }

  @Override
  synchronized public void open() throws IOException {
    List<IOException> exns = new ArrayList<IOException>();
    int attemptRetries = 0;
    openRequests++;
    while (!backoff.isFailed()) {
      try {
        openAttempts++;
        super.open();
        openSuccesses++;
        backoff.reset(); // reset backoff counter;
        LOG.info("Opened " + sink.getName() + " on try " + attemptRetries);
        return;
      } catch (IOException e) {
        long waitTime = backoff.sleepIncrement();
        LOG.info("open attempt " + attemptRetries + " failed, backoff ("
            + waitTime + "ms): " + e.getMessage());
        LOG.debug(e.getMessage(), e);
        exns.add(e);
        backoff.backoff();
        try {
          backoff.waitUntilRetryOk();
        } catch (InterruptedException e1) {
        }
        attemptRetries++;
        openRetries++;
      }
    }
    openGiveups++;
    // failed to start
    throw MultipleIOException.createIOException(exns);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        long initMs = FlumeConfiguration.get().getInsistentOpenInitBackoff();
        long cumulativeMaxMs = FlumeConfiguration.get()
            .getFailoverMaxCumulativeBackoff();
        long maxMs = FlumeConfiguration.get().getFailoverMaxSingleBackoff();

        Preconditions.checkArgument(argv.length <= 3,
            "usage: insistentOpen([max=" + maxMs + "[,init=" + initMs
                + "[,cumulativeMax=maxint]]])");

        if (argv.length >= 1) {
          maxMs = Long.parseLong(argv[0]);
        }
        if (argv.length >= 2) {
          initMs = Long.parseLong(argv[1]);
        }
        if (argv.length == 3) {
          cumulativeMaxMs = Long.parseLong(argv[2]);
          // This one can give up
          return new InsistentOpenDecorator<EventSink>(null,
              new CumulativeCappedExponentialBackoff(initMs, maxMs,
                  cumulativeMaxMs));
        }

        return new InsistentOpenDecorator<EventSink>(null,
            new CappedExponentialBackoff(maxMs, initMs));
      }

    };
  }

  @Override
  public String getName() {
    return "InsistentOpen";
  }

  @Override
  public synchronized ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    // parameters
    rpt.hierarchicalMerge(backoff.getName(), backoff.getReport());

    // counters
    rpt.setLongMetric(A_REQUESTS, openRequests);
    rpt.setLongMetric(A_ATTEMPTS, openAttempts);
    rpt.setLongMetric(A_SUCCESSES, openSuccesses);
    rpt.setLongMetric(A_RETRIES, openRetries);
    rpt.setLongMetric(A_GIVEUPS, openGiveups);
    return rpt;
  }
}
