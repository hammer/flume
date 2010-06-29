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
package com.cloudera.flume.handlers.http;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.DebugOutputFormat;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.flume.handlers.text.output.RawOutputFormat;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This sends an HTTP post. Data is written in the format that is
 * specified. If there no/null format is specified, the default toString format
 * is used.
 */
public class HttpPostSink extends EventSink.Base {
  final static Logger LOG = Logger.getLogger(HttpPostSink.class.getName());

  String serverAddress = null;
  final OutputFormat fmt;
  long count = 0;

  public HttpPostSink(String serverAddress) {
    this(serverAddress, null);
  }

  public HttpPostSink(String serverAddress, OutputFormat fmt) {
    this.serverAddress = serverAddress;
    this.fmt = (fmt == null) ? new RawOutputFormat() : fmt;
  }

  @Override
  synchronized public void append(Event e) throws IOException {
    // Construct data
    String messageData = URLEncoder.encode("body", "UTF-8") + "=" + URLEncoder.encode(new String(e.getBody()), "UTF-8");
    String userCookie = "user=flume";

    // Send data
    URL serverURL = new URL(this.serverAddress);
    HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
    connection.setDoOutput(true);
    connection.setRequestProperty("Cookie", userCookie);
    connection.setRequestMethod("POST");
    OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
    wr.write(messageData);
    wr.flush();
    connection.getInputStream();
    connection.disconnect();

    // Keep track of how many events we've appended for the reporter
    count++;
  }

  @Override
  synchronized public void close() throws IOException {
    return;
  }

  @Override
  synchronized public void open() throws IOException {
    return;
  }

  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = super.getReport();
    Attributes.setLong(rpt, ReportEvent.A_COUNT, count);
    return rpt;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        Preconditions.checkArgument(args.length >= 1 && args.length <= 2,
            "usage: httpPost(url[,format])");
        OutputFormat fmt = new DebugOutputFormat();
        if (args.length >= 2) {
          try {
            fmt = FormatFactory.get().getOutputFormat(args[1]);
          } catch (FlumeSpecException e) {
            LOG.error("Illegal output format " + args[1], e);
            throw new IllegalArgumentException("Illegal output format"
                + args[1]);

          }
        }
        return new HttpPostSink(args[0], fmt);
      }
    };
  }
}
