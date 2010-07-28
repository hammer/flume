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
import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This sends an HTTP post. Data is written in the format that is
 * specified. If there no/null format is specified, the default toString format
 * is used.
 */
public class HttpPostSink extends EventSink.Base {
  String serverAddress = null;
  String paramWithData = null;
  String userCookie = null;
  long count = 0;

  public HttpPostSink(String serverAddress, String paramWithData) {
    this(serverAddress, paramWithData, "");
  }

  public HttpPostSink(String serverAddress, String paramWithData, String userCookie) {
    this.serverAddress = serverAddress;
    this.paramWithData = paramWithData;
    this.userCookie = userCookie;
  }

  @Override
  synchronized public void append(Event e) throws IOException {
    // Construct data
    String messageData = URLEncoder.encode(this.paramWithData, "UTF-8") + "=" + URLEncoder.encode(new String(e.getBody()), "UTF-8");
    //String userCookie = "user=flume";

    // Send data
    URL serverURL = new URL(this.serverAddress);
    HttpURLConnection connection = (HttpURLConnection) serverURL.openConnection();
    connection.setDoOutput(true);
    if (this.userCookie != "") {
      connection.setRequestProperty("Cookie", this.userCookie);
    }
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
    rpt.setLongMetric(ReportEvent.A_COUNT, count);
    return rpt;
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        Preconditions.checkArgument(args.length >= 2 && args.length <= 3,
            "usage: httpPost(url, param[, cookie])");
        String serverAddress = args[0];
        String paramWithData = args[1];
        if (args.length ==2) {
	  return new HttpPostSink(serverAddress, paramWithData);
	} else {
          String userCookie = args[2];
	  return new HttpPostSink(serverAddress, paramWithData, userCookie);
	}
      }
    };
  }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin sink.
   */
  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("httpPost", builder()));
    return builders;
  }
}