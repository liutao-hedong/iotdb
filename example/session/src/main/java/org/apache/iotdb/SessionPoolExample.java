/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SessionPoolExample {

  private static SessionPool sessionPool;

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPoolExample.class);

  private static int THREAD_NUMBER = 300;

  private static int DEVICE_NUMBER = 20000;

  private static int SENSOR_NUMBER = 500;

  private static int WRITE_NUMBER = 100;

  private static List<String> measurements;

  private static List<TSDataType> types;

  private static AtomicInteger totalRowNumber = new AtomicInteger();

  private static AtomicInteger finishedThreadCount = new AtomicInteger();

  private static AtomicLong insertTime = new AtomicLong();

  private static Random r;

  /** Build a custom SessionPool for this example */

  /** Build a redirect-able SessionPool for this example */
  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    nodeUrls.add("10.24.58.58:6667");
    nodeUrls.add("10.24.58.67:6667");
    nodeUrls.add("10.24.58.69:6667");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .maxSize(500)
            .build();
    sessionPool.setFetchSize(10000);
  }

  public static void main(String[] args) throws InterruptedException {
    // Choose the SessionPool you going to use
    constructRedirectSessionPool();

    measurements = new ArrayList<>();
    types = new ArrayList<>();
    for (int i = 0; i < SENSOR_NUMBER; i++) {
      measurements.add("s_" + i);
      types.add(TSDataType.FLOAT);
    }

    r = new Random();

    Thread[] threads = new Thread[THREAD_NUMBER];
    for (int i = 0; i < THREAD_NUMBER; i++) {
      int index = i;
      threads[i] =
          new Thread(
              () -> {
                for (int j = 0; j < WRITE_NUMBER; j++) {
                  long start = System.currentTimeMillis();
                  try {
                    insertRecords(index);
                  } catch (Exception e) {
                    LOGGER.error("insert error:", e);
                  }
                  LOGGER.info(
                      "insert {} rows cost {} ms", (DEVICE_NUMBER / THREAD_NUMBER), System.currentTimeMillis() - start);
                  LOGGER.info("Total rows number: {}", totalRowNumber.addAndGet(DEVICE_NUMBER / THREAD_NUMBER));
                  finishedThreadCount.addAndGet(1);
                  while (finishedThreadCount.get() <= THREAD_NUMBER) {
                    try {
                      TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  finishedThreadCount.set(0);
                  insertTime.set(System.currentTimeMillis());
                }
              });
    }
    Thread.sleep(1000);
    long startTime = System.currentTimeMillis();
    // start write
    insertTime.set(System.currentTimeMillis());
    for (Thread thread : threads) {
      thread.start();
    }

    Thread[] readThreads = new Thread[THREAD_NUMBER];
    for (int i = 0; i < THREAD_NUMBER; i++) {
      readThreads[i] =
          new Thread(
              () -> {
                for (int j = 0; j < WRITE_NUMBER; j++) {
                  try {
                    queryByIterator();
                  } catch (Exception e) {
                    LOGGER.error("query error:", e);
                  }
                }
              });
    }

    // start read
    for (Thread thread : readThreads) {
      thread.start();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  sessionPool.close();
                  System.out.println(System.currentTimeMillis() - startTime);
                }));

    long startTime1 = System.nanoTime();
    new Thread(
            () -> {
              while (true) {
                try {
                  TimeUnit.MINUTES.sleep(1);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                long currentTime = System.nanoTime();
                LOGGER.info(
                    "write rate: {} lines/minute",
                    totalRowNumber.get() / ((currentTime - startTime1) / 60_000_000_000L));
              }
            })
        .start();
  }

  // more insert example, see SessionExample.java
  private static void insertRecords(int threadIndex) throws StatementExecutionException, IoTDBConnectionException {
    long time = insertTime.get();
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int j = 0; j < DEVICE_NUMBER / THREAD_NUMBER; j++) {
      String deviceId = "root.test.g_0.d_" + (DEVICE_NUMBER / THREAD_NUMBER * threadIndex + j);
      deviceIds.add(deviceId);
      times.add(time);
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < SENSOR_NUMBER; i++) {
        values.add(r.nextFloat());
      }
      valuesList.add(values);
      measurementsList.add(measurements);
      typesList.add(types);
    }

    sessionPool.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
  }

  private static void queryByIterator()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSetWrapper wrapper = null;
    int device = r.nextInt(DEVICE_NUMBER);
    try {
      long startTime = System.currentTimeMillis();
      String sql = "select last(*) from root.test.g_0.d_" + device;
      wrapper = sessionPool.executeQueryStatement(sql);
      // get DataIterator like JDBC
      DataIterator dataIterator = wrapper.iterator();
      //              System.out.println(wrapper.getColumnNames());
      //              System.out.println(wrapper.getColumnTypes());
      while (dataIterator.next()) {
        //                StringBuilder builder = new StringBuilder();
        for (String columnName : wrapper.getColumnNames()) {
          dataIterator.getString(columnName);
        }
        //                System.out.println(builder);
      }
      long cost = System.currentTimeMillis() - startTime;
      LOGGER.info("Query data of d_" + device + "cost:" + cost + "ms");
    } finally {
      // remember to close data set finally!
      sessionPool.closeResultSet(wrapper);
    }
  }
}
