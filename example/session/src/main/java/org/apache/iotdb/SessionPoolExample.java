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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SessionPoolExample {

  private static SessionPool sessionPool;

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPoolExample.class);

  private static int THREAD_NUMBER = 300;

  private static int DEVICE_NUMBER = 20000;

  private static int SENSOR_NUMBER = 500;

  private static int TOTAL_BATCH_COUNT_PER_DEVICE = 100;

  private static List<String> measurements;

  private static List<TSDataType> types;

  private static AtomicInteger totalRowNumber = new AtomicInteger();

  private static AtomicLong insertTime = new AtomicLong();

  private static Random r;

  /** Build a custom SessionPool for this example */

  /** Build a redirect-able SessionPool for this example */
  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    //    nodeUrls.add("127.0.0.1:6667");
    nodeUrls.add("192.168.130.16:6667");
    nodeUrls.add("192.168.130.17:6667");
    nodeUrls.add("192.168.130.18:6667");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .maxSize(500)
            .build();
    sessionPool.setFetchSize(10000);
  }

  private static class SyncWriteSignal {
    protected volatile boolean needResetLatch = true;
    protected CountDownLatch latch;
    protected long currentTimestamp;

    protected void syncCountDownBeforeInsert() {
      if (needResetLatch) {
        synchronized (this) {
          if (needResetLatch) {
            latch = new CountDownLatch(THREAD_NUMBER);
            needResetLatch = false;
            currentTimestamp = System.currentTimeMillis();
          }
        }
      }
    }

    protected void finishInsert() {
      if (latch.getCount() == 1) {
        synchronized (this) {
          needResetLatch = true;
          LOGGER.info(
              "one loop finished. cost: {}ms. total rows: {}",
              (System.currentTimeMillis() - currentTimestamp),
              totalRowNumber.get());
        }
      }
      latch.countDown();
    }

    protected void waitCurrentLoopFinished() throws InterruptedException {
      latch.await();
    }
  }

  private static class InsertWorker implements Runnable {
    private SyncWriteSignal signal;
    private int index;

    protected InsertWorker(SyncWriteSignal signal, int index) {
      this.signal = signal;
      this.index = index;
    }

    @Override
    public void run() {
      for (int j = 0; j < TOTAL_BATCH_COUNT_PER_DEVICE; j++) {
        signal.syncCountDownBeforeInsert();
        try {
          int insertDeviceCount = insertRecords(index);
          totalRowNumber.addAndGet(insertDeviceCount);
          signal.finishInsert();
          signal.waitCurrentLoopFinished();
        } catch (Exception e) {
          LOGGER.error("insert error. Thread: {}. Error:", index, e);
        }
      }
    }
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

    Thread[] threads = new Thread[THREAD_NUMBER];

    SyncWriteSignal signal = new SyncWriteSignal();
    for (int i = 0; i < THREAD_NUMBER; i++) {
      threads[i] = new Thread(new InsertWorker(signal, i));
    }

    // initialize read thread
    Thread[] readThreads = new Thread[THREAD_NUMBER];
    for (int i = 0; i < THREAD_NUMBER; i++) {
      readThreads[i] =
          new Thread(
              () -> {
                for (int j = 0; j < TOTAL_BATCH_COUNT_PER_DEVICE; j++) {
                  try {
                    queryByIterator();
                  } catch (Exception e) {
                    LOGGER.error("query error:", e);
                  }
                }
              });
    }

    // count total execution time
    r = new Random();
    long startTime = System.currentTimeMillis();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  sessionPool.close();
                  System.out.println(System.currentTimeMillis() - startTime);
                }));

    // start write thread
    for (Thread thread : threads) {
      thread.start();
    }

    // start read
    //    for (Thread thread : readThreads) {
    //      thread.start();
    //    }

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
  private static int insertRecords(int threadIndex)
      throws StatementExecutionException, IoTDBConnectionException {
    long time = insertTime.get();
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    int deviceCount = 0;
    for (int j = threadIndex; j < DEVICE_NUMBER; j += THREAD_NUMBER) {
      String deviceId = "root.test.g_0.d_" + j;
      deviceIds.add(deviceId);
      times.add(time);
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < SENSOR_NUMBER; i++) {
        values.add(r.nextFloat());
      }
      valuesList.add(values);
      measurementsList.add(measurements);
      typesList.add(types);
      deviceCount++;
    }

    sessionPool.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
    return deviceCount;
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
