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

package org.apache.iotdb.commons.pipe.plugin.builtin.connector;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.connector.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

/**
 * This class is a placeholder and should not be initialized. It represents the IoTDB Thrift
 * connector. There is a real implementation in the server module but cannot be imported here. The
 * pipe agent in the server module will replace this class with the real implementation when
 * initializing the IoTDB Thrift connector.
 */
public class IoTDBThriftConnector implements PipeConnector {

  @Override
  public void validate(PipeParameterValidator validator) {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration) {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void handshake() {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void heartbeat() {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void transfer(Event event) {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("This class is a placeholder and should not be used.");
  }
}
