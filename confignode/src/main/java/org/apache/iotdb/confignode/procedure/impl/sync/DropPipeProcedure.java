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
package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

// Empty procedure for old sync, restored only for compatibility
public class DropPipeProcedure extends Procedure<ConfigNodeProcedureEnv> {

  private String pipeName;

  public DropPipeProcedure() {
    super();
  }

  @Override
  protected Procedure<ConfigNodeProcedureEnv>[] execute(
      ConfigNodeProcedureEnv configNodeProcedureEnv)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    return new Procedure[0];
  }

  @Override
  protected void rollback(ConfigNodeProcedureEnv configNodeProcedureEnv)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean abort(ConfigNodeProcedureEnv configNodeProcedureEnv) {
    return false;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.DROP_PIPE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(pipeName, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DropPipeProcedure that = (DropPipeProcedure) o;
    return Objects.equals(pipeName, that.pipeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName);
  }
}
