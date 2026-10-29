/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestFanoutWriter {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("id").build();

  private static class FailingFileWriter implements FileWriter<Integer, Void> {
    private int closeAttempts = 0;

    @Override
    public void write(Integer row) {}

    @Override
    public long length() {
      return 0;
    }

    @Override
    public Void result() {
      return null;
    }

    @Override
    public void close() throws IOException {
      closeAttempts += 1;
      throw new IOException("Failed to close");
    }
  }

  @Test
  void closeAttemptsEveryWriterWhenOneFails() {
    List<FailingFileWriter> fileWriters = Lists.newArrayList();
    FanoutWriter<Integer, Void> writer =
        new FanoutWriter<>() {
          @Override
          protected FileWriter<Integer, Void> newWriter(PartitionSpec spec, StructLike partition) {
            FailingFileWriter fileWriter = new FailingFileWriter();
            fileWriters.add(fileWriter);
            return fileWriter;
          }

          @Override
          protected void addResult(Void result) {}

          @Override
          protected Void aggregatedResult() {
            return null;
          }
        };

    PartitionKey partition = new PartitionKey(SPEC, SCHEMA);
    for (int id = 0; id < 3; id += 1) {
      partition.partition(Row.of(id));
      writer.write(id, SPEC, partition);
    }

    assertThatThrownBy(writer::close).isInstanceOf(IOException.class).hasMessage("Failed to close");
    assertThat(fileWriters).hasSize(3).allMatch(fileWriter -> fileWriter.closeAttempts == 1);
  }
}
