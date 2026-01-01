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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.harness.io;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

final class SupplyingSource<T, SplitT extends SourceSplit, EnumChckT> implements Source<T, SplitT, EnumChckT> {
  private static final long serialVersionUID = 1;

  private final SerializableSupplier<T> supplier;

  SupplyingSource(SerializableSupplier<T> supplier) {
    this.supplier = supplier;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<SplitT, EnumChckT> createEnumerator(SplitEnumeratorContext<SplitT> splitEnumeratorContext) throws Exception {
    return new SupplyingSourceSplitEnumerator<>();
  }

  @Override
  public SplitEnumerator<SplitT, EnumChckT> restoreEnumerator(SplitEnumeratorContext<SplitT> splitEnumeratorContext, EnumChckT enumChckT) throws Exception {
    return new SupplyingSourceSplitEnumerator<>();
  }

  @Override
  public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
    return new SupplyingSourceSplitSerializer<>();
  }

  @Override
  public SimpleVersionedSerializer<EnumChckT> getEnumeratorCheckpointSerializer() {
    return null;
  }

  @Override
  public SourceReader<T, SplitT> createReader(SourceReaderContext sourceReaderContext) {
    return new SupplyingSourceReader<>(this.supplier);
  }
}