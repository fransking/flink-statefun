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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SupplyingSourceSplitEnumerator<T, SplitT extends SourceSplit, EnumChckT> implements SplitEnumerator<SplitT, EnumChckT> {
    private final List<SplitT> splits;

    public SupplyingSourceSplitEnumerator() {
        this.splits = new ArrayList<>();
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<SplitT> splits, int subTaskId) {
        this.splits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        this.splits.add((SplitT) new SupplyingSourceSplit<T>(subtaskId));
    }

    @Override
    public EnumChckT snapshotState(long l) {
        return (EnumChckT) new HashSet<>(this.splits);
    }

    @Override
    public void close() {}
}
