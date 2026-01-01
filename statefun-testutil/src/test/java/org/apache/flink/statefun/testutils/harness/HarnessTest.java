/*
 * Copyright [2025] [Frans King]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.testutils.harness;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;


public class HarnessTest {
    @Test
    public void basicHarnessTest() throws InvalidProtocolBufferException {
        NamespacedTestHarness harness = NamespacedTestHarness.newInstance();
        harness.addIngressMessage("a test message");

        TypedValue value = harness.getEgressMessage();
        harness.shutdown();

        String result = StringValue.parseFrom(value.getValue()).getValue();
        assertThat(result, is("a test message"));
    }
}