/*
 * Copyright [2025] [Frans King]
 * Copyright [2023] [Frans King, Luke Ashworth]
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
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import com.google.protobuf.StringValue;

public class TestHarness {

    public static TestHarness newInstance() {
        HarnessUtils.ensureHarnessThreadIsRunning();
        return new TestHarness();
    }

    public void addIngressMessage(String message) {
        StringValue stringValue = StringValue.newBuilder().setValue(message).build();

        TypedValue typedValue = TypedValue.newBuilder()
                .setHasValue(true)
                .setValue(stringValue.toByteString())
                .build();

        TestIngress.addMessage(typedValue);
    }

    public TypedValue getEgressMessage() {
        return TestEgress.getMessage();
    }

    public void shutdown() {
        TestIngress.stop();
    }
}
