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
package org.apache.flink.statefun.testutils.testmodule;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class TestPipelineRouter implements Router<TypedValue> {
    @Override
    public void route(TypedValue request, Downstream<TypedValue> downstream) {
        downstream.forward(new Address(IoIdentifiers.ECHO_FUNCTION_TYPE, "id"), request);
    }
}
