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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class TestEgress {
  private static final BlockingQueue<TypedValue> egressQueue = new LinkedBlockingQueue<>();
  private static Thread harnessThread;
  private static final int threadPollFrequencyMs = 100;

  public static void initialise(Thread harnessThread) {
    TestEgress.harnessThread = harnessThread;
  }

  public static void addMessage(TypedValue message) {
    try {
      egressQueue.put(message);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static TypedValue getMessage() {
    try {
      TypedValue result = null;
      while (result == null) {
        if (!harnessThread.isAlive()) {
          throw new RuntimeException("Harness has stopped" + harnessThread);
        }
        result = egressQueue.poll(threadPollFrequencyMs, TimeUnit.MILLISECONDS);
      }
      return result;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
