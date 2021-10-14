/**
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
package org.apache.pinot.core.operator;

import java.util.Map;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.request.context.ThreadTimer;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {
  private static final String OPERATOR_NAME = "InstanceResponseOperator";

  private final Operator _operator;

  public InstanceResponseOperator(Operator combinedOperator) {
    _operator = combinedOperator;
  }

  /*
   * Derive systemActivitiesCpuTimeNs from totalWallClockTimeNs, multipleThreadCpuTimeNs, singleThreadCpuTimeNs,
   * and numServerThreads.
   *
   * For example, let's divide query processing into 4 phases:
   * - phase 1: single thread preparing. Time used: T1
   * - phase 2: N threads processing segments in parallel, each thread use time T2
   * - phase 3: system activities (GC/OS paging). Time used: T3
   * - phase 4: single thread merging intermediate results blocks. Time used: T4
   *
   * Then we have following equations:
   * - singleThreadCpuTimeNs = T1 + T4
   * - multipleThreadCpuTimeNs = T2 * N
   * - totalWallClockTimeNs = T1 + T2 + T3 + T4 = singleThreadCpuTimeNs + T2 + T3
   * - totalThreadCpuTimeNsWithoutSystemActivities = T1 + T2 * N + T4 = singleThreadCpuTimeNs + T2 * N
   * - systemActivitiesCpuTimeNs = T3 = totalWallClockTimeNs - singleThreadCpuTimeNs - T2
   */
  public static long calSystemActivitiesCpuTimeNs(long totalWallClockTimeNs, long multipleThreadCpuTimeNs,
      long singleThreadCpuTimeNs, int numServerThreads) {
    double perMultipleThreadCpuTimeNs = multipleThreadCpuTimeNs * 1.0 / numServerThreads;
    double systemActivitiesCpuTimeNs = (totalWallClockTimeNs - singleThreadCpuTimeNs - perMultipleThreadCpuTimeNs);
    return Math.round(systemActivitiesCpuTimeNs);
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      ThreadTimer mainThreadTimer = new ThreadTimer();
      mainThreadTimer.start();

      long startWallClockTimeNs = System.nanoTime();
      IntermediateResultsBlock intermediateResultsBlock = getCombinedResults();
      InstanceResponseBlock instanceResponseBlock = new InstanceResponseBlock(intermediateResultsBlock);
      long totalWallClockTimeNs = System.nanoTime() - startWallClockTimeNs;

      long mainThreadCpuTimeNs = mainThreadTimer.stopAndGetThreadTimeNs();
      /*
       * If/when the threadCpuTime based instrumentation is done for other parts of execution (planning, pruning etc),
       * we will have to change the wallClockTime computation accordingly. Right now everything under
       * InstanceResponseOperator is the one that is instrumented with threadCpuTime.
       */
      long multipleThreadCpuTimeNs = intermediateResultsBlock.getExecutionThreadCpuTimeNs();
      int numServerThreads = intermediateResultsBlock.getNumServerThreads();
      long singleThreadCpuTimeNs = mainThreadCpuTimeNs - multipleThreadCpuTimeNs / numServerThreads;

      long threadCpuTimeNs = singleThreadCpuTimeNs + multipleThreadCpuTimeNs;
      long systemActivitiesCpuTimeNs =
          calSystemActivitiesCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, singleThreadCpuTimeNs,
              numServerThreads);

      Map<String, String> responseMetaData = instanceResponseBlock.getInstanceResponseDataTable().getMetadata();
      responseMetaData.put(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(threadCpuTimeNs));
      responseMetaData
          .put(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName(), String.valueOf(systemActivitiesCpuTimeNs));

      return instanceResponseBlock;
    } else {
      return new InstanceResponseBlock(getCombinedResults());
    }
  }

  public IntermediateResultsBlock getCombinedResults() {
    return (IntermediateResultsBlock) _operator.nextBlock();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
