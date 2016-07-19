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

package org.apache.kylin.metadata.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RunningQuery extends QueryStep implements Comparable<RunningQuery> {
    public static final String SQL_PARSE_STEP = "parse_sql";
    public static final String CUBE_PLAN_STEP = "cube_plan";

    @JsonProperty("project")
    private String projectName;
    @JsonProperty("query_id")
    private String queryId;
    @JsonProperty("sql")
    private String sql;

    private String stopReason = "";
    @JsonProperty("incoming_record_cnt")
    private AtomicInteger numIncomingRecords = new AtomicInteger(0);

    private List<QueryStopListener> stopListeners = new ArrayList<>();

    public RunningQuery(String projectName, String queryId, String sql, Thread mainQueryThread){
        super("main_query");
        setExecThread(mainQueryThread);
        setStartTime(System.currentTimeMillis());
        this.projectName = projectName;
        this.queryId = queryId;
        this.sql = sql;
    }

    public void startSqlParse() {
        startStep(SQL_PARSE_STEP);
    }

    public void finishSqlParse() {
        finishStep(SQL_PARSE_STEP);
    }

    public void startCubePlan() {
        startStep(CUBE_PLAN_STEP);
    }

    public void finishCubePlan() {
        finishStep(CUBE_PLAN_STEP);
    }


    /**
     * stop the query
     */
    public void stop(String reason) {
        if (isStopped()){
            return;
        }
        setStatus(Status.STOPPED);
        setStopReason(reason);
        for (QueryStopListener stopListener : stopListeners) {
            stopListener.stop(this);
        }
    }

    public boolean isStopped() {
        return getStatus() == Status.STOPPED;
    }

    public String getStopReason() {
        return stopReason;
    }

    public void setStopReason(String stopReason) {
        this.stopReason = stopReason;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getSql() {
        return sql;
    }

    public int incAndGetIncomingRecords(int recordCnt) {
        return numIncomingRecords.addAndGet(recordCnt);
    }

    public void addQueryStopListener(QueryStopListener listener) {
        this.stopListeners.add(listener);
    }

    public String toString(){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        for (QueryStep step : getSubSteps()) {
            pw.print(step.getName() + ":");
        }

        return sw.toString();
    }

    @Override
    public int compareTo(RunningQuery q) {
        return (int)(this.getStartTime() - q.getStartTime());
    }

    public interface QueryStopListener {
        void stop(RunningQuery query);
    }
}
