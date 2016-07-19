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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class QueryStep {
    public static final String LINE_SEP = System.getProperty("line.separator");

    public enum Status {
        RUNNING, STOPPED, SUCCEED
    }

    @JsonProperty("name")
    private String name;

    @JsonProperty("start_time")
    private long startTime;

    @JsonProperty("end_time")
    private long endTime;

    @JsonProperty("status")
    private volatile Status status = Status.RUNNING;

    @JsonProperty("attributes")
    private Map<String, String> attributes = new HashMap<>();

    @JsonProperty("sub_steps")
    private List<QueryStep> subSteps = new CopyOnWriteArrayList<>();

    private Thread execThread;

    public QueryStep(String name) {
        this.name = name;
    }

    public QueryStep startStep(String stepName) {
        return startStep(stepName, Thread.currentThread());
    }

    public QueryStep startStep(String stepName, Thread execThread) {
        QueryStep step = new QueryStep(stepName);
        step.setStartTime(System.currentTimeMillis());
        step.setExecThread(execThread);
        addSubStep(step);

        return step;
    }

    public void finishStep(String stepName) {
        finishStep(stepName, Thread.currentThread());
    }

    public void finishStep(String stepName, Thread thread) {
        QueryStep step = getStepByName(stepName, thread, Status.RUNNING);
        if (step == null) {
            throw new IllegalStateException("the step:" + stepName + " is not exist.");
        }
        finishStep(step);
    }

    public void finishStep(QueryStep step) {
        step.setEndTime(System.currentTimeMillis());
        step.status = Status.SUCCEED;
    }

    public boolean isRunning(){
        return this.status == Status.RUNNING;
    }

    public QueryStep addAttribute(String key, String value) {
        attributes.put(key, value);
        return this;
    }

    public QueryStep getRunningStep(Thread thread) {
        for (QueryStep step : getSubSteps()) {
            if (step.isRunning() && thread.equals(step.getExecThread())) {
                return step;
            }
        }
        return null;
    }

    private void addSubStep(QueryStep step) {
        subSteps.add(step);
    }

    @JsonProperty("thread")
    public ThreadInfo getThread(){
        String stackTraceStr = null;
        if (isRunning()) {
            int maxStackTraceDepth = 50;
            int current = 0;

            StackTraceElement[] stackTrace = execThread.getStackTrace();
            StringBuilder buf = new StringBuilder();
            buf.append(LINE_SEP);
            for (StackTraceElement e : stackTrace) {
                if (++current > maxStackTraceDepth) {
                    break;
                }
                buf.append("\t").append("at ").append(e.toString()).append(LINE_SEP);
            }
            stackTraceStr = buf.toString();
        }


        ThreadInfo threadInfo = new ThreadInfo();
        threadInfo.threadName = execThread.getName();
        threadInfo.stackTrace = stackTraceStr;
        return threadInfo;
    }

    /**
     *
     * @param name
     * @param thread
     * @return null if step not exist
     */
    private QueryStep getStepByName(String name, Thread thread, Status status) {
        for (QueryStep step : subSteps) {
            if (step.name.equals(name) && step.execThread.equals(thread) && step.getStatus().equals(status)) {
                return step;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public Thread getExecThread() {
        return execThread;
    }

    public void setExecThread(Thread execThread) {
        this.execThread = execThread;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<QueryStep> getSubSteps() {
        return subSteps;
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class ThreadInfo{
        @JsonProperty("name")
        public String threadName;
        @JsonProperty("stack_trace")
        public String stackTrace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryStep queryStep = (QueryStep) o;

        if (!name.equals(queryStep.name)) return false;
        return execThread.equals(queryStep.execThread);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + execThread.hashCode();
        return result;
    }
}
