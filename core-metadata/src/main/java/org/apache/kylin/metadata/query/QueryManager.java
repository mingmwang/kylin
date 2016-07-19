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

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class QueryManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryManager.class);
    private static QueryManager instance = new QueryManager();

    private final ConcurrentMap<Thread, RunningQuery> threadQueryMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, RunningQuery> idQueryMap = Maps.newConcurrentMap();

    public static QueryManager getInstance() {
        return instance;
    }

    public String startQuery(String project, String sql) {
        String queryId = UUID.randomUUID().toString();
        Thread currThread = Thread.currentThread();
        RunningQuery query = new RunningQuery(project, queryId, sql, currThread);
        threadQueryMap.put(currThread, query);
        idQueryMap.put(queryId, query);
        return queryId;
    }

    public void endQuery(String queryId) {
        if (queryId == null) {
            logger.warn("query id is null");
            return;
        }
        cleanQuery(queryId);
    }

    public void stopQuery(String queryId, String info) {
        RunningQuery query = idQueryMap.get(queryId);
        if (query != null) {
            query.stop(info);
            cleanQuery(queryId);
        } else {
            logger.info("the query:{} is not existed", queryId);
        }
    }

    private void cleanQuery(String queryId) {
        RunningQuery query = idQueryMap.remove(queryId);
        if (query != null) {
            threadQueryMap.remove(query.getExecThread());
        }
    }

    public RunningQuery getCurrentRunningQuery() {
        Thread thread = Thread.currentThread();
        return getRunningQuery(thread);
    }

    public RunningQuery getRunningQuery(Thread thread) {
        return threadQueryMap.get(thread);
    }

    public List<RunningQuery> getAllRunningQueries() {
        TreeSet<RunningQuery> queriesSet = new TreeSet<>();
        for (RunningQuery runningQuery : idQueryMap.values()) {
            queriesSet.add(runningQuery);
        }
        return new ArrayList<>(queriesSet);
    }
}
