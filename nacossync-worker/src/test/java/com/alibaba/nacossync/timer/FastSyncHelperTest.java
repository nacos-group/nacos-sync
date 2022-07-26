/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.impl.NacosSyncToNacosServiceImpl;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.timer.FastSyncHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * test data synchronization.
 * @ClassName: FastSyncHelperTest
 * @Author: ChenHao26
 * @Date: 2022/7/26 15:16
 * @Description:
 */
@RunWith(MockitoJUnitRunner.class)
public class FastSyncHelperTest {
    
    @Mock
    private SkyWalkerCacheServices skyWalkerCacheServices;
    
    @Mock
    private MetricsManager metricsManager;
    
    @Mock
    private SyncManagerService syncManagerService;
    
    @Mock
    private NacosSyncToNacosServiceImpl nacosSyncToNacosService;
    
    @InjectMocks
    @Spy
    private FastSyncHelper fastSyncHelper;
    
    @Test
    public void testSyncWithThread() {
        TaskDO taskDO = mock(TaskDO.class);
        List<TaskDO> list = new ArrayList<>();
        list.add(taskDO);
        try {
            fastSyncHelper.syncWithThread(list);
        }catch (Exception e) {
            Assert.assertEquals(e,InterruptedException.class);
            e.printStackTrace();
        }
    }
    
    @Test
    public void testAverageAssign(){
        int limit = 2;
        List<String> sourceList = new ArrayList<>();
        sourceList.add("1");
        sourceList.add("2");
        sourceList.add("3");
        List<List<String>> lists = FastSyncHelper.averageAssign(sourceList, limit);
        Assert.assertEquals(lists.get(0).size(),limit);
        Assert.assertNotEquals(lists.get(0).size(),3);
    }
}
