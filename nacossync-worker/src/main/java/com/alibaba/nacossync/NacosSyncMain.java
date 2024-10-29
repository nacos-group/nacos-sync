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

package com.alibaba.nacossync;

import com.alibaba.nacossync.util.BatchTaskExecutor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EurekaClientAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerMain.java, v 0.1 2018-09-24 PM12:42 NacosSync Exp $$
 */
@SpringBootApplication(exclude = EurekaClientAutoConfiguration.class)
public class NacosSyncMain {
    
    public static void main(String[] args) {
        
        ConfigurableApplicationContext context = SpringApplication.run(NacosSyncMain.class, args);
        
        // Register shutdown callback using Spring Boot's context lifecycle
        context.registerShutdownHook();
        context.addApplicationListener(event -> {
            if (event instanceof org.springframework.context.event.ContextClosedEvent) {
                BatchTaskExecutor.shutdown();
            }
        });
    }
}
