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
package com.alibaba.nacossync.template;

import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.pojo.request.BaseRequest;

/**
 *
 * @author NacosSync
 * @version $Id: Processor.java, v 0.1 2018-05-12 下午3:03 NacosSync Exp $$
 */
public interface Processor<R extends BaseRequest, N extends BaseResult> {

    void process(R r, N n, Object... others) throws Exception;
}
