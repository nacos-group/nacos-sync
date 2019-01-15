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
package com.alibaba.nacossync.exception;

import com.alibaba.nacossync.constant.ResultCodeEnum;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerException.java, v 0.1 2018-09-30 AM10:09 NacosSync Exp $$
 */
public class SkyWalkerException extends RuntimeException {
    /**
     * Constructs a new exception with the specified detail message and cause.  <p>Note that the detail message associated with {@code
     * cause} is <i>not</i> automatically incorporated in this exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()} method).  (A <tt>null</tt> value is
     *                permitted, and indicates that the cause is nonexistent or unknown.)
     * @since 1.4
     */
    private ResultCodeEnum resultCode;

    public ResultCodeEnum getResultCode() {
        return resultCode;
    }

    public void setResultCode(ResultCodeEnum resultCode) {
        this.resultCode = resultCode;
    }

    /**
     * Constructs a new exception with the specified detail message.  The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the {@link #getMessage()} method.
     */
    /**
     * Constructs a new runtime exception with {@code null} as its detail message.  The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     */
    public SkyWalkerException(ResultCodeEnum resultCode) {
        super();
        this.resultCode = resultCode;
    }

    /**
     * Constructs a new runtime exception with the specified detail message. The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the {@link #getMessage()} method.
     */
    public SkyWalkerException(String message, ResultCodeEnum resultCode) {
        super(message);
        this.resultCode = resultCode;
    }

    /**
     * Constructs a new runtime exception with the specified detail message. The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the {@link #getMessage()} method.
     */
    public SkyWalkerException(String message) {
        super(message);
        this.resultCode = ResultCodeEnum.SYSTEM_ERROR;
    }

    /**
     * Constructs a new runtime exception with the specified detail message and cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in this runtime exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()} method).  (A <tt>null</tt> value is
     *                permitted, and indicates that the cause is nonexistent or unknown.)
     * @since 1.4
     */
    public SkyWalkerException(String message, Throwable cause, ResultCodeEnum resultCode) {
        super(message, cause);
        this.resultCode = resultCode;
    }
}
