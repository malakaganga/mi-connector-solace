/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.integration.connector;

/**
 * A connector fault with its name, its error code and whether the broker outcome is unknown (guide 0.3).
 * Every operation converts it into ERROR_CODE and ERROR_MESSAGE and raises it through handleException.
 */
public class SolaceFault extends Exception {

    public final String name;
    public final int code;
    public final boolean outcomeUnknown;

    public SolaceFault(String name, int code, String message) {
        this(name, code, message, null, false);
    }

    public SolaceFault(String name, int code, String message, Throwable cause) {
        this(name, code, message, cause, false);
    }

    public SolaceFault(String name, int code, String message, Throwable cause, boolean outcomeUnknown) {
        super(message, cause);
        this.name = name;
        this.code = code;
        this.outcomeUnknown = outcomeUnknown;
    }
}
