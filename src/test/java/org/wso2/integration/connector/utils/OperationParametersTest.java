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

package org.wso2.integration.connector.utils;

import org.junit.Test;
import org.wso2.integration.connector.SolaceFault;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_INVALID_CONFIG;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_INVALID_CONFIG;

/**
 * The strict parse: absent keeps the default, trim and case are forgiven, anything else faults INVALID_CONFIG
 * naming the parameter and the value.
 */
public class OperationParametersTest {

    @Test
    public void choiceAcceptsTrimmedCaseInsensitiveWords() throws SolaceFault {
        assertEquals("INBOUND", OperationParameters.parseChoice("target", null, "INBOUND", "INBOUND", "POLL"));
        assertEquals("INBOUND", OperationParameters.parseChoice("target", "  ", "INBOUND", "INBOUND", "POLL"));
        assertEquals("POLL", OperationParameters.parseChoice("target", " poll ", "INBOUND", "INBOUND", "POLL"));
        assertEquals("PERSISTENT", OperationParameters.parseChoice("deliveryMode", " Persistent ", "DIRECT",
                "PERSISTENT", "NON_PERSISTENT", "DIRECT"));
    }

    @Test
    public void choiceFaultsOnAnyOtherWord() {
        SolaceFault fault = expectInvalid(() -> OperationParameters.parseChoice("target", "POLLL", "INBOUND",
                "INBOUND", "POLL"));
        assertEquals("target must be INBOUND or POLL, got 'POLLL'", fault.getMessage());
        fault = expectInvalid(() -> OperationParameters.parseChoice("deliveryMode", "PERSISTANT", "DIRECT",
                "PERSISTENT", "NON_PERSISTENT", "DIRECT"));
        assertEquals("deliveryMode must be PERSISTENT, NON_PERSISTENT or DIRECT, got 'PERSISTANT'",
                fault.getMessage());
    }

    @Test
    public void flagAcceptsTrueAndFalseOnly() throws SolaceFault {
        assertTrue(OperationParameters.parseFlag("waitForAck", null, true));
        assertFalse(OperationParameters.parseFlag("waitForAck", "", false));
        assertTrue(OperationParameters.parseFlag("waitForAck", " TRUE ", false));
        assertFalse(OperationParameters.parseFlag("waitForAck", "False", true));
        SolaceFault fault = expectInvalid(() -> OperationParameters.parseFlag("waitForAck", "yes", true));
        assertEquals("waitForAck must be true or false, got 'yes'", fault.getMessage());
    }

    @Test
    public void intAcceptsTheRangeAndFaultsOutsideIt() throws SolaceFault {
        assertEquals(5000, OperationParameters.parseInt("pollTimeout", null, 5000, 1, Integer.MAX_VALUE));
        assertEquals(1, OperationParameters.parseInt("pollTimeout", " 1 ", 5000, 1, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, OperationParameters.parseInt("pollTimeout", "2147483647", 5000, 1,
                Integer.MAX_VALUE));
        SolaceFault fault = expectInvalid(() -> OperationParameters.parseInt("pollTimeout", "0", 5000, 1,
                Integer.MAX_VALUE));
        assertEquals("pollTimeout must be a whole number from 1 to 2147483647, got '0'", fault.getMessage());
        fault = expectInvalid(() -> OperationParameters.parseInt("pollTimeout", "2147483648", 5000, 1,
                Integer.MAX_VALUE));
        assertEquals("pollTimeout must be a whole number from 1 to 2147483647, got '2147483648'",
                fault.getMessage());
        fault = expectInvalid(() -> OperationParameters.parseInt("pollTimeout", "abc", 5000, 1, Integer.MAX_VALUE));
        assertEquals("pollTimeout must be a whole number from 1 to 2147483647, got 'abc'", fault.getMessage());
        fault = expectInvalid(() -> OperationParameters.parseInt("priority", "256", 0, 0, 255));
        assertEquals("priority must be a whole number from 0 to 255, got '256'", fault.getMessage());
    }

    @Test
    public void longAcceptsZeroOrMoreWhenUnbounded() throws SolaceFault {
        assertEquals(0L, OperationParameters.parseLong("timeToLive", "0", 0L, 0L, Long.MAX_VALUE));
        assertEquals(10L, OperationParameters.parseLong("timeToLive", " 10 ", 0L, 0L, Long.MAX_VALUE));
        SolaceFault fault = expectInvalid(() -> OperationParameters.parseLong("timeToLive", "-1", 0L, 0L,
                Long.MAX_VALUE));
        assertEquals("timeToLive must be a whole number of 0 or more, got '-1'", fault.getMessage());
        fault = expectInvalid(() -> OperationParameters.parseLong("ackTimeout", "1.5", 10000L, 1L,
                Integer.MAX_VALUE));
        assertEquals("ackTimeout must be a whole number from 1 to 2147483647, got '1.5'", fault.getMessage());
    }

    @Test
    public void contentTypeIsAMediaTypeOrAbsent() throws SolaceFault {
        assertNull(OperationParameters.parseContentType("contentType", null));
        assertNull(OperationParameters.parseContentType("contentType", "   "));
        assertEquals("application/json",
                OperationParameters.parseContentType("contentType", " application/json "));
        assertEquals("text/plain; charset=ISO-8859-1",
                OperationParameters.parseContentType("contentType", "text/plain; charset=ISO-8859-1"));
        assertEquals("Text/Plain;Charset=\"utf-8\"",
                OperationParameters.parseContentType("contentType", "Text/Plain;Charset=\"utf-8\""));
        assertEquals("application/vnd.api+json; ext=\"a;b\"",
                OperationParameters.parseContentType("contentType", "application/vnd.api+json; ext=\"a;b\""));
    }

    @Test
    public void contentTypeFaultsOnSyntaxAndOnAnExplicitUnknownCharset() {
        for (String bad : new String[] {"json", "application/", "/json", "application json", "application/json;x",
                "application/json; x=", "text/plain; charset=\"open", "text/plain, application/json"}) {
            SolaceFault fault = expectInvalid(() -> OperationParameters.parseContentType("contentType", bad));
            assertEquals("contentType must be a media type such as application/json, got '" + bad + "'",
                    fault.getMessage());
        }
        SolaceFault fault = expectInvalid(() -> OperationParameters.parseContentType("contentType",
                "text/plain; charset=no-such-charset"));
        assertEquals("contentType names the unknown charset 'no-such-charset', got "
                + "'text/plain; charset=no-such-charset'", fault.getMessage());
        expectInvalid(() -> OperationParameters.parseContentType("contentType", "text/plain; charset=\"\""));
        expectInvalid(() -> OperationParameters.parseContentType("contentType", "text/plain; charset=bad*name"));
    }

    private interface Parse {
        Object run() throws SolaceFault;
    }

    private static SolaceFault expectInvalid(Parse parse) {
        try {
            parse.run();
        } catch (SolaceFault fault) {
            assertEquals(FAULT_INVALID_CONFIG, fault.name);
            assertEquals(ERROR_CODE_INVALID_CONFIG, fault.code);
            assertFalse(fault.outcomeUnknown);
            return fault;
        }
        fail("expected INVALID_CONFIG");
        return null;
    }
}
