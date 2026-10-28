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

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.TextMessage;
import org.junit.Test;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The user property contract: the outgoing array form with its typed values, and the flat userProp_ attributes
 * of a received message.
 */
public class SolaceUtilsTest {

    @Test
    public void everyKeyOfEveryObjectIsATypedProperty() throws Exception {
        Map<String, Object> props = SolaceUtils.parseUserProperties(
                "[{\"a\":\"x\",\"b\":2},{\"c\":1.5},{\"d\":true},{\"e\":-3}]", "publishMessage");
        assertEquals(5, props.size());
        assertEquals("x", props.get("a"));
        assertEquals(2L, props.get("b"));
        assertEquals(1.5d, props.get("c"));
        assertEquals(Boolean.TRUE, props.get("d"));
        assertEquals(-3L, props.get("e"));
    }

    @Test
    public void invalidUserPropertiesFault() {
        for (String input : new String[] {"[{\"a\":null}]", "[{\"a\":{\"n\":1}}]", "[{\"a\":[1]}]", "{\"a\":\"x\"}",
                "[{a:1}]", "[\"a\"]", "not json", "[{\"big\":12345678901234567890}]",
                "[{\"huge\":1e400}]"}) {
            try {
                SolaceUtils.parseUserProperties(input, "publishMessage");
                fail("expected PAYLOAD_ERROR for " + input);
            } catch (SolaceFault fault) {
                assertEquals(input, SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
                assertEquals(input, SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
                assertTrue(fault.getMessage(), fault.getMessage().startsWith("publishMessage: "));
            }
        }
    }

    @Test
    public void receivedMetadataIsFlatAndTyped() throws Exception {
        JCSMPFactory factory = JCSMPFactory.onlyInstance();
        SDTMap nested = factory.createMap();
        nested.putString("k", "v");
        SDTStream stream = factory.createStream();
        stream.writeLong(5L);
        SDTMap props = factory.createMap();
        props.putString("region", "US");
        props.putInteger("count", 7);
        props.putBoolean("ok", true);
        props.putBytes("raw", new byte[] {1, 2, 3});
        props.putMap("nested", nested);
        props.putStream("list", stream);
        props.putString("a.b", "dotted");
        props.putString("userProp_x", "prefixed");
        props.putString("correlationId", "from a property");
        TextMessage message = factory.createMessage(TextMessage.class);
        message.setProperties(props);
        message.setReplyTo(factory.createQueue("replies"));
        message.setCorrelationId("c1");

        Map<String, Object> sink = new HashMap<>();
        sink.put("destination", "already");
        SolaceUtils.populateMessageMetadata(message, sink);

        assertEquals("already", sink.get("destination"));
        assertEquals("c1", sink.get("correlationId"));
        assertEquals("replies", sink.get("replyToName"));
        assertEquals("QUEUE", sink.get("replyToType"));
        assertFalse(sink.containsKey("replyTo"));
        assertFalse(sink.containsKey("userProperties"));
        assertEquals("US", sink.get("userProp_region"));
        assertEquals(7, sink.get("userProp_count"));
        assertEquals(Boolean.TRUE, sink.get("userProp_ok"));
        assertEquals("AQID", sink.get("userProp_raw"));
        assertEquals("{\"k\":\"v\"}", sink.get("userProp_nested"));
        assertEquals("[5]", sink.get("userProp_list"));
        assertEquals("dotted", sink.get("userProp_a.b"));
        assertEquals("prefixed", sink.get("userProp_userProp_x"));
        // A property named like fixed metadata keeps both, nothing is dropped.
        assertEquals("c1", sink.get("correlationId"));
        assertEquals("from a property", sink.get("userProp_correlationId"));
        for (Object value : sink.values()) {
            assertTrue(value.getClass().getName(),
                    value instanceof String || value instanceof Number || value instanceof Boolean);
        }
    }
}
