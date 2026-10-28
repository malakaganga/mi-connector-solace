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

package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.wso2.integration.connector.constants.SolaceConstants;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The summaries of a browse over messages in memory. The browser itself needs a broker and is not covered here.
 */
public class SolaceBrowseQueueTest {

    @Test
    public void anUnreadablePayloadIsReportedOnItsItemAndTheBrowseContinues() {
        TextMessage first = text("one", "first");
        BytesMessage unreadable = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        unreadable.setData(new byte[]{(byte) 0xC3, 0x28});
        unreadable.setHTTPContentType("text/plain; charset=UTF-8");
        unreadable.setApplicationMessageId("second");
        unreadable.setCorrelationId("c2");
        TextMessage third = text("three", "third");

        JSONArray items = SolaceBrowseQueue.summarise(Arrays.<BytesXMLMessage>asList(first, unreadable, third), null);

        assertEquals(3, items.length());
        assertEquals("one", items.getJSONObject(0).getString("payload"));
        assertEquals("first", items.getJSONObject(0).getString("applicationMessageId"));
        assertEquals("three", items.getJSONObject(2).getString("payload"));
        assertEquals("third", items.getJSONObject(2).getString("applicationMessageId"));

        JSONObject middle = items.getJSONObject(1);
        assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR,
                middle.getInt(SolaceBrowseQueue.RESULT_PAYLOAD_ERROR_CODE));
        assertEquals("browse: the body is not valid UTF-8 text",
                middle.getString(SolaceBrowseQueue.RESULT_PAYLOAD_ERROR_MESSAGE));
        assertEquals("second", middle.getString("applicationMessageId"));
        assertEquals("c2", middle.getString("correlationId"));
        assertFalse(middle.has("payload"));
        assertFalse(middle.has(SolaceConstants.RESULT_CONTENT_TYPE));
    }

    @Test
    public void aMapOrStreamMessageIsItsParsedValue() throws Exception {
        SDTMap map = JCSMPFactory.onlyInstance().createMap();
        map.putString("s", "str");
        map.putLong("l", 7L);
        map.putBoolean("b", true);
        MapMessage mapMessage = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
        mapMessage.setMap(map);
        SDTStream stream = JCSMPFactory.onlyInstance().createStream();
        stream.writeString("one");
        stream.writeInteger(2);
        StreamMessage streamMessage = JCSMPFactory.onlyInstance().createMessage(StreamMessage.class);
        streamMessage.setStream(stream);

        JSONArray items = SolaceBrowseQueue.summarise(
                Arrays.<BytesXMLMessage>asList(mapMessage, streamMessage, text("{\"a\":1}", "json")), null);

        JSONObject parsedMap = items.getJSONObject(0).getJSONObject("payload");
        assertEquals("str", parsedMap.getString("s"));
        assertEquals(7L, parsedMap.getLong("l"));
        assertTrue(parsedMap.getBoolean("b"));
        assertEquals("application/json", items.getJSONObject(0).getString(SolaceConstants.RESULT_CONTENT_TYPE));
        JSONArray parsedStream = items.getJSONObject(1).getJSONArray("payload");
        assertEquals("one", parsedStream.getString(0));
        assertEquals(2, parsedStream.getInt(1));
        // A text body keeps its summary text, also when it looks like JSON.
        assertEquals("{\"a\":1}", items.getJSONObject(2).getString("payload"));
    }

    private static TextMessage text(String body, String applicationMessageId) {
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        message.setText(body);
        message.setApplicationMessageId(applicationMessageId);
        return message;
    }
}
