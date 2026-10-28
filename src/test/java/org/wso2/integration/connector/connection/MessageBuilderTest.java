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

package org.wso2.integration.connector.connection;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLContentMessage;
import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axis2.Constants;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.OperationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.BinaryFormatter;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.junit.Test;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.PayloadCodec;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.activation.DataHandler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The builder resolves a validated value after trim, case insensitive, and no longer falls back on an unknown one.
 * The outgoing message keeps the bytes as they are for BYTES and the decoded text for TEXT and XML. A binary body
 * with a text message type faults, and the user properties keep their types.
 */
public class MessageBuilderTest {

    @Test
    public void deliveryModeResolvesAbsentToDirectAndTrimmedWords() {
        assertEquals(DeliveryMode.DIRECT, MessageBuilder.resolveDeliveryMode(null));
        assertEquals(DeliveryMode.PERSISTENT, MessageBuilder.resolveDeliveryMode(" persistent "));
        assertEquals(DeliveryMode.NON_PERSISTENT, MessageBuilder.resolveDeliveryMode("Non_Persistent"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void deliveryModeRefusesAnUnknownWord() {
        MessageBuilder.resolveDeliveryMode("PERSISTANT");
    }

    @Test
    public void destinationResolvesAbsentToTopicAndTrimmedWords() {
        assertTrue(MessageBuilder.resolveDestination(null, "a/b") instanceof Topic);
        assertTrue(MessageBuilder.resolveDestination(" queue ", "q1") instanceof Queue);
        assertTrue(MessageBuilder.resolveDestination("Topic", "a/b") instanceof Topic);
    }

    @Test(expected = IllegalArgumentException.class)
    public void destinationRefusesAnUnknownType() {
        MessageBuilder.resolveDestination("QUEUEE", "q1");
    }

    @Test
    public void textBodyBecomesTextOrBytesWithTheFormatterEncoding() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        OMElement textElement = OMAbstractFactory.getOMFactory().createOMElement(BaseConstants.DEFAULT_TEXT_WRAPPER);
        textElement.setText("café");
        axis2Mc.getEnvelope().getBody().addChild(textElement);
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "text/plain");
        axis2Mc.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "ISO-8859-1");
        PayloadCodec.Outgoing payload = PayloadCodec.readOutgoing(synCtx, "publishMessage");

        BytesXMLMessage text = MessageBuilder.createMessage("TEXT", payload, synCtx, "publishMessage");
        assertEquals("café", ((TextMessage) text).getText());
        BytesXMLMessage xml = MessageBuilder.createMessage("XML", payload, synCtx, "publishMessage");
        assertEquals("café", ((XMLContentMessage) xml).getXMLContent());
        BytesXMLMessage bytes = MessageBuilder.createMessage("BYTES", payload, synCtx, "publishMessage");
        assertArrayEquals("café".getBytes(StandardCharsets.ISO_8859_1), ((BytesMessage) bytes).getData());
        assertEquals("text/plain; charset=ISO-8859-1", payload.contentType());
    }

    @Test
    public void binaryBodyWithTextTypeFaultsBeforeAnyBrokerCall() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        OMFactory factory = OMAbstractFactory.getOMFactory();
        byte[] data = {0, (byte) 0xC3, 0x28, (byte) 0xFF};
        OMElement binaryElement = factory.createOMElement(BaseConstants.DEFAULT_BINARY_WRAPPER);
        binaryElement.addChild(factory.createOMText(new DataHandler(new ByteArrayDataSource(data,
                "application/octet-stream")), true));
        axis2Mc.getEnvelope().getBody().addChild(binaryElement);
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/octet-stream");
        PayloadCodec.Outgoing payload = PayloadCodec.readOutgoing(synCtx, "publishMessage");
        assertTrue(payload.binary);

        BytesXMLMessage bytes = MessageBuilder.createMessage("BYTES", payload, synCtx, "sendReply");
        assertArrayEquals(data, ((BytesMessage) bytes).getData());
        for (String type : new String[] {"TEXT", "XML"}) {
            try {
                MessageBuilder.createMessage(type, payload, synCtx, "sendReply");
                fail("expected PAYLOAD_ERROR for " + type);
            } catch (SolaceFault fault) {
                assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
                assertEquals(SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
                assertTrue(fault.getMessage(), fault.getMessage().startsWith("sendReply: "));
            }
        }
    }

    @Test
    public void userPropertiesAreAppliedWithTheirTypes() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("s", "text");
        props.put("l", 2L);
        props.put("d", 1.5d);
        props.put("b", Boolean.TRUE);
        SolaceMessageProperties properties = new SolaceMessageProperties();
        properties.setUserProperties(props);
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        MessageBuilder.applyMessageProperties(message, properties);
        assertEquals("text", message.getProperties().getString("s"));
        assertEquals(Long.valueOf(2L), message.getProperties().getLong("l"));
        assertEquals(Double.valueOf(1.5d), message.getProperties().getDouble("d"));
        assertEquals(Boolean.TRUE, message.getProperties().getBoolean("b"));
    }

    @Test
    public void aUserPropertyTheMessageCannotTakeFaultsPublishError() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(null, "text");
        SolaceMessageProperties properties = new SolaceMessageProperties();
        properties.setUserProperties(props);
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        try {
            MessageBuilder.applyMessageProperties(message, properties);
            fail("expected PUBLISH_ERROR");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PUBLISH_ERROR, fault.code);
            assertEquals(SolaceConstants.FAULT_PUBLISH_ERROR, fault.name);
        }
    }

    private static Axis2MessageContext context() throws Exception {
        AxisConfiguration config = new AxisConfiguration();
        config.addMessageFormatter("text/plain", new PlainTextFormatter());
        config.addMessageFormatter("application/octet-stream", new BinaryFormatter());
        MessageContext axis2Mc = new MessageContext();
        axis2Mc.setConfigurationContext(new ConfigurationContext(config));
        axis2Mc.setOperationContext(new OperationContext());
        axis2Mc.setEnvelope(OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope());
        axis2Mc.setDoingREST(true);
        return new Axis2MessageContext(axis2Mc, new SynapseConfiguration(), null);
    }
}
