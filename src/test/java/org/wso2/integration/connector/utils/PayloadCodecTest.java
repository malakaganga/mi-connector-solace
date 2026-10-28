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

import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axis2.Constants;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.OperationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.BinaryFormatter;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.axis2.transport.http.SOAPMessageFormatter;
import org.apache.synapse.commons.json.JsonStreamFormatter;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.junit.Test;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.activation.DataHandler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The payload contract: the read side codec, the strict JSON parse, the body replacement and the formatter
 * based send side.
 */
public class PayloadCodecTest {

    private static final String JSON_STRING_PROPERTY = "JSON_STRING";
    private static final String MESSAGE_AS_BYTES_PROPERTY = "MESSAGE_AS_BYTES";

    // Read side

    @Test
    public void undeclaredTextMessageIsTextWhateverItsBody() throws Exception {
        PayloadCodec.Received received = PayloadCodec.decode(text("{\"a\":1}", null), "poll");
        assertEquals(PayloadCodec.Family.TEXT, received.family);
        assertEquals("text/plain", received.contentType);
        assertNull(received.json);
        assertEquals(new JsonPrimitive("{\"a\":1}"), received.responseValue());
    }

    @Test
    public void undeclaredTextMessageWithUnquotedWordOrScalarIsText() throws Exception {
        PayloadCodec.Received word = PayloadCodec.decode(text("[INFO]", null), "poll");
        assertEquals(PayloadCodec.Family.TEXT, word.family);
        assertEquals("text/plain", word.contentType);
        assertEquals(new JsonPrimitive("[INFO]"), word.responseValue());
        PayloadCodec.Received scalar = PayloadCodec.decode(text("42", null), "poll");
        assertEquals(PayloadCodec.Family.TEXT, scalar.family);
        assertEquals(new JsonPrimitive("42"), scalar.responseValue());
    }

    @Test
    public void declaredJsonParsesEveryValueAndFaultsWhenItDoesNotParse() throws Exception {
        assertEquals(new JsonPrimitive(42), PayloadCodec.decode(text("42", "application/json"), "poll").json);
        assertEquals(JsonNull.INSTANCE, PayloadCodec.decode(text("null", "application/json"), "poll").json);
        PayloadCodec.Received mixedCase = PayloadCodec.decode(text("{\"a\":[1]}", "APPLICATION/Vnd.api+JSON"),
                "poll");
        assertEquals(PayloadCodec.Family.JSON, mixedCase.family);
        assertEquals("APPLICATION/Vnd.api+JSON", mixedCase.contentType);
        assertEquals("application/vnd.api+json", mixedCase.mediaType);
        assertPayloadFault(text("{bad", "application/json"));
        assertPayloadFault(text("", "application/json"));
    }

    @Test
    public void declaredTextPlainKeepsStringSemanticsForJsonText() throws Exception {
        PayloadCodec.Received received = PayloadCodec.decode(text("{\"a\":1}", "text/plain"), "poll");
        assertEquals(PayloadCodec.Family.TEXT, received.family);
        assertEquals(new JsonPrimitive("{\"a\":1}"), received.responseValue());
    }

    @Test
    public void xmlContentMessageIsXmlText() throws Exception {
        XMLContentMessage message = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
        message.setXMLContent("<a/>");
        PayloadCodec.Received received = PayloadCodec.decode(message, "poll");
        assertEquals(PayloadCodec.Family.XML, received.family);
        assertEquals("application/xml", received.contentType);
        assertEquals(new JsonPrimitive("<a/>"), received.responseValue());
        message.setHTTPContentType("text/xml; charset=UTF-8");
        assertEquals("text/xml; charset=UTF-8", PayloadCodec.decode(message, "poll").contentType);
    }

    @Test
    public void bytesMessageDeclaredTextIsDecodedWithTheDeclaredCharset() throws Exception {
        PayloadCodec.Received latin = PayloadCodec.decode(
                bytes(new byte[] {(byte) 0xE9}, "text/plain; charset=ISO-8859-1"), "poll");
        assertEquals(PayloadCodec.Family.TEXT, latin.family);
        assertEquals("é", latin.text);
        assertEquals("ISO-8859-1", latin.charset);
        assertEquals("text/plain; charset=ISO-8859-1", latin.contentType);
        assertNull(latin.bytes);

        PayloadCodec.Received json = PayloadCodec.decode(
                bytes("{\"a\":1}".getBytes(StandardCharsets.UTF_8), "application/json; charset=\"utf-8\""), "poll");
        assertEquals(PayloadCodec.Family.JSON, json.family);
        assertEquals("utf-8", json.charset);

        PayloadCodec.Received replacement = PayloadCodec.decode(
                bytes("a�b".getBytes(StandardCharsets.UTF_8), "text/plain"), "poll");
        assertEquals("a�b", replacement.text);
    }

    @Test
    public void bytesMessageWithoutTextTypeIsBinaryBase64() throws Exception {
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        PayloadCodec.Received undeclared = PayloadCodec.decode(bytes(data, null), "poll");
        assertTrue(undeclared.isBinary());
        assertEquals("application/octet-stream", undeclared.contentType);
        assertArrayEquals(data, undeclared.bytes);
        assertEquals(new JsonPrimitive(Base64.getEncoder().encodeToString(data)), undeclared.responseValue());
        assertEquals(Base64.getEncoder().encodeToString(data), undeclared.summaryText());

        PayloadCodec.Received pdf = PayloadCodec.decode(bytes(data, "application/pdf"), "poll");
        assertTrue(pdf.isBinary());
        assertEquals("application/pdf", pdf.contentType);
    }

    @Test
    public void malformedBytesAndUnknownCharsetFault() {
        assertPayloadFault(bytes(new byte[] {(byte) 0xC3, 0x28}, "text/plain; charset=UTF-8"));
        assertPayloadFault(bytes("x".getBytes(StandardCharsets.UTF_8), "text/plain; charset=no-such-charset"));
    }

    @Test
    public void mapAndStreamMessagesBecomeJson() throws Exception {
        JCSMPFactory factory = JCSMPFactory.onlyInstance();
        SDTMap inner = factory.createMap();
        inner.putLong("n", 7L);
        SDTMap map = factory.createMap();
        map.putString("s", "text");
        map.putBoolean("b", true);
        map.putBytes("raw", new byte[] {1, 2, 3});
        map.putMap("inner", inner);
        MapMessage mapMessage = factory.createMessage(MapMessage.class);
        mapMessage.setMap(map);
        PayloadCodec.Received received = PayloadCodec.decode(mapMessage, "poll");
        assertEquals(PayloadCodec.Family.JSON, received.family);
        assertEquals("application/json", received.contentType);
        assertEquals("text", received.json.getAsJsonObject().get("s").getAsString());
        assertTrue(received.json.getAsJsonObject().get("b").getAsBoolean());
        assertEquals("AQID", received.json.getAsJsonObject().get("raw").getAsString());
        assertEquals(7, received.json.getAsJsonObject().getAsJsonObject("inner").get("n").getAsInt());

        SDTStream stream = factory.createStream();
        stream.writeString("a");
        stream.writeInteger(2);
        StreamMessage streamMessage = factory.createMessage(StreamMessage.class);
        streamMessage.setStream(stream);
        PayloadCodec.Received list = PayloadCodec.decode(streamMessage, "poll");
        assertTrue(list.json.isJsonArray());
        assertEquals("[\"a\",2]", list.text);
    }

    @Test
    public void strictJsonParse() {
        assertNull(PayloadCodec.parseJson("[INFO]"));
        assertNull(PayloadCodec.parseJson("{\"a\":1} x"));
        assertNull(PayloadCodec.parseJson("{'a':1}"));
        assertNull(PayloadCodec.parseJson(""));
        assertNull(PayloadCodec.parseJson("   "));
        assertNull(PayloadCodec.parseJson(null));
        assertTrue(PayloadCodec.parseJson(" {\"a\":[1,{\"b\":null}]} ").isJsonObject());
        assertEquals(new JsonPrimitive("s"), PayloadCodec.parseJson("\"s\""));
        assertEquals(JsonNull.INSTANCE, PayloadCodec.parseJson("null"));
    }

    @Test
    public void contentTypeHelpers() {
        assertEquals("text/plain", PayloadCodec.mediaTypeOf(" Text/Plain ; charset=ISO-8859-1"));
        assertEquals("ISO-8859-1", PayloadCodec.charsetOf("text/plain; Charset=\"ISO-8859-1\""));
        assertNull(PayloadCodec.charsetOf("text/plain"));
        assertNull(PayloadCodec.mediaTypeOf(" "));
        assertTrue(PayloadCodec.isTextType("application/soap+xml"));
        assertTrue(PayloadCodec.isTextType("text/csv"));
        assertFalse(PayloadCodec.isTextType("application/pdf"));
    }

    // The reader override

    @Test
    public void theOverrideWinsOverTheDeclaredTypeAndTheClassDefault() throws Exception {
        // An undeclared TextMessage read as JSON is parsed.
        PayloadCodec.Received json = PayloadCodec.decode(text("{\"a\":1}", null), "application/json", "poll");
        assertEquals(PayloadCodec.Family.JSON, json.family);
        assertEquals("application/json", json.contentType);
        assertEquals(1, json.responseValue().getAsJsonObject().get("a").getAsInt());

        // An undeclared BytesMessage read as JSON is decoded and parsed, it is not binary.
        PayloadCodec.Received bytesJson = PayloadCodec.decode(
                bytes("[1,2]".getBytes(StandardCharsets.UTF_8), null), "application/json", "poll");
        assertEquals(PayloadCodec.Family.JSON, bytesJson.family);
        assertTrue(bytesJson.json.isJsonArray());

        // A declared JSON body read as text/plain keeps string semantics.
        PayloadCodec.Received plain = PayloadCodec.decode(text("{\"a\":1}", "application/json"), "text/plain",
                "poll");
        assertEquals(PayloadCodec.Family.TEXT, plain.family);
        assertEquals("text/plain", plain.contentType);
        assertEquals(new JsonPrimitive("{\"a\":1}"), plain.responseValue());

        // A declared text body read as an octet stream is binary, the bytes unchanged.
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
        PayloadCodec.Received binary = PayloadCodec.decode(bytes(data, "text/plain"), "application/octet-stream",
                "poll");
        assertTrue(binary.isBinary());
        assertEquals("application/octet-stream", binary.contentType);
        assertArrayEquals(data, binary.bytes);

        // An undeclared XMLContentMessage read as text/plain is plain text.
        XMLContentMessage xml = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
        xml.setXMLContent("<a/>");
        PayloadCodec.Received xmlAsText = PayloadCodec.decode(xml, "text/plain", "poll");
        assertEquals(PayloadCodec.Family.TEXT, xmlAsText.family);
        assertEquals("text/plain", xmlAsText.contentType);

        // No override is the declared type.
        assertEquals("application/json",
                PayloadCodec.decode(text("1", "application/json"), null, "poll").contentType);
    }

    @Test
    public void theOverrideCharsetIsItsOwnElseTheDeclaredElseUtf8() throws Exception {
        byte[] latin = {(byte) 'c', (byte) 'a', (byte) 'f', (byte) 0xE9};
        byte[] utf8 = "caf\u00e9".getBytes(StandardCharsets.UTF_8);

        // The override names no charset, so the declared one applies and the content type reports it.
        PayloadCodec.Received inherited = PayloadCodec.decode(
                bytes(latin, "application/octet-stream; charset=ISO-8859-1"), "text/plain", "poll");
        assertEquals("caf\u00e9", inherited.text);
        assertEquals("ISO-8859-1", inherited.charset);
        assertEquals("text/plain; charset=ISO-8859-1", inherited.contentType);

        // The override's own charset wins over the declared one.
        PayloadCodec.Received own = PayloadCodec.decode(bytes(utf8, "text/plain; charset=ISO-8859-1"),
                "text/plain; charset=UTF-8", "poll");
        assertEquals("caf\u00e9", own.text);
        assertEquals("text/plain; charset=UTF-8", own.contentType);

        // No charset anywhere is UTF-8, and the content type stays as written.
        PayloadCodec.Received none = PayloadCodec.decode(bytes(utf8, null), "text/plain", "poll");
        assertEquals("caf\u00e9", none.text);
        assertNull(none.charset);
        assertEquals("text/plain", none.contentType);

        // Only the charset is inherited. A boundary or any other declared parameter is not.
        PayloadCodec.Received multipart = PayloadCodec.decode(
                bytes(utf8, "multipart/mixed; boundary=xyz; charset=UTF-8"), "text/plain; format=flowed", "poll");
        assertEquals("text/plain; format=flowed; charset=UTF-8", multipart.contentType);
    }

    @Test
    public void resolveContentTypeAppliesTheOneCharsetRule() {
        assertEquals("text/plain; charset=ISO-8859-1",
                PayloadCodec.resolveContentType(null, "text/plain; charset=ISO-8859-1"));
        assertNull(PayloadCodec.resolveContentType(null, null));
        assertEquals("application/json", PayloadCodec.resolveContentType("application/json", null));
        assertEquals("application/json; charset=UTF-16",
                PayloadCodec.resolveContentType("application/json", "text/plain; Charset=\"UTF-16\""));
        assertEquals("application/json; charset=UTF-8",
                PayloadCodec.resolveContentType("application/json; charset=UTF-8", "text/plain; charset=UTF-16"));
        assertEquals("text/csv", PayloadCodec.resolveContentType("text/csv", "multipart/form-data; boundary=b"));
    }

    @Test
    public void theOverrideFaultsAfterReceiptOnContentItCannotRead() {
        // A valid override that selects JSON over content that is not JSON.
        assertPayloadFault(text("not json", null), "application/json");
        assertPayloadFault(bytes("{bad".getBytes(StandardCharsets.UTF_8), "text/plain"), "application/json");
        // An unknown charset inherited from the message.
        assertPayloadFault(bytes("x".getBytes(StandardCharsets.UTF_8), "text/plain; charset=no-such-charset"),
                "text/plain");
        assertPayloadFault(text("x", "text/plain; charset=no-such-charset"), "application/xml");
        // Malformed bytes under the override's own charset.
        assertPayloadFault(bytes(new byte[] {(byte) 0xC3, 0x28}, null), "text/plain; charset=UTF-8");
    }

    @Test
    public void mapAndStreamMessagesKeepTheJsonMappingUnderAnOverride() throws Exception {
        SDTMap map = JCSMPFactory.onlyInstance().createMap();
        map.putString("s", "text");
        MapMessage message = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
        message.setMap(map);
        message.setHTTPContentType("text/plain");
        PayloadCodec.Received received = PayloadCodec.decode(message, "application/octet-stream", "poll");
        assertEquals(PayloadCodec.Family.JSON, received.family);
        assertEquals("application/json", received.contentType);
        assertEquals("text", received.json.getAsJsonObject().get("s").getAsString());
    }

    // The response variable

    @Test
    public void theResponseValueAndItsAttributes() throws Exception {
        Map<String, Object> attributes = new HashMap<>();
        PayloadCodec.Received json = PayloadCodec.decode(text("{\"a\":[1]}", "application/json"), "poll");
        assertEquals(json.json, PayloadCodec.prepareResponse(null, json, Boolean.FALSE, attributes, "poll"));
        assertEquals("application/json", attributes.get(SolaceConstants.RESULT_CONTENT_TYPE));
        assertFalse(attributes.containsKey(SolaceConstants.RESULT_PAYLOAD_ENCODING));

        attributes.clear();
        PayloadCodec.Received scalar = PayloadCodec.decode(text("true", "application/json"), "poll");
        assertEquals(new JsonPrimitive(true), PayloadCodec.prepareResponse(null, scalar, null, attributes, "poll"));

        attributes.clear();
        byte[] data = {0, (byte) 0xFF, 0x10};
        PayloadCodec.Received binary = PayloadCodec.decode(bytes(data, "image/png"), "poll");
        assertEquals(new JsonPrimitive(Base64.getEncoder().encodeToString(data)),
                PayloadCodec.prepareResponse(null, binary, Boolean.FALSE, attributes, "poll"));
        assertEquals("image/png", attributes.get(SolaceConstants.RESULT_CONTENT_TYPE));
        assertEquals(SolaceConstants.PAYLOAD_ENCODING_BASE64,
                attributes.get(SolaceConstants.RESULT_PAYLOAD_ENCODING));

        attributes.clear();
        byte[] latin = {(byte) 'c', (byte) 'a', (byte) 'f', (byte) 0xE9};
        PayloadCodec.Received overridden = PayloadCodec.decode(
                bytes(latin, "application/octet-stream; charset=ISO-8859-1"), "text/plain", "poll");
        assertEquals(new JsonPrimitive("caf\u00e9"),
                PayloadCodec.prepareResponse(null, overridden, Boolean.FALSE, attributes, "poll"));
        assertEquals("text/plain; charset=ISO-8859-1", attributes.get(SolaceConstants.RESULT_CONTENT_TYPE));
        assertFalse(attributes.containsKey(SolaceConstants.RESULT_PAYLOAD_ENCODING));
    }

    // Body replacement

    // Send side

    @Test
    public void outgoingJsonComesFromTheJsonFormatter() throws Exception {
        Axis2MessageContext synCtx = context();
        org.apache.axis2.context.MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        JsonUtil.getNewJsonPayload(axis2Mc, "{\"a\":\"é\"}", true, true);
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/json");
        axis2Mc.setProperty(JSON_STRING_PROPERTY, "{\"stale\":true}");
        axis2Mc.getOperationContext().setProperty(MESSAGE_AS_BYTES_PROPERTY, "stale".getBytes(StandardCharsets.UTF_8));

        PayloadCodec.Outgoing outgoing = PayloadCodec.readOutgoing(synCtx, "publishMessage");
        assertFalse(outgoing.binary);
        assertEquals("application/json", outgoing.mediaType);
        assertEquals("UTF-8", outgoing.charset);
        assertEquals("application/json; charset=UTF-8", outgoing.contentType());
        assertEquals("{\"a\":\"é\"}", new String(outgoing.bytes, StandardCharsets.UTF_8));
        assertNull(axis2Mc.getProperty(JSON_STRING_PROPERTY));
        assertNull(axis2Mc.getOperationContext().getProperty(MESSAGE_AS_BYTES_PROPERTY));

        // Without a message type the platform selects the formatter, the XML one in a REST context.
        axis2Mc.removeProperty(Constants.Configuration.MESSAGE_TYPE);
        assertEquals("application/xml", PayloadCodec.readOutgoing(synCtx, "publishMessage").mediaType);
    }

    @Test
    public void outgoingXmlComesFromTheXmlFormatterAndSoapTypesSendTheFirstElement() throws Exception {
        Axis2MessageContext synCtx = context();
        org.apache.axis2.context.MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        axis2Mc.getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<jsonObject><a>1</a></jsonObject>"));
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/xml");
        PayloadCodec.Outgoing xml = PayloadCodec.readOutgoing(synCtx, "publishMessage");
        assertEquals("application/xml", xml.mediaType);
        assertEquals("<jsonObject><a>1</a></jsonObject>", new String(xml.bytes, StandardCharsets.UTF_8));

        for (String soapType : new String[] {"text/xml", "application/soap+xml"}) {
            axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, soapType);
            PayloadCodec.Outgoing mapped = PayloadCodec.readOutgoing(synCtx, "publishMessage");
            assertEquals(soapType, "application/xml", mapped.mediaType);
            assertEquals(soapType, "<jsonObject><a>1</a></jsonObject>", new String(mapped.bytes,
                    StandardCharsets.UTF_8));
        }
        axis2Mc.removeProperty(Constants.Configuration.MESSAGE_TYPE);
        assertEquals("application/xml", PayloadCodec.readOutgoing(synCtx, "publishMessage").mediaType);
    }

    @Test
    public void outgoingTextAndBinary() throws Exception {
        Axis2MessageContext synCtx = context();
        org.apache.axis2.context.MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMElement textElement = factory.createOMElement(BaseConstants.DEFAULT_TEXT_WRAPPER);
        textElement.setText("café");
        axis2Mc.getEnvelope().getBody().addChild(textElement);
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "text/plain");
        axis2Mc.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "ISO-8859-1");
        PayloadCodec.Outgoing text = PayloadCodec.readOutgoing(synCtx, "publishMessage");
        assertFalse(text.binary);
        assertEquals("text/plain", text.mediaType);
        assertEquals("ISO-8859-1", text.charset);
        assertEquals("ISO-8859-1", text.encoding);
        assertArrayEquals("café".getBytes(StandardCharsets.ISO_8859_1), text.bytes);

        byte[] data = {0, 1, 2, (byte) 0xFF};
        textElement.detach();
        OMElement binaryElement = factory.createOMElement(BaseConstants.DEFAULT_BINARY_WRAPPER);
        binaryElement.addChild(factory.createOMText(new DataHandler(new ByteArrayDataSource(data, "application/pdf")),
                true));
        axis2Mc.getEnvelope().getBody().addChild(binaryElement);
        // A type registered with a binary formatter writes the bytes of the binary node.
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/pdf");
        axis2Mc.setProperty(Constants.Configuration.CONTENT_TYPE, "application/pdf");
        PayloadCodec.Outgoing binary = PayloadCodec.readOutgoing(synCtx, "publishMessage");
        assertTrue(binary.binary);
        assertArrayEquals(data, binary.bytes);
        assertNull(binary.charset);
        assertEquals(binary.mediaType, binary.contentType());
    }

    @Test
    public void encodeAndDecodeReportInsteadOfReplacing() throws Exception {
        assertArrayEquals(new byte[] {(byte) 0xE9}, PayloadCodec.encode("é", "ISO-8859-1", "publishMessage"));
        assertEquals("a�b", PayloadCodec.decode("a�b".getBytes(StandardCharsets.UTF_8), null, "poll"));
        try {
            PayloadCodec.encode("€", "ISO-8859-1", "publishMessage");
            fail("expected PAYLOAD_ERROR");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
            assertEquals(SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
        }
        try {
            PayloadCodec.decode(new byte[] {(byte) 0xC3, 0x28}, "UTF-8", "poll");
            fail("expected PAYLOAD_ERROR");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
        }
    }

    // Helpers

    static TextMessage text(String body, String contentType) {
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        message.setText(body);
        if (contentType != null) {
            message.setHTTPContentType(contentType);
        }
        return message;
    }

    static BytesMessage bytes(byte[] body, String contentType) {
        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        message.setData(body);
        if (contentType != null) {
            message.setHTTPContentType(contentType);
        }
        return message;
    }

    private static void assertPayloadFault(BytesXMLMessage message) {
        assertPayloadFault(message, null);
    }

    private static void assertPayloadFault(BytesXMLMessage message, String override) {
        try {
            PayloadCodec.decode(message, override, "poll");
            fail("expected PAYLOAD_ERROR");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
            assertEquals(SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
            assertTrue(fault.getMessage(), fault.getMessage().startsWith("poll: "));
        }
    }

    /** A mediation context with the formatters a pack registers, the octet stream one being the platform's. */
    static Axis2MessageContext context() throws Exception {
        AxisConfiguration config = new AxisConfiguration();
        config.addMessageFormatter("application/json", new JsonStreamFormatter());
        config.addMessageFormatter("application/xml", new ApplicationXMLFormatter());
        config.addMessageFormatter("text/xml", new SOAPMessageFormatter());
        config.addMessageFormatter("application/soap+xml", new SOAPMessageFormatter());
        config.addMessageFormatter("text/plain", new PlainTextFormatter());
        config.addMessageFormatter("application/octet-stream", new BinaryFormatter());
        config.addMessageFormatter("application/pdf", new BinaryFormatter());
        org.apache.axis2.context.MessageContext axis2Mc = new org.apache.axis2.context.MessageContext();
        axis2Mc.setConfigurationContext(new ConfigurationContext(config));
        axis2Mc.setOperationContext(new OperationContext());
        axis2Mc.setEnvelope(OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope());
        axis2Mc.setDoingREST(true);
        return new Axis2MessageContext(axis2Mc, new SynapseConfiguration(), null);
    }
}
