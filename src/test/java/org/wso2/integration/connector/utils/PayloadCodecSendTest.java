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

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.OMText;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.BinaryBuilder;
import org.apache.axis2.format.BinaryFormatter;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.axis2.transport.http.MultipartFormDataFormatter;
import org.apache.axis2.transport.http.SOAPMessageFormatter;
import org.apache.http.nio.ContentDecoder;
import org.apache.synapse.commons.formatters.XFormURLEncodedFormatter;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.Pipe;
import org.apache.synapse.transport.passthru.util.ControlledByteBuffer;
import org.junit.Test;
import org.wso2.carbon.relay.BinaryRelayBuilder;
import org.wso2.carbon.relay.ExpandingMessageFormatter;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.connection.MessageBuilder;
import org.wso2.integration.connector.constants.SolaceConstants;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.activation.DataHandler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The way out of publishMessage, sendRequest and sendReply: the body the platform formatter writes, its two
 * adaptations, the wire charset, the untyped entity, the transfer check and the scoped XML declaration encoding.
 */
public class PayloadCodecSendTest {

    private static final OMFactory FACTORY = OMAbstractFactory.getOMFactory();
    private static final String OPERATION = "publishMessage";
    private static final String CURE = "the request has no Content-Type and its body was not built, declare a "
            + "Content-Type on the client or set ContentType before the first content aware mediator";

    @Test
    public void aBinaryWrapperWithBase64TextIsWrittenAsItsBytes() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        byte[] data = {0, 1, 2, (byte) 0xFF};
        OMElement wrapper = FACTORY.createOMElement(BaseConstants.DEFAULT_BINARY_WRAPPER);
        wrapper.setText(Base64.getEncoder().encodeToString(data));
        axis2Mc.getEnvelope().getBody().addChild(wrapper);
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/octet-stream");
        axis2Mc.setProperty(Constants.Configuration.CONTENT_TYPE, "application/pdf");

        PayloadCodec.Outgoing outgoing = PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertTrue(outgoing.binary);
        assertArrayEquals(data, outgoing.bytes);
        assertEquals("application/pdf", outgoing.contentType());
        assertTrue(((OMText) wrapper.getFirstOMChild()).isBinary());
        // The body is preserved, a second operation writes the same bytes.
        assertArrayEquals(data, PayloadCodec.readOutgoing(synCtx, OPERATION).bytes);
    }

    @Test
    public void aBinaryWrapperWithInvalidBase64Faults() throws Exception {
        for (String text : new String[] {"not base64!", "AAEC\n/w==", "A"}) {
            Axis2MessageContext synCtx = context();
            OMElement wrapper = FACTORY.createOMElement(BaseConstants.DEFAULT_BINARY_WRAPPER);
            wrapper.setText(text);
            synCtx.getAxis2MessageContext().getEnvelope().getBody().addChild(wrapper);
            synCtx.getAxis2MessageContext().setProperty(Constants.Configuration.MESSAGE_TYPE,
                    "application/octet-stream");
            String message = assertPayloadFault(synCtx);
            assertEquals(text, OPERATION + ": the binary wrapper of the message body does not hold valid base64 text",
                    message);
            assertEquals(text, wrapper.getText());
        }
    }

    @Test
    public void aSoapTypeSendsTheFirstBodyChildAndNotTheEnvelope() throws Exception {
        SOAPFactory[] factories = {OMAbstractFactory.getSOAP11Factory(), OMAbstractFactory.getSOAP12Factory()};
        String[] types = {"text/xml", "application/soap+xml"};
        // The XML formatter reports text/xml as it is and maps the SOAP 1.2 type to application/xml.
        String[] wireTypes = {"text/xml; charset=UTF-8", "application/xml; charset=UTF-8"};
        for (int i = 0; i < types.length; i++) {
            Axis2MessageContext synCtx = context();
            MessageContext axis2Mc = synCtx.getAxis2MessageContext();
            config(axis2Mc).addMessageFormatter(types[i], new SOAPMessageFormatter());
            axis2Mc.setEnvelope(envelope(factories[i]));
            axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, types[i]);
            axis2Mc.setProperty(Constants.Configuration.CONTENT_TYPE, types[i]);
            PayloadCodec.Outgoing outgoing = PayloadCodec.readOutgoing(synCtx, OPERATION);
            assertEquals(types[i], "<a>1</a>", new String(outgoing.bytes, StandardCharsets.UTF_8));
            assertEquals(types[i], wireTypes[i], outgoing.contentType());
        }

        // Without a message type outside REST the platform selects the SOAP formatter, which is mapped too.
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        axis2Mc.setEnvelope(envelope(OMAbstractFactory.getSOAP11Factory()));
        axis2Mc.setDoingREST(false);
        assertEquals("<a>1</a>", new String(PayloadCodec.readOutgoing(synCtx, OPERATION).bytes,
                StandardCharsets.UTF_8));
    }

    @Test
    public void aTypeWithoutARegisteredFormatterIsWrittenByItsFamily() throws Exception {
        Axis2MessageContext json = context();
        JsonUtil.getNewJsonPayload(json.getAxis2MessageContext(), "{\"a\":1}", true, true);
        setType(json, "application/problem+json");
        PayloadCodec.Outgoing jsonOut = PayloadCodec.readOutgoing(json, OPERATION);
        assertEquals("{\"a\":1}", new String(jsonOut.bytes, StandardCharsets.UTF_8));
        assertEquals("application/problem+json; charset=UTF-8", jsonOut.contentType());

        Axis2MessageContext xml = context();
        xml.getAxis2MessageContext().getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<a>1</a>"));
        setType(xml, "application/vnd.test+xml");
        PayloadCodec.Outgoing xmlOut = PayloadCodec.readOutgoing(xml, OPERATION);
        assertEquals("<a>1</a>", new String(xmlOut.bytes, StandardCharsets.UTF_8));
        assertEquals("application/vnd.test+xml; charset=UTF-8", xmlOut.contentType());

        Axis2MessageContext text = textContext("x,y", null);
        setType(text, "text/csv");
        PayloadCodec.Outgoing textOut = PayloadCodec.readOutgoing(text, OPERATION);
        assertEquals("x,y", new String(textOut.bytes, StandardCharsets.UTF_8));
        assertEquals("text/csv; charset=UTF-8", textOut.contentType());

        Axis2MessageContext binary = context();
        byte[] data = {(byte) 0xC3, 0x28};
        binary.getAxis2MessageContext().getEnvelope().getBody().addChild(binaryWrapper(data, "application/x-bin"));
        setType(binary, "application/x-bin");
        PayloadCodec.Outgoing binaryOut = PayloadCodec.readOutgoing(binary, OPERATION);
        assertTrue(binaryOut.binary);
        assertArrayEquals(data, binaryOut.bytes);
        assertEquals("application/x-bin", binaryOut.contentType());
    }

    @Test
    public void aBinaryBodyGoesAsBytesAndFaultsAsTextOrXml() throws Exception {
        // The binary wrapper is binary under any formatter, here the XML one.
        Axis2MessageContext synCtx = context();
        synCtx.getAxis2MessageContext().getEnvelope().getBody().addChild(binaryWrapper(new byte[] {1, 2},
                "application/octet-stream"));
        setType(synCtx, "application/xml");
        PayloadCodec.Outgoing payload = PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertTrue(payload.binary);
        BytesXMLMessage bytes = MessageBuilder.createMessage("BYTES", payload, synCtx, OPERATION);
        assertArrayEquals(payload.bytes, ((BytesMessage) bytes).getData());
        for (String messageType : new String[] {"TEXT", "XML"}) {
            try {
                MessageBuilder.createMessage(messageType, payload, synCtx, OPERATION);
                fail("expected PAYLOAD_ERROR for " + messageType);
            } catch (SolaceFault fault) {
                assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
                assertEquals(SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
            }
        }
    }

    @Test
    public void theReportingEncoderConvertsToTheWireCharsetOrFaults() throws Exception {
        PayloadCodec.Outgoing latin = PayloadCodec.readOutgoing(textContext("café", "ISO-8859-1"), OPERATION);
        assertArrayEquals("café".getBytes(StandardCharsets.ISO_8859_1), latin.bytes);
        assertEquals("text/plain; charset=ISO-8859-1", latin.contentType());
        assertEquals("ISO-8859-1", latin.encoding);

        Axis2MessageContext xml = context();
        xml.getAxis2MessageContext().getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<a>café</a>"));
        setType(xml, "application/xml");
        xml.getAxis2MessageContext().setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "ISO-8859-1");
        assertArrayEquals("<a>café</a>".getBytes(StandardCharsets.ISO_8859_1),
                PayloadCodec.readOutgoing(xml, OPERATION).bytes);

        assertPayloadFault(textContext("café", "US-ASCII"));
        assertPayloadFault(textContext("€", "ISO-8859-1"));
        Axis2MessageContext xmlAscii = context();
        xmlAscii.getAxis2MessageContext().getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<a>café</a>"));
        setType(xmlAscii, "application/xml");
        xmlAscii.getAxis2MessageContext().setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "US-ASCII");
        assertPayloadFault(xmlAscii);
    }

    @Test
    public void aJsonBodyIsConvertedToTheWireCharsetOrFaults() throws Exception {
        Axis2MessageContext latin = context();
        JsonUtil.getNewJsonPayload(latin.getAxis2MessageContext(), "{\"a\":\"caf\u00e9\"}", true, true);
        setType(latin, "application/json");
        latin.getAxis2MessageContext().setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "ISO-8859-1");
        PayloadCodec.Outgoing out = PayloadCodec.readOutgoing(latin, OPERATION);
        assertArrayEquals("{\"a\":\"caf\u00e9\"}".getBytes(StandardCharsets.ISO_8859_1), out.bytes);
        assertEquals("application/json; charset=ISO-8859-1", out.contentType());
        // The charset of the sequence is back after the call.
        assertEquals("ISO-8859-1",
                latin.getAxis2MessageContext().getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));

        Axis2MessageContext ascii = context();
        JsonUtil.getNewJsonPayload(ascii.getAxis2MessageContext(), "{\"a\":\"caf\u00e9\"}", true, true);
        setType(ascii, "application/json");
        ascii.getAxis2MessageContext().setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "US-ASCII");
        assertPayloadFault(ascii);
        assertEquals("US-ASCII",
                ascii.getAxis2MessageContext().getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
    }

    @Test
    public void theWireContentTypeCarriesOneCharset() throws Exception {
        Axis2MessageContext twice = textContext("a", "ISO-8859-1");
        twice.getAxis2MessageContext().setProperty(Constants.Configuration.CONTENT_TYPE,
                "text/plain; charset=UTF-16");
        assertEquals("text/plain; charset=ISO-8859-1", PayloadCodec.readOutgoing(twice, OPERATION).contentType());

        // Under the suppression flag the formatter leaves the charset out and the written one is added.
        Axis2MessageContext suppressed = textContext("a", null);
        suppressed.getAxis2MessageContext().setProperty(Constants.Configuration.CONTENT_TYPE,
                "text/plain; format=flowed");
        suppressed.getAxis2MessageContext().setProperty(
                Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING, Boolean.FALSE.toString());
        assertEquals("text/plain; format=flowed; charset=UTF-8",
                PayloadCodec.readOutgoing(suppressed, OPERATION).contentType());

        // Binary never gets a charset.
        Axis2MessageContext binary = context();
        binary.getAxis2MessageContext().getEnvelope().getBody().addChild(binaryWrapper(new byte[] {1},
                "application/pdf"));
        setType(binary, "application/octet-stream");
        binary.getAxis2MessageContext().setProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING,
                Boolean.FALSE.toString());
        assertEquals("application/pdf", PayloadCodec.readOutgoing(binary, OPERATION).contentType());
    }

    @Test
    public void anUntouchedUntypedRequestIsBuiltAsOctetStream() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        byte[] data = {'{', 0, (byte) 0xFF, '<'};
        axis2Mc.setProperty(PassThroughConstants.PASS_THROUGH_PIPE, completePipe(data));

        PayloadCodec.Outgoing outgoing = PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertEquals("application/octet-stream", axis2Mc.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals("application/octet-stream", axis2Mc.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertTrue(outgoing.binary);
        assertArrayEquals(data, outgoing.bytes);
    }

    @Test
    public void anUntypedContextAlreadyMarkedBuiltFaultsWithTheCure() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        axis2Mc.setProperty(PassThroughConstants.PASS_THROUGH_PIPE, completePipe(new byte[] {1}));
        axis2Mc.setProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED, Boolean.TRUE);
        assertEquals(OPERATION + ": " + CURE, assertPayloadFault(synCtx));
        assertNull(axis2Mc.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals(Boolean.TRUE, axis2Mc.getProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED));
    }

    @Test
    public void aFailedTransferFaultsBeforeSerialisation() throws Exception {
        Axis2MessageContext synCtx = textContext("hello", null);
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        CountingFormatter formatter = new CountingFormatter(null);
        config(axis2Mc).addMessageFormatter("text/plain", formatter);
        Pipe pipe = emptyPipe();
        pipe.producerError();
        axis2Mc.setProperty(PassThroughConstants.PASS_THROUGH_PIPE, pipe);
        axis2Mc.setProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED, Boolean.TRUE);

        assertTrue(assertPayloadFault(synCtx).contains("not received completely"));
        assertEquals(0, formatter.writes);
        assertSame(pipe, axis2Mc.getProperty(PassThroughConstants.PASS_THROUGH_PIPE));
    }

    @Test
    public void aTransferThatFailsDuringSerialisationFaultsBeforeTheSend() throws Exception {
        Axis2MessageContext synCtx = textContext("hello", null);
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        Pipe pipe = emptyPipe();
        CountingFormatter formatter = new CountingFormatter(pipe);
        config(axis2Mc).addMessageFormatter("text/plain", formatter);
        axis2Mc.setProperty(PassThroughConstants.PASS_THROUGH_PIPE, pipe);
        axis2Mc.setProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED, Boolean.TRUE);

        assertTrue(assertPayloadFault(synCtx).contains("not received completely"));
        assertEquals(1, formatter.writes);
    }

    @Test
    public void theInstalledBodyIsExemptFromTheTransferCheck() throws Exception {
        MessageContext axis2Mc = BodyInstallerTest.context();
        BodyInstaller.install(axis2Mc, PayloadCodec.decode(PayloadCodecTest.text("hello", "text/plain"), "poll"),
                "poll");
        Pipe pipe = emptyPipe();
        pipe.producerError();
        axis2Mc.setProperty(PassThroughConstants.PASS_THROUGH_PIPE, pipe);
        Axis2MessageContext synCtx = new Axis2MessageContext(axis2Mc, new SynapseConfiguration(), null);
        assertEquals("hello", new String(PayloadCodec.readOutgoing(synCtx, OPERATION).bytes,
                StandardCharsets.UTF_8));

        // A body that replaced the installed one is not exempt.
        axis2Mc.getEnvelope().getBody().getFirstElement().detach();
        OMElement other = FACTORY.createOMElement(BaseConstants.DEFAULT_TEXT_WRAPPER);
        other.setText("other");
        axis2Mc.getEnvelope().getBody().addChild(other);
        assertTrue(assertPayloadFault(synCtx).contains("not received completely"));
    }

    @Test
    public void theXmlDeclarationNamesTheWireCharsetAndIsRestored() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        axis2Mc.getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<a>café</a>"));
        setType(synCtx, "application/xml");
        axis2Mc.setProperty(Constants.Configuration.WRITE_XML_DECLARATION, "true");
        axis2Mc.setProperty(Constants.Configuration.XML_DECLARATION_ENCODING, "ISO8859_1");

        PayloadCodec.Outgoing utf8 = PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\" ?><a>café</a>",
                new String(utf8.bytes, StandardCharsets.UTF_8));
        assertEquals("ISO8859_1", axis2Mc.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));

        axis2Mc.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "ISO-8859-1");
        PayloadCodec.Outgoing latin = PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertArrayEquals("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" ?><a>café</a>"
                .getBytes(StandardCharsets.ISO_8859_1), latin.bytes);
        assertEquals("ISO8859_1", axis2Mc.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));

        axis2Mc.removeProperty(Constants.Configuration.XML_DECLARATION_ENCODING);
        PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertNull(axis2Mc.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));
        // The output policy and the charset of the sequence are untouched, and no output format is left.
        assertEquals("true", axis2Mc.getProperty(Constants.Configuration.WRITE_XML_DECLARATION));
        assertEquals("ISO-8859-1", axis2Mc.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertNull(axis2Mc.getProperty(PassThroughConstants.MESSAGE_OUTPUT_FORMAT));
    }

    @Test
    public void theXmlDeclarationIsRestoredAfterAFailingWrite() throws Exception {
        for (String previous : new String[] {"ISO8859_1", null}) {
            Axis2MessageContext synCtx = context();
            MessageContext axis2Mc = synCtx.getAxis2MessageContext();
            FailingXmlFormatter formatter = new FailingXmlFormatter();
            config(axis2Mc).addMessageFormatter("application/x-failing+xml", formatter);
            axis2Mc.getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<a>1</a>"));
            setType(synCtx, "application/x-failing+xml");
            axis2Mc.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "ISO-8859-1");
            if (previous != null) {
                axis2Mc.setProperty(Constants.Configuration.XML_DECLARATION_ENCODING, previous);
            }
            assertPayloadFault(synCtx);
            assertEquals("ISO-8859-1", formatter.declarationDuringTheCall);
            assertEquals(previous, axis2Mc.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));
        }
    }

    @Test
    public void thePackBinaryRelayFormatterWritesTheBytes() throws Exception {
        // application/binary is the control, the pack registers the plain binary formatter for it.
        byte[] raw = {0, 1, 2, 0x0D, 0x0A, (byte) 0xFF};
        for (String type : new String[] {"application/octet-stream", "application/pdf", "application/msword",
                "application/x-unknown", "application/binary"}) {
            PayloadCodec.Outgoing outgoing = relayRoundTrip(bytesMessage(raw, type));
            assertTrue(type, outgoing.binary);
            assertArrayEquals(type, raw, outgoing.bytes);
            assertEquals(type, type, outgoing.mediaType);
        }

        // Both form declarations go out as their bytes, received as a BytesMessage and as a TextMessage.
        String[] forms = {"x=caf%E9",
                "--abc\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\ncaf\u00e9\r\n--abc--\r\n"};
        String[] formTypes = {"application/x-www-form-urlencoded", "multipart/form-data; boundary=abc"};
        for (int i = 0; i < forms.length; i++) {
            for (String charset : new String[] {"UTF-8", "ISO-8859-1"}) {
                String declared = formTypes[i] + "; charset=" + charset;
                byte[] data = forms[i].getBytes(charset);
                for (BytesXMLMessage message : new BytesXMLMessage[] {bytesMessage(data, declared),
                        textMessage(forms[i], declared)}) {
                    PayloadCodec.Outgoing outgoing = relayRoundTrip(message);
                    assertTrue(declared, outgoing.binary);
                    assertArrayEquals(declared, data, outgoing.bytes);
                    assertEquals(declared, declared, outgoing.contentType());
                }
            }
        }

        // A binary wrapper that holds base64 text, the way the File connector leaves a read.
        Axis2MessageContext file = relayContext();
        OMElement wrapper = FACTORY.createOMElement(BaseConstants.DEFAULT_BINARY_WRAPPER);
        wrapper.setText("AAEC/w==");
        file.getAxis2MessageContext().getEnvelope().getBody().addChild(wrapper);
        file.getAxis2MessageContext().setProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED, Boolean.TRUE);
        setType(file, "application/octet-stream");
        assertArrayEquals(new byte[] {0, 1, 2, (byte) 0xFF}, PayloadCodec.readOutgoing(file, OPERATION).bytes);

        // The stock SOAP formatter is still replaced, so the first body element is sent.
        Axis2MessageContext soap = relayContext();
        soap.getAxis2MessageContext().setEnvelope(envelope(OMAbstractFactory.getSOAP11Factory()));
        setType(soap, "text/xml");
        assertEquals("<a>1</a>", new String(PayloadCodec.readOutgoing(soap, OPERATION).bytes,
                StandardCharsets.UTF_8));
    }

    @Test
    public void theBinaryRelayFormatterNeverWritesAnEnvelope() throws Exception {
        // An empty request body under a binary type has no binary element and goes out as no bytes.
        Axis2MessageContext empty = relayContext();
        setType(empty, "application/octet-stream");
        PayloadCodec.Outgoing none = PayloadCodec.readOutgoing(empty, OPERATION);
        assertEquals(0, none.length());
        assertTrue(none.binary);
        assertEquals("application/octet-stream", none.contentType());

        // Any other element under a binary type is sent as the element and not inside an envelope.
        Axis2MessageContext xml = relayContext();
        xml.getAxis2MessageContext().getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<a>1</a>"));
        setType(xml, "application/pdf");
        PayloadCodec.Outgoing element = PayloadCodec.readOutgoing(xml, OPERATION);
        assertEquals("<a>1</a>", new String(element.bytes, StandardCharsets.UTF_8));
        assertEquals("application/pdf", element.contentType());
    }

    @Test
    public void aRegisteredSoapFormatterSubclassWritesItsOwnBody() throws Exception {
        // Only the pack's binary relay formatter falls back for a body that is not binary. Another subclass of the
        // SOAP formatter is a registered handler: its writer runs, for a normal body and for an empty one.
        for (String body : new String[] {"<value>caf\u00e9</value>", null}) {
            Axis2MessageContext synCtx = relayContext();
            CustomSoapFormatter custom = new CustomSoapFormatter();
            config(synCtx.getAxis2MessageContext()).addMessageFormatter("application/x-custom", custom);
            if (body != null) {
                synCtx.getAxis2MessageContext().getEnvelope().getBody().addChild(AXIOMUtil.stringToOM(body));
            }
            setType(synCtx, "application/x-custom");

            PayloadCodec.Outgoing first = PayloadCodec.readOutgoing(synCtx, OPERATION);
            PayloadCodec.Outgoing second = PayloadCodec.readOutgoing(synCtx, "sendRequest");
            String expected = body == null ? "custom=" : "custom=caf\u00e9";
            assertEquals(expected, new String(first.bytes, StandardCharsets.UTF_8));
            assertArrayEquals(first.bytes, second.bytes);
            assertEquals("application/x-custom", first.contentType());
            assertEquals(2, custom.writes);
        }
    }

    @Test
    public void anEmptyContentTypeGetsTheTypeThatSelectedTheFormatter() throws Exception {
        // A request without a Content-Type header whose body a sequence built as XML.
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        axis2Mc.getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<order><id>42</id></order>"));
        axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/xml");
        axis2Mc.setProperty(Constants.Configuration.CONTENT_TYPE, "");

        PayloadCodec.Outgoing outgoing = PayloadCodec.readOutgoing(synCtx, OPERATION);
        assertEquals("application/xml; charset=UTF-8", outgoing.contentType());
        assertEquals("<order><id>42</id></order>", new String(outgoing.bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void theBodyIsPreservedForASecondOperation() throws Exception {
        Axis2MessageContext synCtx = context();
        MessageContext axis2Mc = synCtx.getAxis2MessageContext();
        OMElement body = AXIOMUtil.stringToOM("<a><b>1</b></a>");
        axis2Mc.getEnvelope().getBody().addChild(body);
        setType(synCtx, "application/xml");
        byte[] first = PayloadCodec.readOutgoing(synCtx, OPERATION).bytes;
        assertArrayEquals(first, PayloadCodec.readOutgoing(synCtx, "sendRequest").bytes);
        assertSame(body, axis2Mc.getEnvelope().getBody().getFirstElement());
        assertFalse(first.length == 0);
    }

    // Helpers

    /** A formatter that counts its writes and can fail the transfer of a pipe while it writes. */
    private static final class CountingFormatter extends PlainTextFormatter {

        private final Pipe pipe;
        int writes;

        CountingFormatter(Pipe pipe) {
            this.pipe = pipe;
        }

        @Override
        public void writeTo(MessageContext messageContext, OMOutputFormat format, OutputStream outputStream,
                            boolean preserve) throws AxisFault {
            writes++;
            super.writeTo(messageContext, format, outputStream, preserve);
            if (pipe != null) {
                pipe.producerError();
            }
        }
    }

    /** A registered handler that extends the SOAP formatter and writes its own representation of the body. */
    private static final class CustomSoapFormatter extends SOAPMessageFormatter {

        int writes;

        @Override
        public void writeTo(MessageContext messageContext, OMOutputFormat format, OutputStream outputStream,
                            boolean preserve) throws AxisFault {
            writes++;
            OMElement first = messageContext.getEnvelope().getBody().getFirstElement();
            try {
                outputStream.write(("custom=" + (first == null ? "" : first.getText()))
                        .getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw AxisFault.makeFault(e);
            }
        }

        @Override
        public String getContentType(MessageContext messageContext, OMOutputFormat format, String soapAction) {
            return "application/x-custom";
        }
    }

    /** The stock XML formatter that records the declaration encoding it sees and then fails. */
    private static final class FailingXmlFormatter extends ApplicationXMLFormatter {

        Object declarationDuringTheCall;

        @Override
        public void writeTo(MessageContext messageContext, OMOutputFormat format, OutputStream outputStream,
                            boolean preserve) throws AxisFault {
            declarationDuringTheCall = messageContext.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING);
            throw new AxisFault("the formatter failed");
        }
    }

    private static Axis2MessageContext context() throws Exception {
        return new Axis2MessageContext(BodyInstallerTest.context(), new SynapseConfiguration(), null);
    }

    /** A context with the binary, document and form handlers the pack registers in axis2.xml. */
    private static Axis2MessageContext relayContext() throws Exception {
        Axis2MessageContext synCtx = context();
        AxisConfiguration config = config(synCtx.getAxis2MessageContext());
        for (String type : new String[] {"application/octet-stream", "application/pdf", "application/msword"}) {
            config.addMessageBuilder(type, new BinaryRelayBuilder());
            config.addMessageFormatter(type, new ExpandingMessageFormatter());
        }
        config.addMessageBuilder("application/binary", new BinaryBuilder());
        config.addMessageFormatter("application/binary", new BinaryFormatter());
        config.addMessageFormatter("application/x-www-form-urlencoded", new XFormURLEncodedFormatter());
        config.addMessageFormatter("multipart/form-data", new MultipartFormDataFormatter());
        config.addMessageFormatter("text/xml", new SOAPMessageFormatter());
        return synCtx;
    }

    /** Installs a received message as poll does with overwriteBody, then writes it twice. */
    private static PayloadCodec.Outgoing relayRoundTrip(BytesXMLMessage message) throws Exception {
        Axis2MessageContext synCtx = relayContext();
        BodyInstaller.install(synCtx.getAxis2MessageContext(), PayloadCodec.decode(message, "poll"), "poll");
        PayloadCodec.Outgoing first = PayloadCodec.readOutgoing(synCtx, OPERATION);
        PayloadCodec.Outgoing second = PayloadCodec.readOutgoing(synCtx, "sendRequest");
        assertArrayEquals(first.bytes, second.bytes);
        assertEquals(first.contentType(), second.contentType());
        return first;
    }

    private static BytesMessage bytesMessage(byte[] data, String type) {
        BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        message.setData(data);
        message.setHTTPContentType(type);
        return message;
    }

    private static TextMessage textMessage(String text, String type) {
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        message.setText(text);
        message.setHTTPContentType(type);
        return message;
    }

    private static Axis2MessageContext textContext(String text, String charset) throws Exception {
        Axis2MessageContext synCtx = context();
        OMElement element = FACTORY.createOMElement(BaseConstants.DEFAULT_TEXT_WRAPPER);
        element.setText(text);
        synCtx.getAxis2MessageContext().getEnvelope().getBody().addChild(element);
        synCtx.getAxis2MessageContext().setProperty(Constants.Configuration.MESSAGE_TYPE, "text/plain");
        if (charset != null) {
            synCtx.getAxis2MessageContext().setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charset);
        }
        return synCtx;
    }

    private static void setType(Axis2MessageContext synCtx, String type) {
        synCtx.getAxis2MessageContext().setProperty(Constants.Configuration.MESSAGE_TYPE, type);
        synCtx.getAxis2MessageContext().setProperty(Constants.Configuration.CONTENT_TYPE, type);
    }

    private static AxisConfiguration config(MessageContext axis2Mc) {
        return axis2Mc.getConfigurationContext().getAxisConfiguration();
    }

    private static OMElement binaryWrapper(byte[] data, String type) {
        OMElement wrapper = FACTORY.createOMElement(BaseConstants.DEFAULT_BINARY_WRAPPER);
        wrapper.addChild(FACTORY.createOMText(new DataHandler(new ByteArrayDataSource(data, type)), true));
        return wrapper;
    }

    /** An envelope with a header block and two body children. */
    private static SOAPEnvelope envelope(SOAPFactory factory) throws Exception {
        SOAPEnvelope envelope = factory.getDefaultEnvelope();
        envelope.getHeader().addHeaderBlock("h", factory.createOMNamespace("urn:test", "t")).setText("header");
        envelope.getBody().addChild(AXIOMUtil.stringToOM("<a>1</a>"));
        envelope.getBody().addChild(AXIOMUtil.stringToOM("<b>2</b>"));
        return envelope;
    }

    private static Pipe emptyPipe() {
        return new Pipe(new ControlledByteBuffer(ByteBuffer.allocate(8)), "test", null);
    }

    /** A pipe that holds the whole entity, its producer complete. */
    private static Pipe completePipe(byte[] entity) {
        ByteBuffer buffer = ByteBuffer.allocate(entity.length + 8);
        buffer.put(entity);
        Pipe pipe = new Pipe(new ControlledByteBuffer(buffer), "test", null);
        pipe.forceProducerComplete(new ContentDecoder() {
            @Override
            public int read(ByteBuffer dst) {
                return -1;
            }

            @Override
            public boolean isCompleted() {
                return false;
            }
        });
        return pipe;
    }

    /** Asserts a payload fault before any broker call and returns its message. */
    private static String assertPayloadFault(Axis2MessageContext synCtx) {
        try {
            PayloadCodec.readOutgoing(synCtx, OPERATION);
            fail("expected PAYLOAD_ERROR");
            return null;
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
            assertEquals(SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
            assertTrue(fault.getMessage(), fault.getMessage().startsWith(OPERATION + ": "));
            return fault.getMessage();
        }
    }
}
