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

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.OMText;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.builder.MultipartFormDataBuilder;
import org.apache.axis2.builder.XFormURLEncodedBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.OperationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.BinaryBuilder;
import org.apache.axis2.format.BinaryFormatter;
import org.apache.axis2.format.PlainTextBuilder;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.json.JSONBadgerfishMessageFormatter;
import org.apache.axis2.json.JSONBadgerfishOMBuilder;
import org.apache.axis2.json.JSONBuilder;
import org.apache.axis2.json.JSONMessageFormatter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.synapse.commons.json.JsonStreamBuilder;
import org.apache.synapse.commons.json.JsonStreamFormatter;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.AbstractListMediator;
import org.apache.synapse.transport.nhttp.NhttpConstants;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.util.DeferredMessageBuilder;
import org.junit.Test;
import org.wso2.integration.connector.SolaceFault;
import org.wso2.integration.connector.constants.SolaceConstants;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.activation.DataHandler;

import static org.apache.synapse.commons.json.Constants.JSON_STRING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.wso2.integration.connector.utils.PayloadCodecTest.bytes;
import static org.wso2.integration.connector.utils.PayloadCodecTest.text;

/**
 * The body install over a plain axis2 message context with the builders and formatters the pack registers.
 */
public class BodyInstallerTest {

    private static final String UTF_8 = StandardCharsets.UTF_8.name();
    private static final String MESSAGE_AS_BYTES = "MESSAGE_AS_BYTES";
    private static final long BUILD_TIMEOUT_SECONDS = 20;

    // Each body class through the install

    @Test
    public void jsonIsTheLazyJsonStreamWithCanonicalUtf8() throws Exception {
        MessageContext ctx = context();
        install(ctx, text("{\"a\":\"café\"}", "application/json; charset=ISO-8859-1"));
        assertTrue(JsonUtil.hasAJsonPayload(ctx));
        assertEquals("{\"a\":\"café\"}", jsonText(ctx));
        assertEquals("application/json", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/json; charset=UTF-8", ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals(UTF_8, ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
    }

    @Test
    public void xmlIsBuiltLazilyAndAMalformedRootFaultsAtTheBuild() throws Exception {
        MessageContext ctx = context();
        install(ctx, text("<root><item>value</root>", "application/xml"));
        OMElement root = ctx.getEnvelope().getBody().getFirstElement();
        assertEquals("root", root.getLocalName());
        assertEquals("application/xml", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        try {
            root.toString();
            fail("the malformed document must fail where it is first consumed");
        } catch (OMException expected) {
            // The parse error surfaces when mediation consumes the body.
        }

        assertPayloadFault(() -> install(context(), text("not xml", "application/xml")));
    }

    @Test
    public void aBuildFailureKeepsItsCodeWhenThePlatformHandlesTheFault() throws Exception {
        // The control: a fault that chains the parser's exception is rewritten by the platform.
        Exception parser = null;
        try {
            AXIOMUtil.stringToOM("not xml");
        } catch (Exception e) {
            parser = e;
        }
        assertTrue(platformSeesAParserFailure(new SolaceFault(SolaceConstants.FAULT_PAYLOAD_ERROR,
                SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, "poll: " + parser.getMessage(), parser)));
        try {
            install(context(), text("not xml", "application/xml"));
            fail("expected PAYLOAD_ERROR");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
            assertNull(fault.getCause());
            assertTrue(fault.getMessage(), fault.getMessage().startsWith(
                    "poll: cannot set the received body declared application/xml as the message body: "));
            assertTrue(fault.getMessage(), fault.getMessage().contains("Unexpected character 'n'"));
            assertFalse(fault.getMessage(), platformSeesAParserFailure(fault));
        }
    }

    @Test
    public void soapTypesAreBuiltByTheXmlBuilder() throws Exception {
        for (String soapType : new String[]{"text/xml", "application/soap+xml; action=x"}) {
            MessageContext ctx = context();
            install(ctx, text("<a><b/></a>", soapType));
            assertEquals("a", ctx.getEnvelope().getBody().getFirstElement().getLocalName());
            assertEquals("application/xml", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
            assertEquals(MediaType.parse(soapType).withCharset(UTF_8),
                    ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        }
    }

    @Test
    public void textIsTheTextWrapper() throws Exception {
        MessageContext ctx = context();
        install(ctx, text("a,b\n1,2", "text/csv"));
        OMElement first = ctx.getEnvelope().getBody().getFirstElement();
        assertEquals(BaseConstants.DEFAULT_TEXT_WRAPPER, first.getQName());
        assertEquals("a,b\n1,2", first.getText());
        assertEquals("text/plain", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("text/csv; charset=UTF-8", ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
    }

    @Test
    public void binaryIsTheNativeBinaryBodyWithItsBytesAndDeclaration() throws Exception {
        MessageContext ctx = context();
        ctx.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "UTF-16");
        byte[] data = {0, 1, 2, (byte) 0xFF};
        install(ctx, bytes(data, "application/pdf"));
        assertArrayEquals(data, binaryBytes(ctx));
        assertEquals("application/octet-stream", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/pdf", ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertNull(ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
    }

    @Test
    public void structuredMessagesAreJson() throws Exception {
        MapMessage map = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
        SDTMap sdt = JCSMPFactory.onlyInstance().createMap();
        sdt.putString("name", "café");
        map.setMap(sdt);
        MessageContext ctx = context();
        install(ctx, map);
        assertTrue(JsonUtil.hasAJsonPayload(ctx));
        assertEquals("{\"name\":\"café\"}", jsonText(ctx));
        assertEquals("application/json; charset=UTF-8", ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
    }

    @Test
    public void anUnregisteredTypeTakesItsFamilyAndNeverXml() throws Exception {
        MessageContext json = context();
        install(json, text("{\"title\":\"x\"}", "application/problem+json"));
        assertTrue(JsonUtil.hasAJsonPayload(json));
        assertEquals("application/json", json.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/problem+json; charset=UTF-8",
                json.getProperty(Constants.Configuration.CONTENT_TYPE));

        MessageContext xml = context();
        install(xml, text("<t/>", "application/vnd.test+xml"));
        assertEquals("t", xml.getEnvelope().getBody().getFirstElement().getLocalName());
        assertEquals("application/xml", xml.getProperty(Constants.Configuration.MESSAGE_TYPE));

        MessageContext binary = context();
        byte[] data = "<not-xml".getBytes(StandardCharsets.UTF_8);
        install(binary, bytes(data, "application/x-custom-binary"));
        assertArrayEquals(data, binaryBytes(binary));
        assertEquals("application/octet-stream", binary.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/x-custom-binary", binary.getProperty(Constants.Configuration.CONTENT_TYPE));
    }

    @Test
    public void aFormIsCarriedAsOpaqueBytesWhateverIsRegistered() throws Exception {
        byte[] data = "a=caf%E9".getBytes(StandardCharsets.US_ASCII);
        String declared = "application/x-www-form-urlencoded; charset=ISO-8859-1";
        MessageContext ctx = context();
        install(ctx, bytes(data, declared));
        assertArrayEquals(data, binaryBytes(ctx));
        assertEquals("application/octet-stream", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals(declared, ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals("ISO-8859-1", ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));

        MessageContext multipart = context();
        String boundary = "multipart/form-data; boundary=\"x;y\"";
        install(multipart, bytes(data, boundary));
        assertArrayEquals(data, binaryBytes(multipart));
        assertEquals(boundary, multipart.getProperty(Constants.Configuration.CONTENT_TYPE));
    }

    @Test
    public void aFormTextMessageKeepsItsDeclaredCharset() throws Exception {
        String latin1 = "application/x-www-form-urlencoded; charset=ISO-8859-1";
        MessageContext ctx = context();
        install(ctx, text("name=caf%E9", latin1));
        assertArrayEquals("name=caf%E9".getBytes(StandardCharsets.ISO_8859_1), binaryBytes(ctx));
        assertEquals("application/octet-stream", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals(latin1, ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals("ISO-8859-1", ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));

        // With no charset the text is encoded as UTF-8 and the declaration gains nothing.
        String plain = "application/x-www-form-urlencoded";
        MessageContext utf8 = context();
        install(utf8, text("v=\u00e9", plain));
        assertArrayEquals("v=\u00e9".getBytes(StandardCharsets.UTF_8), binaryBytes(utf8));
        assertEquals(plain, utf8.getProperty(Constants.Configuration.CONTENT_TYPE));

        // A form text its charset cannot encode faults and the body stays as it was.
        MessageContext invalid = context();
        OMElement before = invalid.getEnvelope().getBody().getFirstElement();
        assertPayloadFault(() -> install(invalid, text("name=\u540d", latin1)));
        assertEquals(before, invalid.getEnvelope().getBody().getFirstElement());
    }

    @Test
    public void cafeUnderACharacterTypeOutsideTheFamiliesIsDecodedStrictly() throws Exception {
        // The registered builder of the type is the plain text builder, so the bytes are text.
        MessageContext ctx = context();
        install(ctx, bytes("café".getBytes(StandardCharsets.ISO_8859_1),
                "application/x-custom-text; charset=ISO-8859-1; v=1"));
        assertEquals("café", ctx.getEnvelope().getBody().getFirstElement().getText());
        assertEquals("application/x-custom-text", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/x-custom-text; v=1; charset=UTF-8",
                ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals(UTF_8, ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));

        // A family type decoded on receipt is canonical too.
        MessageContext csv = context();
        install(csv, bytes("café".getBytes(StandardCharsets.ISO_8859_1), "text/csv; charset=ISO-8859-1"));
        assertEquals("café", csv.getEnvelope().getBody().getFirstElement().getText());
        assertEquals("text/csv; charset=UTF-8", csv.getProperty(Constants.Configuration.CONTENT_TYPE));

        // Bytes that are not valid text in the declared charset fault before the body changes.
        MessageContext invalid = context();
        OMElement before = invalid.getEnvelope().getBody().getFirstElement();
        assertPayloadFault(() -> install(invalid, bytes(new byte[]{(byte) 0xC3, 0x28},
                "application/x-custom-text; charset=UTF-8")));
        assertEquals(before, invalid.getEnvelope().getBody().getFirstElement());
        assertNull(invalid.getProperty(Constants.Configuration.MESSAGE_TYPE));
    }

    @Test
    public void aTextMessageUnderATypeThatIsNotTextIsItsUtf8Bytes() throws Exception {
        PayloadCodec.Received received = PayloadCodec.decode(text("héllo", "application/octet-stream"), "poll");
        assertTrue(received.isBinary());
        byte[] utf8 = "héllo".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(utf8, received.bytes);

        Map<String, Object> attributes = new HashMap<>();
        JsonElement value = PayloadCodec.prepareResponse(null, received, Boolean.FALSE, attributes, "poll");
        assertEquals(new JsonPrimitive(Base64.getEncoder().encodeToString(utf8)), value);
        assertEquals("base64", attributes.get(SolaceConstants.RESULT_PAYLOAD_ENCODING));

        MessageContext ctx = context();
        BodyInstaller.install(ctx, received, "poll");
        assertArrayEquals(utf8, binaryBytes(ctx));
        assertEquals("application/octet-stream", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/octet-stream; charset=UTF-8", ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
    }

    @Test
    public void prepareResponseWithOverwriteBodyInstallsTheBody() throws Exception {
        MessageContext ctx = context();
        Axis2MessageContext synCtx = new Axis2MessageContext(ctx, new SynapseConfiguration(), null);
        Map<String, Object> attributes = new HashMap<>();
        assertEquals(JsonNull.INSTANCE, PayloadCodec.prepareResponse(synCtx,
                PayloadCodec.decode(bytes(new byte[]{1, 2}, "application/pdf"), "poll"), Boolean.TRUE, attributes,
                "poll"));
        assertEquals("application/pdf", attributes.get(SolaceConstants.RESULT_CONTENT_TYPE));
        assertTrue(BodyInstaller.isInstalledBody(ctx));
    }

    // The ordered replacement

    @Test
    public void theOldBodyStateIsClearedAndTheNewBodyDescribed() throws Exception {
        MessageContext ctx = context();
        JsonUtil.getNewJsonPayload(ctx, "{\"old\":true}", true, true);
        ctx.setProperty(JSON_STRING, "{\"old\":true}");
        ctx.getOperationContext().setProperty(MESSAGE_AS_BYTES, new byte[]{1});
        ctx.setProperty(Constants.Configuration.ENABLE_MTOM, Boolean.TRUE);
        ctx.setProperty(Constants.Configuration.ENABLE_SWA, Boolean.TRUE);
        ctx.setProperty(PassThroughConstants.MESSAGE_OUTPUT_FORMAT, new OMOutputFormat());
        ctx.setProperty(NhttpConstants.FORCE_HTTP_CONTENT_LENGTH, Boolean.TRUE);
        ctx.setProperty(PassThroughConstants.COPY_CONTENT_LENGTH_FROM_INCOMING, Boolean.TRUE);
        ctx.setProperty(PassThroughConstants.ORGINAL_CONTEN_LENGTH, "10");
        ctx.setProperty(PassThroughConstants.PASSTROUGH_MESSAGE_LENGTH, 10);
        ctx.setProperty(PassThroughConstants.BUFFERED_INPUT_STREAM, new ByteArrayInputStream(new byte[]{1}));
        ctx.setProperty(DeferredMessageBuilder.RELAY_FORMATTERS_MAP, new HashMap<>());
        ctx.setProperty(Constants.Configuration.XML_PROCESSING_INSTRUCTION_ELEMENTS, new ArrayList<>());
        ctx.setProperty(Constants.Configuration.XML_DECLARATION_ENCODING, "UTF-16");
        ctx.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "UTF-16");
        ctx.setProperty(PassThroughConstants.NO_ENTITY_BODY, Boolean.TRUE);
        ctx.setProperty(Constants.Configuration.WRITE_XML_DECLARATION, "true");
        ctx.setProperty(Constants.Configuration.PRESERVE_XML_PROCESSING_INSTRUCTIONS, "true");
        Map<String, Object> headers = new TreeMap<>();
        headers.put("content-type", "application/json");
        headers.put("CONTENT-LENGTH", "10");
        headers.put("X-Other", "kept");
        ctx.setProperty(MessageContext.TRANSPORT_HEADERS, headers);

        // The document declares another encoding than its canonical bytes, the builder records it.
        install(ctx, text("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><a>café</a>",
                "application/xml; charset=ISO-8859-1"));

        assertEquals("café", ctx.getEnvelope().getBody().getFirstElement().getText());
        assertFalse(JsonUtil.hasAJsonPayload(ctx));
        assertNull(ctx.getProperty(JSON_STRING));
        assertNull(ctx.getOperationContext().getProperty(MESSAGE_AS_BYTES));
        assertNull(ctx.getProperty(Constants.Configuration.ENABLE_MTOM));
        assertNull(ctx.getProperty(Constants.Configuration.ENABLE_SWA));
        assertNull(ctx.getProperty(PassThroughConstants.MESSAGE_OUTPUT_FORMAT));
        assertNull(ctx.getProperty(NhttpConstants.FORCE_HTTP_CONTENT_LENGTH));
        assertEquals("true", ctx.getProperty(PassThroughConstants.DISABLE_CHUNKING));
        assertNull(ctx.getProperty(PassThroughConstants.COPY_CONTENT_LENGTH_FROM_INCOMING));
        assertNull(ctx.getProperty(PassThroughConstants.ORGINAL_CONTEN_LENGTH));
        assertNull(ctx.getProperty(PassThroughConstants.PASSTROUGH_MESSAGE_LENGTH));
        assertNull(ctx.getProperty(PassThroughConstants.BUFFERED_INPUT_STREAM));
        assertNull(ctx.getProperty(DeferredMessageBuilder.RELAY_FORMATTERS_MAP));
        assertNull(ctx.getProperty(Constants.Configuration.XML_PROCESSING_INSTRUCTION_ELEMENTS));
        assertNull(ctx.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));
        assertNull(ctx.getProperty(PassThroughConstants.NO_ENTITY_BODY));
        assertEquals("true", ctx.getProperty(Constants.Configuration.WRITE_XML_DECLARATION));
        assertEquals("true", ctx.getProperty(Constants.Configuration.PRESERVE_XML_PROCESSING_INSTRUCTIONS));

        assertEquals("application/xml", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/xml; charset=UTF-8", ctx.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals(UTF_8, ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertEquals("false", ctx.getProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING));
        assertEquals(Boolean.TRUE, ctx.getProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED));
        assertEquals(2, headers.size());
        assertEquals("application/xml; charset=UTF-8", headers.get(HTTPConstants.HEADER_CONTENT_TYPE));
        assertEquals("kept", headers.get("X-Other"));
        assertFalse(headers.containsKey(HTTPConstants.HEADER_CONTENT_LENGTH));
        assertFalse(headers.containsKey("CONTENT-LENGTH"));
    }

    @Test
    public void withoutTheForcedLengthOnlyTheStaleLengthsGo() throws Exception {
        MessageContext ctx = context();
        ctx.setProperty(PassThroughConstants.ORGINAL_CONTEN_LENGTH, "10");
        install(ctx, text("x", "text/plain"));
        assertNull(ctx.getProperty(PassThroughConstants.ORGINAL_CONTEN_LENGTH));
        assertNull(ctx.getProperty(PassThroughConstants.DISABLE_CHUNKING));
    }

    // The ownership record

    @Test
    public void theOwnershipRecordFollowsTheIdentityOfTheBody() throws Exception {
        MessageContext ctx = context();
        assertFalse(BodyInstaller.isInstalledBody(ctx));
        install(ctx, text("<a/>", "application/xml"));
        assertTrue(BodyInstaller.isInstalledBody(ctx));

        // Mediation that edits the body in place keeps the identity.
        OMFactory factory = OMAbstractFactory.getOMFactory();
        OMElement first = ctx.getEnvelope().getBody().getFirstElement();
        first.addChild(factory.createOMElement("b", null));
        assertTrue(BodyInstaller.isInstalledBody(ctx));

        // A replaced first element does not match.
        first.detach();
        ctx.getEnvelope().getBody().addChild(factory.createOMElement("c", null));
        assertFalse(BodyInstaller.isInstalledBody(ctx));

        // The next install replaces the record, and a replaced envelope does not match.
        install(ctx, text("{\"a\":1}", "application/json"));
        assertTrue(BodyInstaller.isInstalledBody(ctx));
        ctx.setEnvelope(OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope());
        assertFalse(BodyInstaller.isInstalledBody(ctx));
    }

    // The legacy JSON builders and the formatters that append a charset

    @Test
    public void incompleteJsonFaultsBeforeALegacyJsonBuilder() throws Exception {
        for (String body : new String[]{"{\"n\"", "{\"n\":", "{\"n\":{\"$\":\"1\"}} x", ""}) {
            AtomicReference<Throwable> outcome = new AtomicReference<>();
            Thread build = new Thread(() -> {
                try {
                    install(context(), text(body, "application/json/badgerfish"));
                } catch (Throwable e) {
                    outcome.set(e);
                }
            });
            build.setDaemon(true);
            build.start();
            build.join(TimeUnit.SECONDS.toMillis(BUILD_TIMEOUT_SECONDS));
            assertFalse("the install of " + body + " did not finish", build.isAlive());
            assertTrue(String.valueOf(outcome.get()), outcome.get() instanceof SolaceFault);
            SolaceFault fault = (SolaceFault) outcome.get();
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
            assertEquals("poll: the body read as application/json/badgerfish is not valid JSON",
                    fault.getMessage());
        }
    }

    @Test
    public void validBadgerfishIsBuiltAndItsContentTypeCarriesOneCharset() throws Exception {
        MessageContext ctx = context();
        Map<String, Object> headers = new HashMap<>();
        ctx.setProperty(MessageContext.TRANSPORT_HEADERS, headers);
        install(ctx, bytes("{\"a\":{\"$\":\"café\"}}".getBytes(StandardCharsets.ISO_8859_1),
                "application/json/badgerfish; charset=ISO-8859-1; v=1"));
        OMElement first = ctx.getEnvelope().getBody().getFirstElement();
        assertEquals("a", first.getLocalName());
        assertEquals("café", first.getText());
        assertEquals("application/json/badgerfish", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals(UTF_8, ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertDescribedWithOneCharset(ctx, headers, "application/json/badgerfish; v=1; charset=UTF-8");
    }

    @Test
    public void javascriptKeepsItsParametersAndOneCharsetInContentType() throws Exception {
        MessageContext ctx = context();
        Map<String, Object> headers = new HashMap<>();
        ctx.setProperty(MessageContext.TRANSPORT_HEADERS, headers);
        install(ctx, text("{\"a\":\"1\"}", "text/javascript; v=1"));
        assertEquals("text/javascript", ctx.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals(UTF_8, ctx.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertDescribedWithOneCharset(ctx, headers, "text/javascript; v=1; charset=UTF-8");
    }

    // Helpers

    /**
     * ContentType and the transport Content-Type header are the same complete type with one charset, and
     * setCharacterEncoding is false. The HTTP paths that read ContentType send it as it is.
     */
    private static void assertDescribedWithOneCharset(MessageContext ctx, Map<String, Object> headers,
                                                      String expected) {
        String contentType = (String) ctx.getProperty(Constants.Configuration.CONTENT_TYPE);
        String header = (String) headers.get(HTTPConstants.HEADER_CONTENT_TYPE);
        assertEquals(expected, contentType);
        assertEquals(contentType, header);
        assertEquals(contentType, 1, charsetCount(contentType));
        assertEquals(header, 1, charsetCount(header));
        assertEquals(Boolean.FALSE.toString(),
                ctx.getProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING));
    }

    /** The number of charset parameters a content type names. */
    static int charsetCount(String contentType) {
        return contentType.toLowerCase(Locale.ROOT).split("charset=", -1).length - 1;
    }

    private interface Install {
        void run() throws Exception;
    }

    /**
     * Whether the platform would replace the fault's code with its own message parsing error. The connector throws
     * the fault as a SynapseException with the fault's cause, the class mediator wraps it, and the list mediator
     * matches its build failure pattern against the whole stack trace.
     */
    static boolean platformSeesAParserFailure(SolaceFault fault) throws Exception {
        Field field = AbstractListMediator.class.getDeclaredField("MSG_BUILD_FAILURE_EXCEPTION_PATTERN");
        field.setAccessible(true);
        Throwable thrown = new SynapseException("Error occured in the mediation of the class mediator",
                new SynapseException(fault.name + ": " + fault.getMessage(), fault.getCause()));
        StringWriter trace = new StringWriter();
        thrown.printStackTrace(new PrintWriter(trace));
        return Pattern.compile((String) field.get(null)).matcher(trace.toString()).find();
    }

    private static void install(MessageContext ctx, BytesXMLMessage message) throws SolaceFault {
        BodyInstaller.install(ctx, PayloadCodec.decode(message, "poll"), "poll");
    }

    private static void assertPayloadFault(Install install) throws Exception {
        try {
            install.run();
            fail("expected PAYLOAD_ERROR");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_PAYLOAD_ERROR, fault.code);
            assertEquals(SolaceConstants.FAULT_PAYLOAD_ERROR, fault.name);
        }
    }

    private static String jsonText(MessageContext ctx) throws Exception {
        try (InputStream json = JsonUtil.getJsonPayload(ctx)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[256];
            int n;
            while ((n = json.read(buffer)) > 0) {
                out.write(buffer, 0, n);
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static byte[] binaryBytes(MessageContext ctx) throws Exception {
        OMElement first = ctx.getEnvelope().getBody().getFirstElement();
        assertEquals(BaseConstants.DEFAULT_BINARY_WRAPPER, first.getQName());
        OMText content = (OMText) first.getFirstOMChild();
        assertTrue(content.isBinary());
        try (InputStream in = ((DataHandler) content.getDataHandler()).getInputStream()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[256];
            int n;
            while ((n = in.read(buffer)) > 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        }
    }

    /** A mediation context with the builders and formatters a pack registers. */
    static MessageContext context() throws Exception {
        AxisConfiguration config = new AxisConfiguration();
        config.addMessageBuilder("application/json", new JsonStreamBuilder());
        config.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        config.addMessageBuilder("text/plain", new PlainTextBuilder());
        config.addMessageBuilder("application/octet-stream", new BinaryBuilder());
        config.addMessageBuilder("application/json/badgerfish", new JSONBadgerfishOMBuilder());
        config.addMessageBuilder("text/javascript", new JSONBuilder());
        config.addMessageBuilder("application/x-www-form-urlencoded", new XFormURLEncodedBuilder());
        config.addMessageBuilder("multipart/form-data", new MultipartFormDataBuilder());
        config.addMessageBuilder("application/x-custom-text", new PlainTextBuilder());
        config.addMessageFormatter("application/json", new JsonStreamFormatter());
        config.addMessageFormatter("application/xml", new ApplicationXMLFormatter());
        config.addMessageFormatter("text/plain", new PlainTextFormatter());
        config.addMessageFormatter("application/octet-stream", new BinaryFormatter());
        config.addMessageFormatter("application/json/badgerfish", new JSONBadgerfishMessageFormatter());
        config.addMessageFormatter("text/javascript", new JSONMessageFormatter());
        MessageContext ctx = new MessageContext();
        ctx.setConfigurationContext(new ConfigurationContext(config));
        ctx.setOperationContext(new OperationContext());
        ctx.setEnvelope(OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope());
        ctx.setDoingREST(true);
        return ctx;
    }
}
