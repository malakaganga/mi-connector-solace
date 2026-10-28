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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.OMText;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.BinaryFormatter;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseConstants;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.http.MultipartFormDataFormatter;
import org.apache.axis2.transport.http.SOAPMessageFormatter;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.formatters.XFormURLEncodedFormatter;
import org.apache.synapse.commons.json.JsonFormatter;
import org.apache.synapse.commons.json.JsonStreamFormatter;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.Pipe;
import org.apache.synapse.transport.passthru.util.RelayUtils;
import org.wso2.integration.connector.SolaceFault;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.activation.DataHandler;
import javax.xml.stream.XMLStreamException;

import static org.apache.synapse.commons.json.Constants.JSON_STRING;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PAYLOAD_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PAYLOAD_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.PAYLOAD_ENCODING_BASE64;
import static org.wso2.integration.connector.constants.SolaceConstants.RESULT_CONTENT_TYPE;
import static org.wso2.integration.connector.constants.SolaceConstants.RESULT_PAYLOAD_ENCODING;

/**
 * The payload contract of the connector.
 *
 * <p>The send side reads the outgoing body the way the HTTP transport does: the platform's message formatter
 * writes the bytes and names the content type. The read side decodes a received message by its JCSMP class and
 * its content type, the operation's override, else the declared type, else the default of the class, never by
 * the first character of the body, and writes it into the response variable or into the message body. The same
 * codec serves publishMessage, sendRequest, sendReply, poll, browse and requestCachedMessages.</p>
 */
public final class PayloadCodec {

    public static final String CONTENT_TYPE_JSON = "application/json";
    public static final String CONTENT_TYPE_XML = "application/xml";
    public static final String CONTENT_TYPE_TEXT = "text/plain";
    public static final String CONTENT_TYPE_OCTET_STREAM = "application/octet-stream";
    public static final String CONTENT_TYPE_JSON_BADGERFISH = "application/json/badgerfish";

    private static final Log log = LogFactory.getLog(PayloadCodec.class);

    private static final String CONTENT_TYPE_TEXT_XML = "text/xml";
    private static final String JSON_SUFFIX = "+json";
    private static final String XML_SUFFIX = "+xml";
    private static final String MULTIPART_PREFIX = "multipart/";
    private static final String CHARSET_PARAMETER = "charset";
    // The operation context's cache of the serialised body, read before the body by some formatters.
    private static final String MESSAGE_AS_BYTES_PROPERTY = "MESSAGE_AS_BYTES";
    /** The fault for a request whose body was never built, with its cure. */
    private static final String UNTYPED_BODY_NOT_BUILT = "the request has no Content-Type and its body was not "
            + "built, declare a Content-Type on the client or set ContentType before the first content aware mediator";
    // A formatter of the pack that is not on the class path of the connector is named.
    // The platform carries two copies of gson, and a reader of one given to a parser of the other fails with a
    // loader constraint error. So JSON is parsed through a Gson instance and never with the stream package.
    private static final Gson STRICT_JSON = new GsonBuilder().setStrictness(Strictness.STRICT).create();
    private static final String MI_JSON_STREAM_FORMATTER = "org.wso2.micro.integrator.core.json.JsonStreamFormatter";
    /** The stock formatters that write characters. Their subclasses write characters too. */
    private static final Set<String> CHARACTER_FORMATTERS = new HashSet<>(Arrays.asList(
            PlainTextFormatter.class.getName(),
            ApplicationXMLFormatter.class.getName(),
            JsonStreamFormatter.class.getName(),
            JsonFormatter.class.getName(),
            MI_JSON_STREAM_FORMATTER,
            BodyInstaller.LEGACY_JSON_FORMATTER));
    /** The stock URL encoded form formatters, the one of synapse and the one of axis2. */
    private static final Set<String> FORM_FORMATTERS = new HashSet<>(Arrays.asList(
            XFormURLEncodedFormatter.class.getName(),
            org.apache.axis2.transport.http.XFormURLEncodedFormatter.class.getName()));

    /** The representation a received body is written with. */
    public enum Family { JSON, XML, TEXT, BINARY }

    private PayloadCodec() {
    }

    /**
     * The outgoing body as the platform formatter wrote it, in the charset of the wire. The bytes are what the
     * HTTP transport would have sent for the same context.
     */
    public static final class Outgoing {

        public final byte[] bytes;
        /** The base media type, lower case. */
        public final String mediaType;
        /** The charset the content type names, null when it names none. */
        public final String charset;
        /** The charset the bytes are encoded in. */
        public final String encoding;
        /** True when a binary formatter wrote the body or the body is the platform's binary wrapper. */
        public final boolean binary;
        private final String contentType;

        Outgoing(byte[] bytes, String contentType, String encoding, boolean binary) {
            MediaType type = MediaType.parse(contentType);
            this.bytes = bytes;
            this.contentType = contentType;
            this.mediaType = type.baseType;
            this.charset = type.charset();
            this.encoding = encoding;
            this.binary = binary;
        }

        /** The complete content type for the Solace message, with one charset parameter at most. */
        public String contentType() {
            return contentType;
        }

        public int length() {
            return bytes.length;
        }
    }

    /**
     * A received body, decoded once. Text carries its characters, binary carries its bytes, and a JSON family
     * body also carries its parsed value.
     */
    public static final class Received {

        public final Family family;
        /**
         * The content type the body was read with: the override with the inherited charset, else the declared
         * type as received, else the default of the message class.
         */
        public final String contentType;
        /** The base media type, lower case. */
        public final String mediaType;
        /** The charset of that content type, null when it names none. */
        public final String charset;
        /**
         * The characters of a text body. A TextMessage or XMLContentMessage read with a type that is not text is
         * binary and keeps its characters here too. Null for a binary body of bytes.
         */
        public final String text;
        /** The bytes of a binary body, the UTF-8 encoding of the characters for a text message. Null for text. */
        public final byte[] bytes;
        /** The parsed value of a JSON family body, null otherwise. */
        public final JsonElement json;

        Received(Family family, String contentType, String mediaType, String charset, String text, byte[] bytes,
                 JsonElement json) {
            this.family = family;
            this.contentType = contentType;
            this.mediaType = mediaType;
            this.charset = charset;
            this.text = text;
            this.bytes = bytes;
            this.json = json;
        }

        public boolean isBinary() {
            return family == Family.BINARY;
        }

        /** The value for the response variable: the parsed JSON, a text as a JSON string, or base64 text. */
        public JsonElement responseValue() {
            if (json != null) {
                return json;
            }
            return new JsonPrimitive(summaryText());
        }

        /** The value of a browse or cache summary: the text, or base64 text for binary. */
        public String summaryText() {
            return isBinary() ? Base64.getEncoder().encodeToString(bytes) : text;
        }
    }

    // Send side

    /**
     * Reads the outgoing body of publishMessage, sendRequest and sendReply through the platform's message
     * formatter, the way the HTTP transport writes it. The body is written once per operation and preserved, so
     * the sequence can still use it after the send.
     *
     * <p>The transfer of the request is checked, the body is built the way the mediators build it, and a binary
     * wrapper that holds base64 text gets a binary node. Then the formatter registered for the message type writes
     * the bytes, and the transfer is checked again before anything is sent. A SOAP message type, or the stock SOAP
     * formatter the platform selects, is written by the application/xml formatter, so the first body element is
     * sent and not the envelope. A formatter that extends the SOAP formatter, such as the binary relay formatter of
     * the pack, writes a binary element itself. Any other body, an empty one as well, it would write as an envelope,
     * so there the application/xml formatter writes the bytes and the selected formatter still names the content
     * type. A message type without a
     * registered formatter is written by the formatter of its family: application/json for a JSON type,
     * application/xml for an XML type, text/plain for any other text type and application/octet-stream for
     * everything else. A context without a message type gets the formatter the platform selects.</p>
     *
     * <p>The text, XML and JSON formatters write UTF-8, and a reporting encoder converts the bytes when the
     * charset of the context is another one. Binary and multipart bytes are never converted. Any other formatter
     * writes with the charset of the context. The content type is the one the formatter reports, with one charset
     * parameter. When the formatter reports no media type, a binary body gets application/octet-stream and any
     * other body the type that selected the formatter.</p>
     */
    public static Outgoing readOutgoing(MessageContext synCtx, String operation) throws SolaceFault {
        org.apache.axis2.context.MessageContext axis2Mc = ((Axis2MessageContext) synCtx).getAxis2MessageContext();
        checkTransfer(axis2Mc, operation);
        buildBody(axis2Mc, operation);
        adaptBinaryWrapper(axis2Mc, operation);

        AxisConfiguration config = axis2Mc.getConfigurationContext().getAxisConfiguration();
        String messageType = MediaType.baseTypeOf(stringProperty(axis2Mc, Constants.Configuration.MESSAGE_TYPE));
        String formatterType = messageType;
        MessageFormatter formatter;
        if (messageType == null) {
            try {
                formatter = MessageProcessorSelector.getMessageFormatter(axis2Mc);
            } catch (AxisFault e) {
                throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                        operation + ": no message formatter for the message body: " + e.getMessage(), e);
            }
        } else {
            formatter = config.getMessageFormatter(messageType);
            if (formatter == null) {
                formatterType = BodyInstaller.familyType(messageType);
                formatter = registeredFormatter(config, formatterType);
            }
        }
        // A subclass of the SOAP formatter, such as the binary relay formatter, writes a binary element itself. It
        // writes any other body, an empty one as well, as a SOAP envelope, so there the XML formatter writes the
        // bytes, and the selected formatter still decides binary and names the content type.
        MessageFormatter selected = formatter;
        boolean soapFormatter = formatter instanceof SOAPMessageFormatter
                && (formatter.getClass() == SOAPMessageFormatter.class || !isBinaryWrapper(firstElement(axis2Mc)));
        if (soapFormatter
                || HTTPConstants.MEDIA_TYPE_TEXT_XML.equals(messageType)
                || HTTPConstants.MEDIA_TYPE_APPLICATION_SOAP_XML.equals(messageType)) {
            formatterType = CONTENT_TYPE_XML;
            formatter = registeredFormatter(config, formatterType);
        }

        MessageFormatter binaryFormatter = config.getMessageFormatter(CONTENT_TYPE_OCTET_STREAM);
        boolean binary = formatter instanceof BinaryFormatter
                || (binaryFormatter != null && binaryFormatter.getClass() == selected.getClass())
                || isBinaryWrapper(firstElement(axis2Mc));
        boolean multipart = formatter instanceof MultipartFormDataFormatter
                || (formatterType != null && formatterType.startsWith(MULTIPART_PREFIX));
        boolean transcoded = !binary && !multipart && isOneOf(formatter, CHARACTER_FORMATTERS);
        boolean form = !binary && !multipart && isOneOf(formatter, FORM_FORMATTERS);
        if (log.isDebugEnabled()) {
            log.debug(operation + ": the message body is written by " + formatter.getClass().getName()
                    + " for the message type " + messageType + ", formatter type " + formatterType);
        }

        // A cache of a previous formatter run must not win over the body the sequence left.
        axis2Mc.removeProperty(JSON_STRING);
        if (axis2Mc.getOperationContext() != null) {
            axis2Mc.getOperationContext().removeProperty(MESSAGE_AS_BYTES_PROPERTY);
        }

        String wireCharset = stringProperty(axis2Mc, Constants.Configuration.CHARACTER_SET_ENCODING);
        if (StringUtils.isEmpty(wireCharset)) {
            wireCharset = org.apache.axis2.context.MessageContext.DEFAULT_CHAR_SET_ENCODING;
        }
        boolean convert = transcoded && !StandardCharsets.UTF_8.equals(charsetFor(wireCharset, operation));
        OMOutputFormat format = new OMOutputFormat();
        format.setSOAP11(axis2Mc.isSOAP11());
        format.setCharSetEncoding(transcoded ? StandardCharsets.UTF_8.name() : wireCharset);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String reported = write(axis2Mc, formatter, format, out, wireCharset, transcoded, operation);
        if (binary && selected != formatter) {
            reported = selected.getContentType(axis2Mc, format, axis2Mc.getSoapAction());
        }
        byte[] bytes = out.toByteArray();
        if (convert) {
            bytes = encode(decode(bytes, StandardCharsets.UTF_8.name(), operation), wireCharset, operation);
        }
        String fallback = binary ? CONTENT_TYPE_OCTET_STREAM
                : formatterType != null ? formatterType : CONTENT_TYPE_TEXT;
        String contentType = wireContentType(reported, fallback, transcoded || form ? wireCharset : null);

        checkTransfer(axis2Mc, operation);
        return new Outgoing(bytes, contentType, wireCharset, binary);
    }

    /**
     * Runs the formatter with preserve true and returns the content type it reports with the same output format.
     * The stock XML formatter writes its declaration with the charset of the wire: the declaration encoding names
     * it for the call and gets its previous value back after it, on a failure as well.
     * A character serialisation is written as UTF-8 and converted after the call. The JSON stream formatter reads
     * the charset of the context and not the one of the output format, so the context says UTF-8 for the call and
     * gets the charset of the sequence back after it, on a failure as well.
     */
    private static String write(org.apache.axis2.context.MessageContext axis2Mc, MessageFormatter formatter,
                                OMOutputFormat format, ByteArrayOutputStream out, String wireCharset,
                                boolean transcoded, String operation) throws SolaceFault {
        boolean xmlFormatter = formatter instanceof ApplicationXMLFormatter;
        Object previousDeclaration = axis2Mc.getLocalProperty(Constants.Configuration.XML_DECLARATION_ENCODING);
        Object previousCharset = axis2Mc.getLocalProperty(Constants.Configuration.CHARACTER_SET_ENCODING);
        try {
            if (xmlFormatter) {
                axis2Mc.setProperty(Constants.Configuration.XML_DECLARATION_ENCODING, wireCharset);
            }
            if (transcoded) {
                axis2Mc.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, StandardCharsets.UTF_8.name());
            }
            formatter.writeTo(axis2Mc, format, out, true);
            return formatter.getContentType(axis2Mc, format, axis2Mc.getSoapAction());
        } catch (AxisFault | RuntimeException e) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": cannot read the message body through the " + formatter.getClass().getSimpleName()
                            + ": " + e.getMessage(), e);
        } finally {
            if (xmlFormatter) {
                if (previousDeclaration == null) {
                    axis2Mc.removeProperty(Constants.Configuration.XML_DECLARATION_ENCODING);
                } else {
                    axis2Mc.setProperty(Constants.Configuration.XML_DECLARATION_ENCODING, previousDeclaration);
                }
            }
            if (transcoded) {
                if (previousCharset == null) {
                    axis2Mc.removeProperty(Constants.Configuration.CHARACTER_SET_ENCODING);
                } else {
                    axis2Mc.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, previousCharset);
                }
            }
        }
    }

    /**
     * The content type of the Solace message: the type the formatter reported, else the fallback, with one
     * charset parameter. A character formatter gets the charset the bytes were written in, also when it left the
     * charset out under the suppression flag. Any other type keeps its own charset, if it names one.
     */
    private static String wireContentType(String reported, String fallback, String writtenCharset) {
        String described = StringUtils.isBlank(reported) ? fallback : reported.trim();
        MediaType type = MediaType.parse(described);
        if (type == null) {
            // A formatter that reads an empty content type reports only its charset parameter.
            described = fallback;
            type = MediaType.parse(described);
        }
        if (writtenCharset != null) {
            return type.withCharset(writtenCharset);
        }
        String charset = type.charset();
        return charset == null ? described : type.withCharset(charset);
    }

    /**
     * Faults when the HTTP request under this context failed in transfer. A body the connector installed from a
     * Solace message is exempt, its bytes did not come from the request. The pipe is only read for its state.
     */
    private static void checkTransfer(org.apache.axis2.context.MessageContext axis2Mc, String operation)
            throws SolaceFault {
        Object pipe = axis2Mc.getProperty(PassThroughConstants.PASS_THROUGH_PIPE);
        if (pipe instanceof Pipe && ((Pipe) pipe).isProducerError() && !BodyInstaller.isInstalledBody(axis2Mc)) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": the HTTP request body was not received completely, nothing is sent");
        }
    }

    /**
     * Builds the body the way the mediators do. An untouched request without a content type is opaque bytes:
     * it gets application/octet-stream first, so the platform's binary builder wraps the entity. A context that
     * is already marked built, with no content type and no body, faults, because its entity was never read.
     */
    private static void buildBody(org.apache.axis2.context.MessageContext axis2Mc, String operation)
            throws SolaceFault {
        Object pipe = axis2Mc.getProperty(PassThroughConstants.PASS_THROUGH_PIPE);
        if (axis2Mc.getProperty(Constants.Configuration.CONTENT_TYPE) == null && firstElement(axis2Mc) == null) {
            if (Boolean.TRUE.equals(axis2Mc.getProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED))) {
                throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                        operation + ": " + UNTYPED_BODY_NOT_BUILT);
            }
            if (pipe != null) {
                axis2Mc.setProperty(Constants.Configuration.CONTENT_TYPE, CONTENT_TYPE_OCTET_STREAM);
                axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, CONTENT_TYPE_OCTET_STREAM);
            }
        }
        try {
            RelayUtils.buildMessage(axis2Mc);
        } catch (IOException | XMLStreamException | OMException e) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": cannot build the message body: " + e.getMessage(), e);
        }
        // With the passthrough builder disabled nothing builds the entity, and an empty body is never published.
        if (pipe != null && !Boolean.TRUE.equals(axis2Mc.getProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED))
                && firstElement(axis2Mc) == null) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": the message body was not built");
        }
    }

    /**
     * Gives the platform's binary wrapper a binary node when it holds base64 text, so the binary formatters can
     * write it. The text is decoded by a strict decoder and invalid base64 faults. No other shape is converted.
     */
    private static void adaptBinaryWrapper(org.apache.axis2.context.MessageContext axis2Mc, String operation)
            throws SolaceFault {
        OMElement wrapper = firstElement(axis2Mc);
        if (!isBinaryWrapper(wrapper)) {
            return;
        }
        for (Iterator<?> children = wrapper.getChildren(); children.hasNext();) {
            Object child = children.next();
            if (child instanceof OMText && ((OMText) child).isBinary()) {
                return;
            }
        }
        byte[] data;
        try {
            data = Base64.getDecoder().decode(wrapper.getText());
        } catch (IllegalArgumentException e) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": the binary wrapper of the message body does not hold valid base64 text", e);
        }
        String contentType = stringProperty(axis2Mc, Constants.Configuration.CONTENT_TYPE);
        if (StringUtils.isBlank(contentType)) {
            contentType = CONTENT_TYPE_OCTET_STREAM;
        }
        for (Iterator<?> children = wrapper.getChildren(); children.hasNext();) {
            children.next();
            children.remove();
        }
        OMFactory factory = wrapper.getOMFactory();
        wrapper.addChild(factory.createOMText(new DataHandler(new ByteArrayDataSource(data, contentType)), true));
    }

    private static OMElement firstElement(org.apache.axis2.context.MessageContext axis2Mc) {
        SOAPEnvelope envelope = axis2Mc.getEnvelope();
        if (envelope == null || envelope.getBody() == null) {
            return null;
        }
        return envelope.getBody().getFirstElement();
    }

    private static boolean isBinaryWrapper(OMElement element) {
        return element != null && BaseConstants.DEFAULT_BINARY_WRAPPER.equals(element.getQName());
    }

    /** True when the class of the handler or one of its superclasses is named in the set. */
    private static boolean isOneOf(Object handler, Set<String> classNames) {
        for (Class<?> c = handler.getClass(); c != null; c = c.getSuperclass()) {
            if (classNames.contains(c.getName())) {
                return true;
            }
        }
        return false;
    }

    /** The formatter the configuration registered for the type, or the platform's own class when none is. */
    private static MessageFormatter registeredFormatter(AxisConfiguration config, String type) {
        MessageFormatter formatter = config.getMessageFormatter(type);
        if (formatter != null) {
            return formatter;
        }
        switch (type) {
            case CONTENT_TYPE_JSON:
                return new JsonStreamFormatter();
            case CONTENT_TYPE_TEXT:
                return new PlainTextFormatter();
            case CONTENT_TYPE_OCTET_STREAM:
                return new BinaryFormatter();
            default:
                return new ApplicationXMLFormatter();
        }
    }

    private static String stringProperty(org.apache.axis2.context.MessageContext axis2Mc, String name) {
        Object value = axis2Mc.getProperty(name);
        return value instanceof String ? (String) value : null;
    }

    /**
     * Encodes text with a charset, reporting a character the charset cannot represent as a payload fault.
     * The default is UTF-8.
     */
    public static byte[] encode(String text, String charsetName, String operation) throws SolaceFault {
        Charset charset = charsetFor(charsetName, operation);
        try {
            ByteBuffer buffer = charset.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(text));
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": the text payload has a character that " + charset.name() + " cannot encode", e);
        }
    }

    /**
     * Decodes bytes with a charset, reporting malformed and unmappable input as a payload fault. No replacement
     * character is ever introduced. A replacement character the producer sent is valid text and stays. The
     * default is UTF-8.
     */
    public static String decode(byte[] bytes, String charsetName, String operation) throws SolaceFault {
        Charset charset = charsetFor(charsetName, operation);
        try {
            return charset.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": the body is not valid " + charset.name() + " text", e);
        }
    }

    private static Charset charsetFor(String charsetName, String operation) throws SolaceFault {
        if (StringUtils.isEmpty(charsetName)) {
            return StandardCharsets.UTF_8;
        }
        try {
            return Charset.forName(charsetName);
        } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": unknown charset '" + charsetName + "' in the content type", e);
        }
    }

    // Read side

    /** Decodes a received message without a reader override. */
    public static Received decode(BytesXMLMessage message, String operation) throws SolaceFault {
        return decode(message, null, operation);
    }

    /**
     * Decodes a received message. The content type it is read with is the override, else the type declared on
     * the message, else the default of the message class: text/plain for a TextMessage, application/xml for an
     * XMLContentMessage, application/octet-stream for a BytesMessage. The payload is never inspected to choose
     * a type. TextMessage and XMLContentMessage are text, and read with a type that is not text they are binary
     * whose bytes are the UTF-8 encoding of their text. A BytesMessage read as a text type is text decoded with
     * the charset of the content type, UTF-8 when none is named. Any other BytesMessage is binary. A JSON family
     * body is parsed, and one that does not parse faults. A charset Java does not know faults. MapMessage and
     * StreamMessage are converted to JSON and declared application/json, the override does not apply to them.
     *
     * @param override the contentType parameter of the operation, already checked, null when absent
     */
    public static Received decode(BytesXMLMessage message, String override, String operation) throws SolaceFault {
        if (message instanceof MapMessage || message instanceof StreamMessage) {
            JsonElement json;
            try {
                json = message instanceof MapMessage
                        ? sdtMapToJson(((MapMessage) message).getMap())
                        : sdtStreamToJson(((StreamMessage) message).getStream());
            } catch (SDTException e) {
                throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                        operation + ": cannot read the structured body of the message: " + e.getMessage(), e);
            }
            return new Received(Family.JSON, CONTENT_TYPE_JSON, CONTENT_TYPE_JSON, null, json.toString(), null, json);
        }

        // The content type is resolved once: base media type and charset, case insensitive, parameters kept.
        String resolved = resolveContentType(override,
                StringUtils.trimToNull(message.getHTTPContentType()));
        String mediaType = mediaTypeOf(resolved);
        String charset = charsetOf(resolved);
        if (charset != null) {
            charsetFor(charset, operation);
        }

        if (message instanceof TextMessage || message instanceof XMLContentMessage) {
            String text = message instanceof TextMessage
                    ? ((TextMessage) message).getText()
                    : ((XMLContentMessage) message).getXMLContent();
            if (text == null) {
                text = "";
            }
            if (mediaType == null) {
                // The class default. A TextMessage is text/plain whatever its body.
                return message instanceof XMLContentMessage
                        ? new Received(Family.XML, CONTENT_TYPE_XML, CONTENT_TYPE_XML, null, text, null, null)
                        : new Received(Family.TEXT, CONTENT_TYPE_TEXT, CONTENT_TYPE_TEXT, null, text, null, null);
            }
            if (!isTextType(mediaType)) {
                // The resolved type decides. The bytes of the body are the UTF-8 encoding of its text.
                return new Received(Family.BINARY, resolved, mediaType, charset, text,
                        text.getBytes(StandardCharsets.UTF_8), null);
            }
            return text(resolved, mediaType, charset, text, operation);
        }

        byte[] bytes;
        if (message instanceof BytesMessage) {
            bytes = ((BytesMessage) message).getData();
        } else if (message.getAttachmentByteBuffer() != null) {
            ByteBuffer buffer = message.getAttachmentByteBuffer();
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        } else {
            bytes = message.getBytes();
        }
        if (bytes == null) {
            bytes = new byte[0];
        }
        if (mediaType != null && isTextType(mediaType)) {
            return text(resolved, mediaType, charset, decode(bytes, charset, operation), operation);
        }
        String contentType = resolved != null ? resolved : CONTENT_TYPE_OCTET_STREAM;
        return new Received(Family.BINARY, contentType, mediaType != null ? mediaType : CONTENT_TYPE_OCTET_STREAM,
                null, null, bytes, null);
    }

    /**
     * The content type a received message is read with, and the one attributes.contentType reports. Without an
     * override it is the declared type as received. An override replaces the declared base type and every
     * declared parameter. When the override names no charset, the declared charset is added to it. No other
     * declared parameter, a boundary for example, is kept. Null when neither is set, the class default applies.
     */
    public static String resolveContentType(String override, String declared) {
        if (override == null) {
            return declared;
        }
        if (charsetOf(override) != null) {
            return override;
        }
        String inherited = charsetOf(declared);
        return inherited == null ? override : override + "; " + CHARSET_PARAMETER + "=" + inherited;
    }

    private static Received text(String contentType, String mediaType, String charset, String text, String operation)
            throws SolaceFault {
        if (isJsonFamily(mediaType)) {
            JsonElement json = parseJson(text);
            if (json == null) {
                throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                        operation + ": the body read as " + contentType + " is not valid JSON");
            }
            return new Received(Family.JSON, contentType, mediaType, charset, text, null, json);
        }
        Family family = isXmlFamily(mediaType) ? Family.XML : Family.TEXT;
        return new Received(family, contentType, mediaType, charset, text, null, null);
    }

    /**
     * Prepares the response of a read operation and returns the value to store through the response API with
     * overwrite false. The content type goes on the attributes, and for binary the payload encoding base64. With
     * overwriteBody the builder of its content type builds the received payload into the message body, and the
     * response variable carries no payload, as the framework does for every connector. Without it the payload is the
     * response value: the parsed JSON, a text as a JSON string or base64 text. The framework's own JSON overwrite
     * is never asked for after the body was prepared here, it would replace the native body with JSON.
     */
    public static JsonElement prepareResponse(MessageContext synCtx, Received received, Boolean overwriteBody,
                                              Map<String, Object> attributes, String operation) throws SolaceFault {
        attributes.put(RESULT_CONTENT_TYPE, received.contentType);
        if (received.isBinary()) {
            attributes.put(RESULT_PAYLOAD_ENCODING, PAYLOAD_ENCODING_BASE64);
        }
        if (Boolean.TRUE.equals(overwriteBody)) {
            BodyInstaller.install(((Axis2MessageContext) synCtx).getAxis2MessageContext(), received, operation);
            return JsonNull.INSTANCE;
        }
        return received.responseValue();
    }

    // JSON and SDT conversion

    /**
     * Parses one complete JSON document, any JSON value, strictly. Returns null when the text is not one, so an
     * unquoted word, a trailing character or an empty text is never taken for JSON.
     */
    public static JsonElement parseJson(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        try {
            // The instance reads one value and faults when the document is not fully consumed.
            return STRICT_JSON.fromJson(text, JsonElement.class);
        } catch (Exception e) {
            return null;
        }
    }

    /** Converts an SDTMap to a JSON object. Nested containers are converted inside, bytes become base64 text. */
    public static JsonObject sdtMapToJson(SDTMap map) throws SDTException {
        JsonObject json = new JsonObject();
        if (map == null) {
            return json;
        }
        for (String key : map.keySet()) {
            json.add(key, sdtValueToJson(map.get(key)));
        }
        return json;
    }

    /** Converts an SDTStream to a JSON array. Nested containers are converted inside, bytes become base64 text. */
    public static JsonArray sdtStreamToJson(SDTStream stream) throws SDTException {
        JsonArray json = new JsonArray();
        if (stream == null) {
            return json;
        }
        while (stream.hasRemaining()) {
            json.add(sdtValueToJson(stream.read()));
        }
        return json;
    }

    private static JsonElement sdtValueToJson(Object value) throws SDTException {
        if (value == null) {
            return JsonNull.INSTANCE;
        }
        if (value instanceof Boolean) {
            return new JsonPrimitive((Boolean) value);
        }
        if (value instanceof Number) {
            return new JsonPrimitive((Number) value);
        }
        if (value instanceof SDTMap) {
            return sdtMapToJson((SDTMap) value);
        }
        if (value instanceof SDTStream) {
            return sdtStreamToJson((SDTStream) value);
        }
        return new JsonPrimitive(sdtScalarToText(value));
    }

    /**
     * The text form of a scalar SDT value: bytes as base64, a destination by its name, anything else by its
     * string form. Containers are not scalars and must not reach here.
     */
    public static String sdtScalarToText(Object value) {
        if (value instanceof ByteArray) {
            return Base64.getEncoder().encodeToString(((ByteArray) value).asBytes());
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        if (value instanceof Destination) {
            return ((Destination) value).getName();
        }
        return String.valueOf(value);
    }

    // Content type helpers

    /** The base media type of a content type, lower case and trimmed. Null for an empty content type. */
    public static String mediaTypeOf(String contentType) {
        return MediaType.baseTypeOf(contentType);
    }

    /** The charset parameter of a content type, case insensitive, quotes removed. Null when none is named. */
    public static String charsetOf(String contentType) {
        MediaType type = MediaType.parse(contentType);
        return type == null ? null : type.charset();
    }

    public static boolean isJsonFamily(String mediaType) {
        return CONTENT_TYPE_JSON.equals(mediaType) || mediaType.endsWith(JSON_SUFFIX);
    }

    public static boolean isXmlFamily(String mediaType) {
        return CONTENT_TYPE_XML.equals(mediaType) || CONTENT_TYPE_TEXT_XML.equals(mediaType)
                || mediaType.endsWith(XML_SUFFIX);
    }

    /** A text type is text/*, the JSON family, the XML family or the Badgerfish JSON type. */
    public static boolean isTextType(String mediaType) {
        return mediaType.startsWith("text/") || isJsonFamily(mediaType) || isXmlFamily(mediaType)
                || CONTENT_TYPE_JSON_BADGERFISH.equals(mediaType);
    }
}
