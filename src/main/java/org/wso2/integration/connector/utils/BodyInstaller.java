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

import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.context.OperationContext;
import org.apache.axis2.format.DataSourceMessageBuilder;
import org.apache.axis2.format.PlainTextBuilder;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.json.JsonBuilder;
import org.apache.synapse.commons.json.JsonStreamBuilder;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.transport.nhttp.NhttpConstants;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.util.DeferredMessageBuilder;
import org.wso2.integration.connector.SolaceFault;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.synapse.commons.json.Constants.JSON_STRING;
import static org.wso2.integration.connector.constants.SolaceConstants.ERROR_CODE_PAYLOAD_ERROR;
import static org.wso2.integration.connector.constants.SolaceConstants.FAULT_PAYLOAD_ERROR;

/**
 * Installs a received Solace payload as the message body, with overwriteBody on poll and on the reply of
 * sendRequest. The body is built by the builder the axis2 configuration registers for the content type, the way
 * the transports and the file inbound build a body, and the context is then left the way the platform's own
 * mediators leave it. The installed body is recorded on the context, so a later send can tell its own body from
 * a body that came from somewhere else.
 */
public final class BodyInstaller {

    private static final Log log = LogFactory.getLog(BodyInstaller.class);

    /** The context property that records the envelope and the first body element the connector installed. */
    private static final String INSTALLED_BODY = "SOLACE_CONNECTOR_INSTALLED_BODY";
    /** The operation context's cache of the serialised body. */
    private static final String MESSAGE_AS_BYTES = "MESSAGE_AS_BYTES";

    // Builders and formatters of the pack that are not on the class path of the connector are named.
    private static final String LEGACY_JSON_BUILDER = "org.apache.axis2.json.AbstractJSONOMBuilder";
    static final String LEGACY_JSON_FORMATTER = "org.apache.axis2.json.AbstractJSONMessageFormatter";
    /** The stock builders that read characters. Their subclasses read characters too. */
    private static final Set<String> CHARACTER_BUILDERS = new HashSet<>(Arrays.asList(
            PlainTextBuilder.class.getName(),
            ApplicationXMLBuilder.class.getName(),
            JsonStreamBuilder.class.getName(),
            JsonBuilder.class.getName(),
            "org.wso2.micro.integrator.core.json.JsonStreamBuilder",
            "org.apache.axis2.json.JSONBuilder",
            LEGACY_JSON_BUILDER));

    /** The identity of an installed body. */
    private static final class InstalledBody {

        final SOAPEnvelope envelope;
        final OMElement firstElement;

        InstalledBody(SOAPEnvelope envelope, OMElement firstElement) {
            this.envelope = envelope;
            this.firstElement = firstElement;
        }
    }

    private BodyInstaller() {
    }

    /**
     * True only when the current envelope and its first body element are, by identity, the ones the connector
     * installed last on this context. Mediation that edits that body in place keeps the identity. A body
     * replaced by any later mediator does not match.
     */
    public static boolean isInstalledBody(MessageContext axis2Ctx) {
        Object value = axis2Ctx.getLocalProperty(INSTALLED_BODY);
        if (!(value instanceof InstalledBody)) {
            return false;
        }
        InstalledBody installed = (InstalledBody) value;
        SOAPEnvelope envelope = axis2Ctx.getEnvelope();
        return envelope != null && envelope == installed.envelope
                && envelope.getBody().getFirstElement() == installed.firstElement;
    }

    /**
     * Builds a received payload into the message body.
     *
     * <p>The builder is the one registered for the base type. When none is registered the family decides:
     * application/json for a JSON type, application/xml for an XML type, text/plain for any other text type and
     * application/octet-stream for everything else, so a missing builder never turns bytes into XML. Two types
     * do not follow the registration: text/xml and application/soap+xml are built by the application/xml
     * builder, and a form is built by the octet stream builder as opaque bytes, no field is parsed.</p>
     *
     * <p>Text is given to the builder as UTF-8 bytes with charset=UTF-8 in its declaration, whatever the type:
     * the characters of a text message, and the bytes of a BytesMessage decoded strictly when its type is a text
     * type or its builder is a stock character builder. Any other BytesMessage is given as its bytes with its
     * declared type unchanged.</p>
     *
     * <p>The state of the old body is cleared before the builder runs and the new body is described after it.
     * Then the new body is recorded as the connector's own.</p>
     */
    public static void install(MessageContext axis2Ctx, PayloadCodec.Received received, String operation)
            throws SolaceFault {
        String declared = received.contentType;
        MediaType type = MediaType.parse(declared);
        String baseType = type.baseType;

        // The selector sets the message type as a side effect. The context keeps its type until the build.
        String messageType;
        Builder builder;
        Object previousMessageType = axis2Ctx.getLocalProperty(Constants.Configuration.MESSAGE_TYPE);
        boolean form = HTTPConstants.MEDIA_TYPE_X_WWW_FORM.equals(baseType)
                || HTTPConstants.MEDIA_TYPE_MULTIPART_FORM_DATA.equals(baseType);
        try {
            if (form) {
                messageType = PayloadCodec.CONTENT_TYPE_OCTET_STREAM;
                builder = BuilderUtil.getBuilderFromSelector(messageType, axis2Ctx);
            } else if (HTTPConstants.MEDIA_TYPE_TEXT_XML.equals(baseType)
                    || HTTPConstants.MEDIA_TYPE_APPLICATION_SOAP_XML.equals(baseType)) {
                messageType = PayloadCodec.CONTENT_TYPE_XML;
                builder = BuilderUtil.getBuilderFromSelector(messageType, axis2Ctx);
            } else {
                messageType = baseType;
                builder = BuilderUtil.getBuilderFromSelector(messageType, axis2Ctx);
                if (builder == null) {
                    messageType = familyType(baseType);
                    builder = BuilderUtil.getBuilderFromSelector(messageType, axis2Ctx);
                }
            }
        } catch (AxisFault e) {
            throw buildFault(declared, e, operation);
        } finally {
            if (previousMessageType == null) {
                axis2Ctx.removeProperty(Constants.Configuration.MESSAGE_TYPE);
            } else {
                axis2Ctx.setProperty(Constants.Configuration.MESSAGE_TYPE, previousMessageType);
            }
        }
        if (builder == null) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR, operation
                    + ": no message builder is registered for " + messageType + ", the type used for the content type "
                    + declared);
        }

        // The bytes and the declaration agree. Text is canonical UTF-8, other bytes keep their declared type.
        // A form is the exception. Its charset says how the percent encoded octets are read, so relabelling it
        // would change the value. The text of a form is encoded with its own charset and the declaration stays.
        String text = received.text;
        if (text == null && !form && isCharacterBuilder(builder)) {
            text = PayloadCodec.decode(received.bytes, type.charset(), operation);
        }
        byte[] bytes;
        String declaration;
        String charset;
        if (form) {
            bytes = received.text != null ? PayloadCodec.encode(received.text, type.charset(), operation)
                    : received.bytes;
            charset = type.charset();
            declaration = declared;
            text = null;
        } else if (text != null) {
            bytes = PayloadCodec.encode(text, StandardCharsets.UTF_8.name(), operation);
            charset = StandardCharsets.UTF_8.name();
            declaration = type.withCharset(charset);
        } else {
            bytes = received.bytes;
            charset = type.charset();
            declaration = declared;
        }
        boolean canonical = text != null;

        // A legacy JSON builder does not stop on some incomplete input, so the text is checked once before it.
        if (isSubclassOf(builder, LEGACY_JSON_BUILDER) && received.json == null
                && PayloadCodec.parseJson(text) == null) {
            throw new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR,
                    operation + ": the body read as " + declared + " is not valid JSON");
        }

        clearBodyState(axis2Ctx, charset);

        OMElement document;
        SOAPEnvelope envelope;
        try {
            if (builder instanceof DataSourceMessageBuilder) {
                document = ((DataSourceMessageBuilder) builder).processDocument(
                        new ByteArrayDataSource(bytes, declaration), declaration, axis2Ctx);
            } else {
                document = builder.processDocument(new ByteArrayInputStream(bytes), declaration, axis2Ctx);
            }
            envelope = TransportUtils.createSOAPEnvelope(document);
            axis2Ctx.setEnvelope(envelope);
        } catch (AxisFault | RuntimeException e) {
            throw buildFault(declared, e, operation);
        }

        describeBody(axis2Ctx, messageType, type, declaration, canonical);
        axis2Ctx.setProperty(INSTALLED_BODY, new InstalledBody(envelope, envelope.getBody().getFirstElement()));
        if (log.isDebugEnabled()) {
            log.debug(operation + ": the body of content type " + declared + " is built by "
                    + builder.getClass().getName() + " with message type " + messageType);
        }
    }

    /**
     * Clears the state of the old body before the builder runs, so nothing of it is formatted, streamed or
     * measured for the new body. A builder that sets attachment state itself keeps what it sets.
     */
    private static void clearBodyState(MessageContext axis2Ctx, String charset) {
        JsonUtil.removeJsonPayload(axis2Ctx);
        axis2Ctx.removeProperty(JSON_STRING);
        OperationContext operationContext = axis2Ctx.getOperationContext();
        if (operationContext != null) {
            operationContext.removeProperty(MESSAGE_AS_BYTES);
        }
        axis2Ctx.removeProperty(Constants.Configuration.ENABLE_MTOM);
        axis2Ctx.removeProperty(Constants.Configuration.ENABLE_SWA);
        axis2Ctx.removeProperty(PassThroughConstants.MESSAGE_OUTPUT_FORMAT);

        // A forced content length is recalculated for the new body, never copied from the old request.
        if (axis2Ctx.isPropertyTrue(NhttpConstants.FORCE_HTTP_CONTENT_LENGTH)) {
            axis2Ctx.removeProperty(NhttpConstants.FORCE_HTTP_CONTENT_LENGTH);
            axis2Ctx.setProperty(PassThroughConstants.DISABLE_CHUNKING, Boolean.TRUE.toString());
        }
        axis2Ctx.removeProperty(PassThroughConstants.COPY_CONTENT_LENGTH_FROM_INCOMING);
        axis2Ctx.removeProperty(PassThroughConstants.ORGINAL_CONTEN_LENGTH);
        axis2Ctx.removeProperty(PassThroughConstants.PASSTROUGH_MESSAGE_LENGTH);

        // The buffered stream of the old entity and its relay formatters leave with it, as after a build failure.
        axis2Ctx.setProperty(PassThroughConstants.BUFFERED_INPUT_STREAM, null);
        axis2Ctx.setProperty(DeferredMessageBuilder.RELAY_FORMATTERS_MAP, null);

        // The processing instructions and the declared encoding of the old document.
        axis2Ctx.removeProperty(Constants.Configuration.XML_PROCESSING_INSTRUCTION_ELEMENTS);
        axis2Ctx.removeProperty(Constants.Configuration.XML_DECLARATION_ENCODING);

        // The charset of the new bytes, never the old body's.
        if (charset == null) {
            axis2Ctx.removeProperty(Constants.Configuration.CHARACTER_SET_ENCODING);
        } else {
            axis2Ctx.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charset);
        }
    }

    /**
     * Describes the new body after the build. The message type is the key of the formatter that will write it.
     * ContentType and the transport Content-Type header are the complete type, with its charset, and no platform
     * path appends a second charset. A formatter that appends the charset whatever the flag says gets ContentType
     * without its charset, the charset stays in the character set encoding property.
     */
    private static void describeBody(MessageContext axis2Ctx, String messageType, MediaType type,
                                     String declaration, boolean canonical) {
        MessageFormatter formatter = axis2Ctx.getConfigurationContext().getAxisConfiguration()
                .getMessageFormatter(messageType);
        String contentType = canonical && formatter != null && isSubclassOf(formatter, LEGACY_JSON_FORMATTER)
                ? type.withoutCharset() : declaration;
        axis2Ctx.setProperty(Constants.Configuration.MESSAGE_TYPE, messageType);
        axis2Ctx.setProperty(Constants.Configuration.CONTENT_TYPE, contentType);
        axis2Ctx.setProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING, Boolean.FALSE.toString());

        Object headers = axis2Ctx.getProperty(MessageContext.TRANSPORT_HEADERS);
        if (headers instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> transportHeaders = (Map<String, Object>) headers;
            transportHeaders.keySet().removeIf(name -> HTTPConstants.HEADER_CONTENT_TYPE.equalsIgnoreCase(name)
                    || HTTPConstants.HEADER_CONTENT_LENGTH.equalsIgnoreCase(name));
            transportHeaders.put(HTTPConstants.HEADER_CONTENT_TYPE, declaration);
        }
        axis2Ctx.removeProperty(PassThroughConstants.NO_ENTITY_BODY);
        axis2Ctx.setProperty(PassThroughConstants.MESSAGE_BUILDER_INVOKED, Boolean.TRUE);

        // The XML builder records the encoding the document declares. After a canonical build the bytes are
        // UTF-8 whatever the declaration says, so a declaration written later follows the format that writes it.
        if (canonical) {
            axis2Ctx.removeProperty(Constants.Configuration.XML_DECLARATION_ENCODING);
        }
    }

    /** The type whose builder serves a base type that has none registered. */
    static String familyType(String baseType) {
        if (PayloadCodec.isJsonFamily(baseType)) {
            return PayloadCodec.CONTENT_TYPE_JSON;
        }
        if (PayloadCodec.isXmlFamily(baseType)) {
            return PayloadCodec.CONTENT_TYPE_XML;
        }
        if (baseType.startsWith("text/")) {
            return PayloadCodec.CONTENT_TYPE_TEXT;
        }
        return PayloadCodec.CONTENT_TYPE_OCTET_STREAM;
    }

    private static boolean isCharacterBuilder(Builder builder) {
        for (Class<?> c = builder.getClass(); c != null; c = c.getSuperclass()) {
            if (CHARACTER_BUILDERS.contains(c.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSubclassOf(Object handler, String className) {
        for (Class<?> c = handler.getClass(); c != null; c = c.getSuperclass()) {
            if (className.equals(c.getName())) {
                return true;
            }
        }
        return false;
    }

    private static SolaceFault buildFault(String declared, Exception e, String operation) {
        return new SolaceFault(FAULT_PAYLOAD_ERROR, ERROR_CODE_PAYLOAD_ERROR, operation
                + ": cannot set the received body declared " + declared + " as the message body: " + e.getMessage(),
                e);
    }
}
