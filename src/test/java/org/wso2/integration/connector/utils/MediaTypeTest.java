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
import org.wso2.integration.connector.constants.SolaceConstants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * The media type parameters follow the quoted string rules of HTTP.
 */
public class MediaTypeTest {

    @Test
    public void aSemicolonInsideQuotesIsNotASeparator() {
        MediaType type = MediaType.parse("Text/Plain; foo=\"a;charset=x\"; Charset=ISO-8859-1");
        assertEquals("text/plain", type.baseType);
        assertEquals("ISO-8859-1", type.charset());
        assertEquals("text/plain; foo=\"a;charset=x\"", type.withoutCharset());
        assertEquals("text/plain; foo=\"a;charset=x\"; charset=UTF-8", type.withCharset("UTF-8"));

        assertNull(MediaType.parse("text/plain; foo=\"x;charset=bad\"").charset());
        assertNull(PayloadCodec.charsetOf("text/plain; foo=\"x; charset=bad\""));
    }

    @Test
    public void anEscapedQuoteDoesNotEndTheValue() {
        MediaType type = MediaType.parse("text/plain; foo=\"a\\\";charset=x\"; charset=\"utf\\-8\"");
        assertEquals("utf-8", type.charset());
        assertEquals("text/plain; foo=\"a\\\";charset=x\"", type.withoutCharset());
    }

    @Test
    public void theCharsetIsFoundCaseInsensitivelyAndUnquoted() {
        assertEquals("UTF-16", PayloadCodec.charsetOf("application/json;CHARSET=\"UTF-16\""));
        assertEquals("", MediaType.parse("text/plain; charset=\"\"").charsetParameter());
        assertNull(MediaType.parse("text/plain; charset=\"\"").charset());
        assertNull(MediaType.parse("text/plain").charsetParameter());
        assertNull(MediaType.parse("  "));
        assertEquals("application/xml", PayloadCodec.mediaTypeOf("application/xml ; x=\"a;b\""));
    }

    @Test
    public void aTypeNeverCarriesTwoCharsets() {
        assertEquals("text/plain; v=1; charset=UTF-8",
                MediaType.parse("text/plain; charset=ISO-8859-1; v=1; charset=UTF-16").withCharset("UTF-8"));
    }

    @Test
    public void theOverrideUsesTheSameParser() throws Exception {
        assertEquals("text/plain; foo=\"a;charset=x\"",
                OperationParameters.parseContentType("contentType", "text/plain; foo=\"a;charset=x\""));
        try {
            OperationParameters.parseContentType("contentType", "text/plain; foo=\"a;b\"; charset=nope");
            fail("expected INVALID_CONFIG");
        } catch (SolaceFault fault) {
            assertEquals(SolaceConstants.ERROR_CODE_INVALID_CONFIG, fault.code);
            assertEquals("contentType names the unknown charset 'nope', got "
                    + "'text/plain; foo=\"a;b\"; charset=nope'", fault.getMessage());
        }

        // The inherited charset is the declared one, a charset inside a quoted value is not inherited.
        assertEquals("application/json; charset=ISO-8859-1", PayloadCodec.resolveContentType("application/json",
                "text/plain; foo=\"x;charset=UTF-16\"; charset=ISO-8859-1"));
        assertEquals("application/json",
                PayloadCodec.resolveContentType("application/json", "text/plain; foo=\"x;charset=UTF-16\""));
    }
}
