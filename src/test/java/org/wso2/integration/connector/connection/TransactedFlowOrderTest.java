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

import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import org.junit.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

/**
 * A connector owned transaction closes its transacted flows before it commits or rolls back. A flow still open while
 * the transaction completes takes the next message into its window, or the rolled back message again, and its close
 * then returns that message with one more delivery counted on the broker.
 */
public class TransactedFlowOrderTest {

    @Test
    public void theFlowsCloseBeforeTheCommit() throws Exception {
        List<String> calls = new ArrayList<>();
        connectionWith(calls).commitTransaction();
        assertEquals(Arrays.asList("flow.close", "session.commit", "producer.close", "session.close"), calls);
    }

    @Test
    public void theFlowsCloseBeforeTheRollback() throws Exception {
        List<String> calls = new ArrayList<>();
        connectionWith(calls).rollbackTransaction();
        assertEquals(Arrays.asList("flow.close", "session.rollback", "producer.close", "session.close"), calls);
    }

    // Helpers

    /** A connection holding an open transaction with one transacted flow, every call recorded in order. */
    private static SolaceConnection connectionWith(List<String> calls) throws Exception {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        SolaceConnection connection = (SolaceConnection) ((Unsafe) theUnsafe.get(null))
                .allocateInstance(SolaceConnection.class);
        set(connection, "connectionId", "test-0");
        set(connection, "txLock", new Object());
        Class<?> entryType = Class.forName(SolaceConnection.class.getName() + "$TxFlowEntry");
        Constructor<?> entry = entryType.getDeclaredConstructor(FlowReceiver.class, String.class);
        entry.setAccessible(true);
        Map<String, Object> flows = new ConcurrentHashMap<>();
        flows.put("Q.test", entry.newInstance(recorder(FlowReceiver.class, "flow", calls), null));
        set(connection, "txFlowReceivers", flows);
        set(connection, "txSession", recorder(TransactedSession.class, "session", calls));
        set(connection, "txProducer", recorder(XMLMessageProducer.class, "producer", calls));
        return connection;
    }

    private static <T> T recorder(Class<T> type, String name, List<String> calls) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return method.getName().equals("equals") ? proxy == args[0]
                        : method.getName().equals("hashCode") ? System.identityHashCode(proxy) : name;
            }
            calls.add(name + "." + method.getName());
            Class<?> result = method.getReturnType();
            if (result == boolean.class) {
                return Boolean.FALSE;
            }
            if (result == int.class) {
                return 0;
            }
            if (result == long.class) {
                return 0L;
            }
            return null;
        }));
    }

    private static void set(Object target, String fieldName, Object value) throws Exception {
        Field field = SolaceConnection.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
