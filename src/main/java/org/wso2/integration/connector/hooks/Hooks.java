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
package org.wso2.integration.connector.hooks;

import java.util.concurrent.CompletableFuture;

/**
 * The one indirection the test campaign uses for latches and toggles (guide 0.5).
 * The shipped build resolves the no op subclass and pays one static read and one virtual call per hook point.
 */
public abstract class Hooks {

    private static final Hooks INSTANCE = resolve();

    public static Hooks get() {
        return INSTANCE;
    }

    public Object point(String name) {
        return null;
    }

    public Object point(String name, Object... args) {
        return null;
    }

    public void record(String kind, long nanos) {
    }

    public <T> CompletableFuture<T> wrapConfirmation(CompletableFuture<T> inner) {
        return inner;
    }

    public static final class NoOp extends Hooks {
    }

    public static class HookFailure extends RuntimeException {

        public HookFailure(String message) {
            super(message);
        }
    }

    private static Hooks resolve() {
        String className = System.getProperty("solace.test.hooks", "");
        if (className.isEmpty()) {
            return new NoOp();
        }
        try {
            Class<?> hookClass = Class.forName(className, true, Hooks.class.getClassLoader());
            return (Hooks) hookClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            return new NoOp();
        }
    }
}
