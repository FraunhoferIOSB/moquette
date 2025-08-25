/*
 * Copyright (c) 2012-2025 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.interception;

import io.moquette.broker.subscriptions.Subscription;
import io.moquette.broker.subscriptions.Topic;

/**
 * A topic re-writer can change the topics of subscriptions and publishes before
 * they are handled internally.
 */
public interface TopicRewriter {

    /**
     * Rewrite the topic for the given subscription.
     *
     * @param subscription The subscription to rewrite the Topic for.
     * @return the rewritten topic.
     */
    public Topic rewriteTopic(Subscription subscription);

    /**
     * Reverse the rewrite for the given Topic. This is needed when a
     * subscription that this topic matched has a wild-card and is rewritten.
     *
     * @param topic The topic to reverse-rewrite.
     * @return The topic as expected by the client.
     */
    public Topic inverseRewrite(Topic topic);
}
