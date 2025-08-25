/*
 * Copyright (c) 2012-2018 The original author or authors
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
package io.moquette.broker.subscriptions;

import static io.moquette.broker.subscriptions.ShareName.EMPTY_SHARENAME;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Maintain the information about which Topic a certain ClientID is subscribed and at which QoS
 */
public final class Subscription implements Serializable, Comparable<Subscription> {

    private static final long serialVersionUID = -3383457629635732794L;
    private final MqttSubscriptionOption option;
    final String clientId;
    final Topic topicFilterClient;
    private Topic topicFilterInternal;
    final ShareName shareName;

    private final Optional<SubscriptionIdentifier> subscriptionId;

    public Subscription(String clientId, Topic topicFilter, MqttSubscriptionOption options) {
        this(clientId, topicFilter, options, EMPTY_SHARENAME, Optional.empty());
    }

    public Subscription(String clientId, Topic topicFilter, MqttSubscriptionOption options, SubscriptionIdentifier subscriptionId) {
        this(clientId, topicFilter, options, EMPTY_SHARENAME, subscriptionId);
    }

    public Subscription(String clientId, Topic topicFilter, MqttSubscriptionOption options, ShareName shareName) {
        this(clientId, topicFilter, options, shareName, Optional.empty());
    }

    public Subscription(String clientId, Topic topicFilter, MqttSubscriptionOption options, ShareName shareName,
            SubscriptionIdentifier subscriptionId) {
        this(clientId, topicFilter, options, shareName, Optional.ofNullable(subscriptionId));
    }

    public Subscription(String clientId, Topic topicFilter, MqttSubscriptionOption options,
            Optional<SubscriptionIdentifier> subscriptionId) {
        this(clientId, topicFilter, options, EMPTY_SHARENAME, subscriptionId);
    }

    public Subscription(String clientId, Topic topicFilter, MqttSubscriptionOption options, ShareName shareName,
            Optional<SubscriptionIdentifier> subscriptionId) {
        this.clientId = clientId;
        this.topicFilterClient = topicFilter;
        this.topicFilterInternal = topicFilter;
        this.shareName = shareName;
        this.subscriptionId = subscriptionId;
        this.option = options;
    }

    public String getClientId() {
        return clientId;
    }

    public Topic getTopicFilterInternal() {
        return topicFilterInternal;
    }

    public void setTopicFilterInternal(Topic topicFilterInternal) {
        this.topicFilterInternal = topicFilterInternal;
    }

    public Topic getTopicFilterClient() {
        return topicFilterClient;
    }

    public boolean isTopicRewritten() {
        return topicFilterInternal.equals(topicFilterClient);
    }

    public boolean qosLessThan(Subscription sub) {
        return option.qos().value() < sub.option.qos().value();
    }

    public boolean hasSubscriptionIdentifier() {
        return subscriptionId.isPresent();
    }

    public SubscriptionIdentifier getSubscriptionIdentifier() {
        return subscriptionId.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(clientId, that.clientId) &&
            Objects.equals(shareName, that.shareName) &&
            Objects.equals(topicFilterInternal, that.topicFilterInternal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, shareName, topicFilterInternal);
    }

    @Override
    public String toString() {
        return String.format("[filter:%s, clientID: %s, options: %s - shareName: %s]", topicFilterInternal, clientId, option, shareName);
    }

    @Override
    public Subscription clone() {
        try {
            return (Subscription) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }

    // The identity is important because used in CTries CNodes to check when a subscription is a duplicate or not.
    @Override
    public int compareTo(Subscription o) {
        int compare = this.clientId.compareTo(o.clientId);
        if (compare != 0) {
            return compare;
        }
        compare = this.shareName.compareTo(o.shareName);
        if (compare != 0) {
            return compare;
        }
        return this.topicFilterInternal.compareTo(o.topicFilterInternal);
    }

    public String clientAndShareName() {
        return clientId + (shareName.isEmpty() ? "" : "-" + shareName);
    }

    public boolean hasShareName() {
        return !shareName.isEmpty();
    }

    public ShareName getShareName() {
        return shareName;
    }

    public MqttSubscriptionOption getOption() {
        return option;
    }
}
