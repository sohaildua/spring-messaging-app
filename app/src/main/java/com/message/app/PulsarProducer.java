package com.message.app;

import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class PulsarProducer {

    private final PulsarTemplate<User> template;
    private final PulsarTemplate<String> stringTemplate;

    private static final String USER_TOPIC = "user-topic";
    private static final String USER_TOPIC_STR = "string-topic";

    public PulsarProducer(PulsarTemplate<User> template, PulsarTemplate<String> stringTemplate) {
        this.template = template;
        this.stringTemplate = stringTemplate;
    }

    public void sendMessageToPulsarTopic(User user) throws PulsarClientException {
        template.newMessage(user)
                .withProducerCustomizer(pc -> pc.accessMode(ProducerAccessMode.Shared))
                .withMessageCustomizer(mc -> mc.deliverAfter(10L, TimeUnit.SECONDS))
                .withTopic(USER_TOPIC)
                .send();
    }

    public void sendStringMessageToPulsarTopic(String str) throws PulsarClientException {
        stringTemplate.send(USER_TOPIC_STR, str);
    }
}