package com.palmtree.demo;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowControlSettings.Builder;
import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;

import static com.google.cloud.pubsub.v1.Subscriber.defaultBuilder;

public class PubSubSubscriberDemo {

    private static final String PROJECT_ID = "krtestprojectshelf";
    private static final String SUBSCRIPTION_ID = "visualizer-subscription";

    public static void main(String... args) throws Exception {
        try {
            receiveMessages();
        } catch (Exception e) {
            System.out.println("Failed while receiving pub/sub messages - " + e.getMessage());
        }
    }

    private static void receiveMessages() throws Exception {

        SubscriptionName subscription = SubscriptionName.create(PROJECT_ID, SUBSCRIPTION_ID);
        MessageReceiver receiver = new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                System.out.println("Received message: " + message.getData().toStringUtf8());
                consumer.ack();
            }
        };

        Subscriber subscriber = null;
        try {
            subscriber = defaultBuilder(subscription, receiver).build();
            subscriber.addListener(
                    new Subscriber.Listener() {
                        @Override
                        public void failed(Subscriber.State from, Throwable failure) {
                            // Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
                            System.out.println("error");
                        }
                    },
                    MoreExecutors.directExecutor());
            subscriber.startAsync().awaitRunning();
            subscriber.getFlowControlSettings().toBuilder().setMaxOutstandingElementCount(5L);
            Thread.sleep(60000);
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }


}
