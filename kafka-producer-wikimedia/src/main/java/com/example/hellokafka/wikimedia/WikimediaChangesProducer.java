package com.example.hellokafka.wikimedia;

import com.launchdarkly.eventsource.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) {

        var myProducer = new MyProducer();

        var url = "https://stream.wikimedia.org/v2/stream/recentchange";
        var eventSource = new EventSource.Builder(URI.create(url))
                .build();

        try {

            while (true) {

                // pattern matching for switch!!
                var streamEvent = eventSource.readAnyEvent();
                switch (streamEvent) {
                    case StartedEvent s -> log.info("ignoring started event");
                    case CommentEvent c -> log.info("ignoring comment event");
                    case MessageEvent m -> myProducer.sendIt(m.getData());
                    case FaultEvent f -> log.info("ignoring fault event");
                    default -> log.error("unexpected event: {}", streamEvent);
                }
            }

        } catch (StreamException e) {
            log.error("exception occurred", e);
            myProducer.closeIt();
        } finally {
            eventSource.close();
            myProducer.closeIt();
        }

    }

}
