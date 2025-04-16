## logging cheats

The kafka client is quite chatty and by default a lot gets logged on info level. To be less noisy, turn the log
level to `warn`:

```
org.apache.kafka=warn
```

However, interesting stuff might happen in some components, e.g. the producer. To show for example the configuration
of the producer:

```
org.apache.kafka.clients.producer=info
```

Show information about consumer:

```
org.apache.kafka.clients.consumer=info
```