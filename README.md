## Extensions for Datafaker Gen

This is a collection of extensions for the [Datafaker Gen](https://github.com/datafaker-net/datafaker-gen)

### How to build

```bash
./mvnw clean install compile assembly:single      
```

### How run

```bash
bin/datafaker_gen -f json -n 2 -sink cli
```