# tika-grpc-message-parser

Parse your own protobuf messages into Tika documents.

You already have a typed protobuf message: a reply from your own gRPC service, a Kafka record,
a crawler's WARC record. You want it back from Tika in the same shape as any parsed file. Write a
`MessageParser` for it, the same way you'd write a Tika parser for a file format.

## The interface

```java
public interface MessageParser<T extends Message> {
    Class<T> messageType();          // the message you read
    List<Property> properties();     // your dictionary: the keys you write, with their types
    List<Metadata> parse(T message) throws TikaException;
}
```

`parse` returns what `/rmeta` returns: `[0]` is the container, and embedded documents follow, each
with `TikaCoreProperties.EMBEDDED_ID_PATH` set. Text goes under `TikaCoreProperties.TIKA_CONTENT`.
You do the mapping in plain Java: read the getters, set the metadata.

## An example

```java
public class WarcRecordParser implements MessageParser<WarcRecord> {
    static final Property STATUS = Property.externalInteger("warc:http-status");

    public Class<WarcRecord> messageType() { return WarcRecord.class; }

    public List<Property> properties() { return List.of(STATUS); }

    public List<Metadata> parse(WarcRecord record) {
        Metadata m = new Metadata();
        m.set(TikaCoreProperties.SOURCE_PATH, record.getTargetUri());
        m.set(STATUS, record.getHttpStatus());
        m.set(TikaCoreProperties.TIKA_CONTENT, record.getText());
        return List.of(m);
    }
}
```

Put the class and your generated stubs in one jar, list the class in
`META-INF/services/org.apache.tika.grpc.spi.MessageParser`, and put the jar on the tika-grpc
classpath. Tika never sees your `.proto`. Two parsers for one message type stop the server at
startup.

## Calling it

`MessageParserService` in `org.apache.tika.grpc.messages.v2alpha1` runs on the tika-grpc port. It
takes the message as a `google.protobuf.Any` and picks the parser by its type URL:

| RPC | What you get |
|---|---|
| `ParseMessage` | every document in one `ParseMessageResponse` |
| `ParseMessageStream` | one document per message, same order |
| `ParseMessageBidi` | send many messages on one stream; one response per `request_id`. A failed message gets `error` and the stream keeps going |

```java
ParseMessageResponse r = MessageParserServiceGrpc.newBlockingStub(channel).parseMessage(
        ParseMessageRequest.newBuilder().setMessage(Any.pack(record)).build());
```

Each `ParsedDocument` has `embedded_path`, `content`, and `metadata` as a `google.protobuf.Struct`,
so it reads as plain JSON:

```json
{ "dc:title": ["Example"], "warc:http-status": 200, "tk:chunks": [{"text": "...", "start": 0}] }
```

Booleans and numbers are themselves, JSON keys are nested JSON, multi-valued keys are lists, and
everything else is Tika's string. A value that doesn't fit its type stays a string. Keys in Tika's
metadata registry use the registry's type; any other key uses the type from your `properties()`.

## Errors

| | Unary and stream | Bidi |
|---|---|---|
| no parser for the type | `NOT_FOUND` | `error.code` 5 |
| no message, bytes that don't unpack, or `parse` throws `TikaException` | `INVALID_ARGUMENT` | `error.code` 3 |
| `parse` throws anything else | `INTERNAL` | `error.code` 13 |

## Notes

- Parsers run in the tika-grpc server JVM, so they are trusted code.
- Message size is bounded by the server's gRPC inbound message limit.
