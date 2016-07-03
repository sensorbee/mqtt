# MQTT source and sink plugins for SensorBee

This repository has MQTT source and sink plugins for SensorBee.

## Usage

Add `gopkg.in/sensorbee/mqtt.v1/plugin` to build.yaml and build a `sensorbee`
command with `build_sensorbee`.

### Source

The MQTT source subscribes a topic from a MQTT broker. To create a source,
use the `CREATE SOURCE` statement with the type `mqtt`:

```sql
> CREATE SOURCE mqtt_src TYPE mqtt WITH topic = "some/topic";
```

The source generates tuples having two fields as shown below:

```
{
    "topic": "topic/of/the/message",
    "payload": <blob>
}
```

The `topic` field has a topic of a message. Its value varies if a source is
subscribing a topic with a wildcard. The `payload` field has a payload of a
message as a blob. To manipulate `payload` in BQL, it needs to be parsed. For
example, when the payload contains a JSON object, a stream like a following is
required:

```sql
> CREATE STREAM mqtt_parsed AS
    SELECT RSTREAM topic, decode_json(payload) AS data FROM mqtt_src [RANGE 1 TUPLES];
```

When a topic isn't necessary, the result of decode_json can be extracted at the
top level:

```sql
> CREATE STREAM mqtt_parsed AS
    SELECT RSTREAM decode_json(payload) AS * FROM mqtt_src [RANGE 1 TUPLES];
```

### Sink

The MQTT sink publishes a message to a MQTT broker. To create a sink, use the
`CREATE SINK` statement with type `mqtt`:

```sql
> CREATE SINK mqtt_sink TYPE mqtt;
```

To publish a message from the sink, insert a tuple having `topic` and `payload`
fields to it. Then, the message will be published with the given topic and
data in the `payload` field.

The `payload` field can be one of four types: `string`, `blob`, `array`, or
`map`. When it's a `string` or a `blob`, its value is directly sent to the
broker.

``` sql
> CREATE STREAM processed AS
    SELECT RSTREAM "foo/bar" AS topic, encode_json(data) AS payload
    FROM some_stream [RANGE 1 TUPLES];
> INSERT INTO mqtt_sink FROM processed;
```

The `processed` stream above encodes the `data` field that is assumed to have
data as a map, so the resulting `payload` field is a string cnotaining a JSON
object. Then, the content will directly be sent to the broker with "foo/bar"
topic.

When the payload is an `array` or a `map`, it'll be encoded into JSON and sent
to the broker. Therefore, the example above is actually equivalent to the one
below:

``` sql
> CREATE STREAM processed AS
    SELECT RSTREAM "foo/bar" AS topic, data AS payload
    FROM some_stream [RANGE 1 TUPLES];
> INSERT INTO mqtt_sink FROM processed;
```

The name of `topic` and `payload` fields can be changed by setting `topic_field`
and `payload_field` parameters described later.

## Reference

### Source Parameters

The MQTT source has a required parameter `topic` and following optional
parameters.

* `broker`
* `user`
* `password`

#### `topic`

`topic` specifies a topic to which the source subscribes. It can contain
wildcards. `topic` is a required parameter.

#### `broker`

`broker` is the address of the MQTT broker from which the source subscribes.
The address should be in `"host:port"` format. The default value is
`127.0.0.1:1883`.

#### `user`

`user` is the user name used to connect to the broker. The default value is
an empty string.

#### `password`

`password` is the password of the user specified by the `user` parameter.
The default value is an empty string.

### Sink

The MQTT sink has following optional parameters.

* `broker`
* `user`
* `password`
* `topic_field`
* `payload_field`
* `default_topic`

#### `broker`

`broker` is the address of the MQTT broker to which the sink publishes messages.
The address should be in `"host:port"` format. The default value is
`127.0.0.1:1883`.

#### `user`

`user` is the user name used to connect to the broker. The default value is
an empty string.

#### `password`

`password` is the password of the user specified by the `user` parameter.
The default value is an empty string.

#### `topic_field`

`topic_field` is the name of the field containing a topic as a string. For
example, when `topic_field = "my_topic"`, a tuple inserted into the sink should
look like below:

```
{
    "my_topic": "some/topic",
    "payload": ... payload data ...
}
```

The default value is `topic`.

#### `payload_field`

`payload_field` is the name of the field containing a payload. For example,
when `payload_field = "my_payload"`, a tuple inserted into the sink should look
like below:

```
{
    "topic": "some/topic",
    "my_payload": ... payload data ...
}
```

Note that both `topic_field` and `payload_field` can be specified at once.

#### `default_topic`

`default_topic` is used when a tuple doesn't have a topic field. Its value
must not be empty. When this parameter is not specified, a tuple missing the
topic field will result in an error and will not be published.
