package mqtt

import (
	"fmt"

	"github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
)

type sink struct {
	opts   *mqtt.ClientOptions
	client mqtt.Client

	qos          byte
	retained     bool
	broker       string
	user         string
	password     string
	payloadPath  data.Path
	topicPath    data.Path
	qosPath      data.Path
	defaultTopic string
}

func (s *sink) Write(ctx *core.Context, t *core.Tuple) error {
	if !s.client.IsConnected() {
		return nil
	}

	p, err := t.Data.Get(s.payloadPath)
	if err != nil {
		return err
	}

	var b []byte
	switch p.Type() {
	case data.TypeString:
		str, _ := data.AsString(p)
		b = []byte(str) // TODO: reduce this data copy
	case data.TypeBlob:
		b, _ = data.AsBlob(p)
	case data.TypeArray, data.TypeMap:
		b = []byte(p.String()) // TODO: reduce this data copy
	default:
		return fmt.Errorf("data type '%v' cannot be used as payload", p.Type())
	}

	topic := ""
	if to, err := t.Data.Get(s.topicPath); err != nil {
		if s.defaultTopic == "" {
			return fmt.Errorf("topic field '%v' is missing", s.topicPath)
		}
		topic = s.defaultTopic
	} else if topic, err = data.AsString(to); err != nil {
		return err
	}

	qos := byte(0)
	if q, err := t.Data.Get(s.qosPath); err != nil {
		qos = s.qos
	} else {
		qq, err := data.AsInt(q)
		if err != nil {
			return err
		} else if qq < 0 || qq > 2 {
			return fmt.Errorf("Wrong QoS: %d", qq)
		} else {
			qos = byte(qq)
		}
	}

	if token := s.client.Publish(topic, qos, s.retained, b); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (s *sink) Close(ctx *core.Context) error {
	s.client.Disconnect(250)
	return nil
}

// NewSink returns a sink as MQTT publisher. To publish a message, a tuple
// inserted into the sink needs to have two fields: "topic" and "payload".
// There is also one optional field: "qos", that should contain MQTT qos to
// publish message with:
//
//	{
//		"topic": "foo/bar",
//		"payload": "any form of data including JSON encoded in string",
//		"qos": 1
//	}
//
// In the case above, the topic of the message is "foo/bar" and the QoS is
// increased to 1 meaing message must be delivered at least once.
// The field names of topic and payload can be changed by setting topic_field,
// payload_field and qos_field parameters, respectively. When a payload
// needs is a string or a blob, it's directly sent to a broker. The payload can
// also be an array or a map, and it will be sent as JSON.
//
// The sink has following optional parameters:
//
//	* broker: the address of the broker in URI "schema://host:port" format (default: "tcp://127.0.0.1:1883")
//	* user: the user name to be connected (default: "")
//	* password: the password of the user (default: "")
//	* payload_field: the field name in tuples having a payload (default: "payload")
//	* topic_field: the field name in tuples having a topic (default: "")
//	* default_topic: the default topic used when a tuple doesn't have topic_field (default: "")
//	* default_qos: the default to publish tuples with, can be 0, 1 or 2 (default: 0)
func NewSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Sink, error) {
	s := &sink{
		qos:          0,
		retained:     false,
		broker:       "tcp://127.0.0.1:1883",
		user:         "",
		password:     "",
		payloadPath:  data.MustCompilePath("payload"),
		topicPath:    data.MustCompilePath("topic"),
		qosPath:      data.MustCompilePath("qos"),
		defaultTopic: "",
	}

	if v, ok := params["broker"]; ok {
		b, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		s.broker = b
	}

	if v, ok := params["user"]; ok {
		u, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		s.user = u
	}

	if v, ok := params["password"]; ok {
		p, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		s.password = p
	}

	if v, ok := params["payload_field"]; ok {
		name, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		path, err := data.CompilePath(name)
		if err != nil {
			return nil, err
		}
		s.payloadPath = path
	}

	if v, ok := params["topic_field"]; ok {
		name, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		path, err := data.CompilePath(name)
		if err != nil {
			return nil, err
		}
		s.topicPath = path
	}

	if v, ok := params["default_topic"]; ok {
		t, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		if t == "" {
			return nil, fmt.Errorf("empty default topic is not supported")
		}
		s.defaultTopic = t
	}

	if v, ok := params["qos_field"]; ok {
		name, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		path, err := data.CompilePath(name)
		if err != nil {
			return nil, err
		}
		s.qosPath = path
	}
	if v, ok := params["default_qos"]; ok {
		q, err := data.AsInt(v)
		if err != nil {
			return nil, err
		}
		if q < 0 || q > 2 {
			return nil, fmt.Errorf("Unknown QoS. Qos can only be between 0 and 2")
		}
		s.qos = byte(q)
	}

	s.opts = mqtt.NewClientOptions()
	s.opts.AddBroker(s.broker)
	if s.user != "" {
		s.opts.Username = s.user
		s.opts.Password = s.password
	}

	s.client = mqtt.NewClient(s.opts)
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		// TODO: error log
		return nil, token.Error()
	}

	return s, nil
}
