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
		b = []byte(str)
	case data.TypeBlob:
		b, _ = data.AsBlob(p)
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

	if token := s.client.Publish(topic, s.qos, s.retained, b); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (s *sink) Close(ctx *core.Context) error {
	s.client.Disconnect(250)
	return nil
}

// NewSink returns a sink as MQTT publisher. To publish a message, a tuple
// inserted into the sink needs to have two fields: "topic" and "payload":
//
//	{
//		"topic": "foo/bar",
//		"payload": "any form of dataa including JSON encoded in string"
//	}
//
// A payload needs to be a string or a blob and it's directly sent to a broker.
// In the case above, the topic of the message is "foo/bar". The field names of
// topic and payload can be changed by setting topic_field and payload_field
// parameters, respectively.
//
// The sink has following optional parameters:
//
//	* broker: the address of the broker in "host:port" format (default: "127.0.0.1:1883")
//	* user: the user name to be connected (default: "")
//	* password: the password of the user (default: "")
//	* payload_field: the field name in tuples having a payload (default: "payload")
//	* topic_field: the field name in tuples having a topic (default: "")
//	* default_topic: the default topic used when a tuple doesn't have topic_field (default: "")
func NewSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Sink, error) {
	s := &sink{
		qos:          0,
		retained:     false,
		broker:       "127.0.0.1:1883",
		user:         "",
		password:     "",
		payloadPath:  data.MustCompilePath("payload"),
		topicPath:    data.MustCompilePath("topic"),
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

	s.opts = mqtt.NewClientOptions()
	s.opts.AddBroker("tcp://" + s.broker)
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
