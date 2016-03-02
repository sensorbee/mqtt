package mqtt

import (
	"fmt"
	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
)

type sink struct {
	opts   *MQTT.ClientOptions
	client *MQTT.Client

	qos         byte
	retained    bool
	topic       string
	broker      string
	user        string
	password    string
	payloadPath data.Path
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

	if token := s.client.Publish(s.topic, s.qos, s.retained, b); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (s *sink) Close(ctx *core.Context) error {
	s.client.Disconnect(250)
	return nil
}

// NewSink returns a sink as MQTT publisher.
//
// topic: set topics
//
// broker: set IP address, default "172.0.0.1:1883"
//
// user: set user name, default ""
//
// password: set password, default ""
//
// payload_field: set payload key name, default "payload"
func NewSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Sink, error) {
	s := &sink{
		qos:         0,
		retained:    false,
		broker:      "127.0.0.1:1883",
		topic:       "/",
		user:        "",
		password:    "",
		payloadPath: data.MustCompilePath("payload"),
	}

	if v, ok := params["topic"]; ok {
		t, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		s.topic = t
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

	s.opts = MQTT.NewClientOptions()
	s.opts.AddBroker("tcp://" + s.broker)
	if s.user != "" {
		s.opts.Username = s.user
		s.opts.Password = s.password
	}

	s.client = MQTT.NewClient(s.opts)
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		// TODO: error log
		return nil, token.Error()
	}

	return s, nil
}
