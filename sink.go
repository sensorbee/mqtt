package mqtt

import (
	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/sensorbee/core"
	"pfi/sensorbee/sensorbee/data"
)

type sink struct {
	opts   *MQTT.ClientOptions
	client *MQTT.Client

	qos      byte
	retained bool
	topic    string
	broker   string
	user     string
	password string
}

func (s *sink) Write(ctx *core.Context, t *core.Tuple) error {
	if !s.client.IsConnected() {
		return nil
	}

	js := t.Data.String() // convert json string
	if token := s.client.Publish(s.topic, s.qos, s.retained, []byte(js)); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (s *sink) Close(ctx *core.Context) error {
	s.client.Disconnect(250)
	return nil
}

func NewSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Sink, error) {
	s := &sink{
		qos:      0,
		retained: false,
		broker:   "127.0.0.1:1883",
		topic:    "/",
		user:     "",
		password: "",
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
