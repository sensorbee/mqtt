package mqtt

import (
	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
	"time"
)

type source struct {
	ctx *core.Context
	w   core.Writer

	opts   *MQTT.ClientOptions
	client *MQTT.Client

	topic    string
	broker   string
	user     string
	password string

	// channel that will be written to when the
	// connection is lost
	disconnect chan struct{}
}

func (s *source) GenerateStream(ctx *core.Context, w core.Writer) error {
	s.ctx = ctx
	s.w = w

	s.disconnect = make(chan struct{}, 1)

	s.opts = MQTT.NewClientOptions()
	s.opts.AddBroker("tcp://" + s.broker)
	if s.user != "" {
		s.opts.Username = s.user
		s.opts.Password = s.password
	}
	s.opts.OnConnectionLost = func(*MQTT.Client, error) {
		s.disconnect <- struct{}{}
	}

	s.client = MQTT.NewClient(s.opts)

	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		// TODO: error log
		return token.Error()
	}

	msgHandler := func(c *MQTT.Client, m MQTT.Message) {
		now := time.Now().UTC()
		t := &core.Tuple{
			ProcTimestamp: now,
			Timestamp:     now,
		}
		t.Data = data.Map{
			"topic":   data.String(m.Topic()),
			"payload": data.Blob(m.Payload()),
		}
		w.Write(ctx, t)
	}

	// subscribe to topic
	if token := s.client.Subscribe(s.topic, 0, msgHandler); token.Wait() && token.Error() != nil {
		// TODO: error log
		return token.Error()
	}

	// wait for the disconnect handler to push
	// something into the pipe
	<-s.disconnect

	return nil
}

func (s *source) Stop(ctx *core.Context) error {
	// Disconnects caused by calling Disconnect or ForceDisconnect will
	// not cause an OnConnectionLost callback to execute. Therefore
	// we need to write to the channel explicitly.
	s.client.Disconnect(250)
	s.disconnect <- struct{}{}
	return nil
}

// NewSource create a new Source receiving data from MQTT broker.
//
// topic: set topics
//
// broker: set IP address, default "172.0.0.1:1883"
//
// user: set user name, default ""
//
// password: set password, default ""
func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	s := &source{
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

	return core.ImplementSourceStop(s), nil
}
