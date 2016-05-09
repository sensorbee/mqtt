package mqtt

import (
	"github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
	"time"
)

type source struct {
	ctx *core.Context
	w   core.Writer

	topic    string
	broker   string
	user     string
	password string

	// channel that will be written to when the
	// connection is lost
	disconnect chan bool
}

func (s *source) GenerateStream(ctx *core.Context, w core.Writer) error {
	s.ctx = ctx
	s.w = w

	s.disconnect = make(chan bool, 1)

	// define where and how to connect
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + s.broker)
	if s.user != "" {
		opts.Username = s.user
		opts.Password = s.password
	}
	opts.OnConnectionLost = func(c mqtt.Client, e error) {
		// write `true` to signal that the connection was not
		// terminated on purpose and we should try to reconnect
		ctx.Log().Info("Lost connection to MQTT broker")
		s.disconnect <- true
	}
	opts.AutoReconnect = false

	// NB. if we have just one client instance and create it here,
	//     then the OnConnectionLost handler will only be called once;
	//     therefore we create a new client for every reconnect
	client := mqtt.NewClient(opts)

	// define what to do with messages
	msgHandler := func(c mqtt.Client, m mqtt.Message) {
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

	waitUntilReconnect := 0 * time.Second
	// TODO make minWait and maxWait user-configurable
	minWait := 1 * time.Second
	maxWait := 1 * time.Minute
	backoff := func() error {
		// exponential backoff
		if waitUntilReconnect == 0 {
			waitUntilReconnect = minWait
		} else {
			waitUntilReconnect *= 2
		}
		// truncate to maximum
		if waitUntilReconnect > maxWait {
			waitUntilReconnect = maxWait
		}
		// TODO also keep track of how often we have retried
		//      and return an error if above some limit
		return nil
	}

	// connect in an endless loop
ReconnectLoop:
	for {
		// we wait here the specified time between reconnects,
		// but if Stop() (or another disconnect, but actually
		// we should not be connected here) is called, return
		// earlier
		select {
		case <-time.After(waitUntilReconnect):
		case needsReconnect := <-s.disconnect:
			if !needsReconnect {
				break ReconnectLoop
			}
		}

		// try to connect
		ctx.Log().WithField("broker", s.broker).Info("Connecting to MQTT broker")
		if connTok := client.Connect(); connTok.WaitTimeout(10*time.Second) && connTok.Error() != nil {
			backoff()
			ctx.ErrLog(connTok.Error()).WithField("waitUntilReconnect", waitUntilReconnect).
				Info("Failed to connect to MQTT broker")
			continue
		}

		// subscribe to topic
		if subTok := client.Subscribe(s.topic, 0, msgHandler); subTok.WaitTimeout(10*time.Second) && subTok.Error() != nil {
			backoff()
			ctx.ErrLog(subTok.Error()).WithField("topic", s.topic).
				Info("Failed to subscribe to topic")
			// create a new client object for the next try
			client.Disconnect(0)
			client = mqtt.NewClient(opts)
			continue
		}

		// once we succeeded, we reset the reconnect counter
		waitUntilReconnect = 0 * time.Second

		// here we wait until the handler in OnConnectionLost
		// or Stop() pushes something into the `disconnect` channel
		needsReconnect := <-s.disconnect
		if !needsReconnect {
			// if needsReconnect is false, then Stop()
			// was called before, so we need to do a graceful
			// shutdown
			client.Disconnect(250)
			break
		}
		// create a new client object for the next try
		client = mqtt.NewClient(opts)
	}

	return nil
}

func (s *source) Stop(ctx *core.Context) error {
	// write `false` to signal that we should not try to reconnect
	s.disconnect <- false

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
