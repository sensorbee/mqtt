package mqtt

import (
	"errors"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
)

type source struct {
	ctx *core.Context
	w   core.Writer

	topic    string
	broker   string
	user     string
	password string

	minWait       time.Duration
	maxWait       time.Duration
	reconnRetries int64
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
	opts.AddBroker(s.broker)
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
		t := core.NewTuple(data.Map{
			"topic":   data.String(m.Topic()),
			"payload": data.Blob(m.Payload()),
		})
		w.Write(ctx, t)
	}

	waitUntilReconnect := 0 * time.Second
	retries := int64(0)
	backoff := func() error {
		// exponential backoff
		if waitUntilReconnect == 0 {
			waitUntilReconnect = s.minWait
		} else {
			waitUntilReconnect *= 2
		}
		// truncate to maximum
		if waitUntilReconnect > s.maxWait {
			waitUntilReconnect = s.maxWait
		}

		if s.reconnRetries >= 0 {
			if retries > s.reconnRetries {
				return errors.New("Gave up to connect to MQTT broker")
			} else {
				retries++
			}
		}
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
			if err := backoff(); err != nil {
				return err
			}
			ctx.ErrLog(connTok.Error()).WithField("waitUntilReconnect", waitUntilReconnect).
				Info("Failed to connect to MQTT broker")
			continue
		}

		// subscribe to topic
		if subTok := client.Subscribe(s.topic, 0, msgHandler); subTok.WaitTimeout(10*time.Second) && subTok.Error() != nil {
			if err := backoff(); err != nil {
				return err
			}
			ctx.ErrLog(subTok.Error()).WithField("topic", s.topic).
				Info("Failed to subscribe to topic")
			// create a new client object for the next try
			client.Disconnect(0)
			client = mqtt.NewClient(opts)
			continue
		}

		// once we succeeded, we reset the reconnect and retry counters
		waitUntilReconnect = 0 * time.Second
		retries = 0

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

// NewSource create a new Source receiving data from a MQTT broker. The source
// emits tuples like;
//
//	{
//		"topic": "foo/bar",
//		"payload": <blob>
//	}
//
// The topic field has topic of the message and the payload field has data
// as a blob. If the data contains JSON and a user wants to manipulate it,
// another stream needs to be created:
//
//	CREATE STREAM hoge AS
//	  SELECT RSTREAM decode_json(payload) AS * FROM mqtt_src [RANGE 1 TUPLES];
//
// The source has following required parameters:
//
//	* topic: the topic to be subscribed
//
// The source has following optional parameters:
//
//	* broker: the address of the broker in URI schema://"host:port" format (default: "tcp://127.0.0.1:1883")
//	* user: the user name to be connected (default: "")
//	* password: the password of the user (default: "")
//	* reconnect_min_time: minimal time to wait before reconnecting in Go duration format (default: 1s)
//	* reconnect_max_time: maximal time to wait before reconnecting in Go duration format (default: 30s)
//	* reconnect_retries: maximum numbers of reconnect retries. Any negative number means infinite retries (default: 10)
func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	s := &source{
		broker:        "tcp://127.0.0.1:1883",
		user:          "",
		password:      "",
		minWait:       1 * time.Second,
		maxWait:       30 * time.Second,
		reconnRetries: 10,
	}

	if v, ok := params["topic"]; !ok {
		return nil, errors.New("topic parameter is missing")
	} else {
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

	if v, ok := params["reconnect_min_time"]; ok {
		t, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		d, err := time.ParseDuration(t)
		if err != nil {
			return nil, err
		}
		s.minWait = d
	}

	if v, ok := params["reconnect_max_time"]; ok {
		t, err := data.AsString(v)
		if err != nil {
			return nil, err
		}
		d, err := time.ParseDuration(t)
		if err != nil {
			return nil, err
		}
		s.maxWait = d
	}

	if v, ok := params["reconnect_retries"]; ok {
		r, err := data.AsInt(v)
		if err != nil {
			return nil, err
		}
		s.reconnRetries = r
	}

	return core.ImplementSourceStop(s), nil
}
