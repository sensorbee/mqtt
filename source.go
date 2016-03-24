package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
	"sync"
	"time"
)

type source struct {
	ctx *core.Context
	w   core.Writer

	opts   *MQTT.ClientOptions
	client *MQTT.Client

	mut     sync.Mutex
	stopped bool

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
	s.opts = MQTT.NewClientOptions()
	s.opts.AddBroker("tcp://" + s.broker)
	if s.user != "" {
		s.opts.Username = s.user
		s.opts.Password = s.password
	}
	s.opts.OnConnectionLost = func(c *MQTT.Client, e error) {
		// write `true` to signal that the connection was not
		// terminated on purpose and we should try to reconnect
		ctx.Log().Info("Lost connection to MQTT broker")
		s.disconnect <- true
	}
	s.opts.AutoReconnect = false

	// NB. if we have just one client instance and create it here,
	//     then the OnConnectionLost handler will only be called once;
	//     therefore we create a new client for every reconnect
	s.client = MQTT.NewClient(s.opts)

	// define what to do with messages
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
		// wait in second-steps so that we can check whether
		// the Stop() function has been called while waiting
		for i := int64(0); i < int64(waitUntilReconnect/time.Second); i++ {
			time.Sleep(time.Second)
			s.mut.Lock()
			stopped := s.stopped // set by the Stop() function
			s.mut.Unlock()
			if stopped {
				break ReconnectLoop
			}
		}

		// try to connect
		ctx.Log().Info("Connecting to MQTT broker at ", s.broker)
		if connTok := s.client.Connect(); connTok.WaitTimeout(10*time.Second) && connTok.Error() != nil {
			backoff()
			ctx.ErrLog(connTok.Error()).
				Info("Failed to connect to MQTT broker, reconnecting in ", waitUntilReconnect)
			continue
		}

		// subscribe to topic
		if subTok := s.client.Subscribe(s.topic, 0, msgHandler); subTok.WaitTimeout(10*time.Second) && subTok.Error() != nil {
			backoff()
			ctx.ErrLog(subTok.Error()).
				Info("Failed to subscribe to topic: %s", s.topic)
			// create a new client object for the next try
			s.client.Disconnect(0)
			s.client = MQTT.NewClient(s.opts)
			continue
		}

		// once we succeeded, we reset the reconnect counter
		waitUntilReconnect = 0 * time.Second

		// here we wait until the handler in OnConnectionLost
		// or Stop() pushes something into the `disconnect` channel
		needsReconnect := <-s.disconnect
		if !needsReconnect {
			break
		}
		// create a new client object for the next try
		s.client = MQTT.NewClient(s.opts)
	}

	return nil
}

func (s *source) Stop(ctx *core.Context) error {
	// Disconnects caused by calling Disconnect or ForceDisconnect will
	// not cause an OnConnectionLost callback to execute. Therefore
	// we need to write to the channel explicitly.
	s.client.Disconnect(250)
	// write `false` to signal that we should not try to reconnect
	s.disconnect <- false
	// also, in case that we are not connected yet, set a flag
	// that we should stop trying
	s.mut.Lock()
	defer s.mut.Unlock()
	s.stopped = true

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
