package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	pmqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	uuid "github.com/satori/go.uuid"
	"os"
	"time"
)

type Config struct {
	Connection string
	Topic      string
	Key        string
	LogLevel   int
}

type Mqtt struct {
	Topic    string
	logger   zerolog.Logger
	opt      *pmqtt.ClientOptions
	ClientID uuid.UUID
	client   pmqtt.Client
}

// NewMqtt initializes a new Mqtt instance with given configuration, log level, and context.
// It sets up the logger, client options, and handles connection and reconnection behaviors.
// It also handles graceful disconnection upon context cancellation and returns the created Mqtt instance or error.
func NewMqtt(conf Config, ctx context.Context) (*Mqtt, error) {
	var (
		err error
		l   zerolog.Logger
		cid uuid.UUID
	)

	l = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(conf.LogLevel)).With().Timestamp().Int("pid", os.Getpid()).Logger()
	cid = uuid.NewV4()
	c := &Mqtt{
		Topic:    conf.Topic,
		logger:   l,
		ClientID: cid,
		opt: pmqtt.NewClientOptions().
			AddBroker(conf.Connection).
			SetClientID("oem-converter-forwarder-" + cid.String()).
			SetCleanSession(true).
			SetAutoReconnect(true).
			SetTLSConfig(&tls.Config{
				InsecureSkipVerify: true,
			}).
			SetConnectionLostHandler(ConnectLostHandler(l)).
			SetOnConnectHandler(ConnectHandler(l)),
	}

	//opt.AddBroker("ssl://broker.emqx.io:8883")

	//c.opt.AddBroker("tcp://broker.hivemq.com:1883")

	go func() {
		<-ctx.Done()
		c.client.Disconnect(500)
		c.logger.Warn().Msg("Mqtt disconnect")
	}()

	err = c.Connect()

	return c, err
}

// Forward publishes a byte slice `b` to the configured MQTT topic as a QoS 1 message.
// It waits for up to 200 milliseconds for the publish operation to complete.
// If the publish operation exceeds the timeout, the function logs an error message.
func (m *Mqtt) Forward(b []byte) error {
	var (
		token pmqtt.Token
	)

	token = m.client.Publish(m.Topic, 1, false, b)
	if token.WaitTimeout(200*time.Millisecond) && token.Error() != nil {
		m.logger.Error().Err(token.Error()).Str("event", fmt.Sprintf("%v", string(b))).Msg("Timeout exceeded during publishing")
	}

	return nil
}

// Disconnect terminates the connection to the MQTT broker, waits up to 500 milliseconds, logs the action, and sets the client to nil.
func (m *Mqtt) Disconnect() {
	m.client.Disconnect(500)
	m.logger.Warn().Msg("Disconnected from mqtt broker")
	m.client = nil
}

// Connect establishes a connection to the MQTT broker using the provided client options.
// If the connection fails, it logs the error and returns an aggregated error.
func (m *Mqtt) Connect() error {
	m.client = pmqtt.NewClient(m.opt)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		m.logger.Error().Err(token.Error()).Msg("Error connecting to mqtt broker")
		return errors.Join(token.Error(), errors.New("Error connecting to mqtt broker"))
	}
	return nil
}

// ConnectHandler returns a function that logs a message when the MQTT client successfully connects to the broker.
func (m *Mqtt) ConnectHandler() func(client pmqtt.Client) {
	return func(client pmqtt.Client) {
		m.logger.Info().Msg("Forwarder connected to mqtt broker")
	}
}

// ConnectLostHandler returns a function that handles MQTT connection loss by logging a warning message with the error details.
func (m *Mqtt) ConnectLostHandler() func(client pmqtt.Client, err error) {
	return func(client pmqtt.Client, err error) {
		m.logger.Warn().Err(err).Msg("Forwarder connection Lost")
	}
}

// ConnectHandler returns a function that logs a message when the MQTT client successfully connects to the broker.
func ConnectHandler(logger zerolog.Logger) func(client pmqtt.Client) {
	return func(client pmqtt.Client) {
		logger.Info().Msg("Forwarder connected to mqtt broker")
	}
}

// ConnectLostHandler returns a function that handles MQTT connection loss by logging a warning message with the error details.
func ConnectLostHandler(logger zerolog.Logger) func(client pmqtt.Client, err error) {
	return func(client pmqtt.Client, err error) {
		logger.Warn().Err(err).Msg("Forwarder connection Lost")
	}
}
