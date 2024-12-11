package controller

import (
	"context"
	"crypto/tls"
	"github.com/Go-routine-4595/oem-converter/model"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	uuid "github.com/satori/go.uuid"
	"os"
	"sync"
	"time"
)

const (
	maxChanSize        = 1000
	qos                = 0
	waitTime           = 1000 * time.Millisecond
	maxRetry           = 30
	mqttClientIDPrefix = "oem-converter-"
)

type MqttConf struct {
	Connection string `yaml:"Connection"`
	Topic      string `yaml:"Topic"`
}

type Controller struct {
	client   mqtt.Client
	dispatch chan *model.Item
	logger   zerolog.Logger
}

// NewController initializes and returns a new instance of Controller with the given MQTT configuration, context, and log level.
func NewController(conf MqttConf, ctx context.Context, logl int, wg *sync.WaitGroup) *Controller {
	var (
		opts *mqtt.ClientOptions
		c    *Controller
	)

	c = &Controller{}
	wg.Add(1)

	//create a new client
	c.logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(logl)).With().Timestamp().Int("pid", os.Getpid()).Logger()

	c.dispatch = make(chan *model.Item, maxChanSize)
	opts = c.newClientOptions(conf.Connection)

	if !c.connectClient(opts) {
		c.logger.Error().Msg("Failed to connect to MQTT server after maximum retries. Exiting.")
		os.Exit(1) // Exit with a non-zero status indicating failure.
	}

	if !c.subscribeToTopic(conf.Topic) {
		c.logger.Error().Msg("Failed to connect to subscribe to MQTT server topics after maximum retries. Exiting.")
		os.Exit(1) // Exit with a non-zero status indicating failure.
	}
	c.logger.Info().Msgf("Controller connected to mqtt server: %s \t topic: %s", conf.Connection, conf.Topic)

	go func() {
		<-ctx.Done()
		c.client.Disconnect(250)
		c.logger.Warn().Msg("Mqtt controller disconnect")
		wg.Done()
	}()

	return c
}

// Helper to create MQTT client options
func (c *Controller) newClientOptions(broker string) *mqtt.ClientOptions {
	return mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(mqttClientIDPrefix + uuid.NewV4().String()).
		SetTLSConfig(&tls.Config{InsecureSkipVerify: true}).
		SetDefaultPublishHandler(c.forward)
}

// Handles topic subscription with retries
func (c *Controller) subscribeToTopic(topic string) bool {
	var token mqtt.Token
	for i := 0; i < maxRetry; i++ {
		if token = c.client.Subscribe(topic, qos, nil); token.Wait() && token.Error() == nil {
			return true
		}
		c.logger.Error().Msgf("Failed to connect to subscribe to MQTT server topics after maximum retries. Exiting. %v \n", token.Error())
		time.Sleep(waitTime)
	}
	return false
}

// Handles connecting to the MQTT client with retries
func (c *Controller) connectClient(opts *mqtt.ClientOptions) bool {
	var token mqtt.Token
	for i := 0; i < maxRetry; i++ {
		c.client = mqtt.NewClient(opts)
		if token = c.client.Connect(); token.Wait() && token.Error() == nil {
			return true
		}
		c.logger.Err(token.Error()).Msg("Error connecting to MQTT server")
		time.Sleep(waitTime)
	}
	return false
}

func (c *Controller) Start() {

}

// forward processes incoming MQTT messages, assigns them timestamps, and forwards them to the dispatch channel.
func (c *Controller) forward(_ mqtt.Client, msg mqtt.Message) {
	var (
		i *model.Item
	)

	i = new(model.Item)

	i.Rcv = time.Now()
	i.Data = msg.Payload()

	select {
	case c.dispatch <- i:
	default:
		c.logger.Warn().Msgf("Channel full Dropping message: %s", msg.Payload())
	}

}

// GetMessage retrieves an item from the dispatch channel in the Controller.
func (c *Controller) GetMessage() *model.Item {
	var (
		i *model.Item
	)
	select {
	case i = <-c.dispatch:
		return i
	default:
		return nil
	}
}
