package service

import (
	"Go-routine-4595/oem-converter/adapters/gateway/mqtt"
	"Go-routine-4595/oem-converter/model"
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"os"
	"runtime"
	"sync"
	"time"
)

type IController interface {
	GetMessage() *model.Item
}

type IGateway interface {
	Forward(b []byte) error
	Disconnect()
}

type Service struct {
	controller IController
	gateWay    []IGateway
	logger     zerolog.Logger
	key        string
}

// NewService initializes a new Service instance with the specified IController, MqttConf configuration, and log level.
// It sets up the logging, creates MQTT gateways for each CPU core, and returns the created Service instance.
func NewService(c IController, conf mqtt.MqttConf, logL int) *Service {
	var (
		ncpu int
		g    []IGateway
		ctx  = context.Background()
		err  error
		srv  *Service
	)

	if conf.Key == "" {
		srv.logger.Error().Msg("Key is empty. Exiting application.")
		os.Exit(1)
	}
	srv = &Service{
		logger:     zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(logL)).With().Timestamp().Int("pid", os.Getpid()).Logger(),
		controller: c,
		key:        conf.Key,
	}
	ncpu = runtime.NumCPU()

	srv.logger.Info().Int("ncpu", ncpu).Msg("Number of CPUs")
	srv.logger.Info().Msgf("Starting service server: %s, topic: %s \n", conf.Connection, conf.Topic)

	g = make([]IGateway, ncpu)
	for i := 0; i < ncpu; i++ {
		g[i], err = mqtt.NewMqtt(conf, logL, ctx)
		if err != nil {
			srv.logger.Error().Err(err).Msg("Error creating gateway")
		}
	}
	srv.gateWay = g

	return srv
}

// Start initiates the Service by launching a goroutine that continuously processes and forwards messages until the context is canceled.
func (s *Service) Start(ctx context.Context, wg *sync.WaitGroup) {
	var (
		i     *model.Item
		n     time.Duration
		err   error
		b     []byte
		count int
		l     = len(s.gateWay)
	)

	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				for g := range s.gateWay {
					s.gateWay[g].Disconnect()
				}
				wg.Done()
				return
			default:

			}

			i = s.controller.GetMessage()
			if i == nil {
				continue
			}
			s.logger.Debug().Msgf("Received message: %s", i.Data)

			b, err = processMsg(i.Data, s.key)
			if err != nil {
				s.logger.Error().Err(err).Msg("Error processing message")

			} else {
				go s.gateWay[count%l].Forward(b)

				n = time.Now().Sub(i.Rcv)
				s.logger.Info().Int("count", count).Msgf("Forwarded message: in %d", n.Milliseconds())
				count++
			}

		}
	}()

}

// processMsg takes a JSON byte slice and a key string, unmarshals the JSON, retrieves the value for the key, and returns its JSON encoding.
func processMsg(b []byte, key string) ([]byte, error) {
	var (
		data map[string]interface{}
		out  []byte
		err  error
	)

	data = make(map[string]interface{})

	err = json.Unmarshal(b, &data)
	if err != nil {
		return []byte{}, err
	}
	v, ok := data[key]
	if !ok {
		return []byte{}, nil
	}
	if v == nil {
		return []byte{}, nil
	}
	out, err = json.Marshal(v)
	if err != nil {
		return []byte{}, err
	}
	return out, nil
}
