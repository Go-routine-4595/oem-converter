package service

import (
	"context"
	"encoding/json"
	"github.com/Go-routine-4595/oem-converter/model"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"time"
)

type IGateway interface {
	Forward(b []byte) error
	Disconnect()
}

type IController interface {
	GetMessage() *model.Item
}

type Service struct {
	controller IController
	gateWay    []IGateway
	logger     zerolog.Logger
	key        string
	wg         *sync.WaitGroup
}

// NewService initializes a new Service instance with the specified IController, MqttConf configuration, and log level.
// It sets up the logging, creates MQTT gateways for each CPU core, and returns the created Service instance.
func NewService(c IController, key string, logL int, wg *sync.WaitGroup) *Service {
	var (
		srv *Service
	)

	if key == "" {
		srv.logger.Error().Msg("Key is empty. Exiting application.")
		os.Exit(1)
	}
	return &Service{
		logger:     zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(logL)).With().Timestamp().Int("pid", os.Getpid()).Logger(),
		controller: c,
		key:        key,
		wg:         wg,
	}
}

func (s *Service) Gateway(gtw []IGateway) {
	s.gateWay = gtw
}

// Start initiates the Service by launching a goroutine that continuously processes and forwards messages until the context is canceled.
func (s *Service) Start(ctx context.Context) {
	var (
		i     *model.Item
		n     time.Duration
		err   error
		b     [][]byte
		count int
		l     = len(s.gateWay)
	)

	s.wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				//for g := range s.gateWay {
				//	s.gateWay[g].Disconnect()
				//}
				time.Sleep(500 * time.Millisecond)
				s.logger.Warn().Msgf(
					"Service stopped. Received %d messages",
					count,
				)
				s.wg.Done()
				return
			default:

			}

			i = s.controller.GetMessage()
			if i == nil {
				continue
			}
			s.logger.Debug().Msgf("Received message: %s", i.Data)

			b, err = splitMessage(i.Data)
			if err != nil {
				s.logger.Error().Err(err).Msg("Error processing message")

			} else {
				for j := 0; j < len(b); j++ {

					go s.gateWay[count%l].Forward(b[j])

					n = time.Now().Sub(i.Rcv)
					s.logger.Info().Int("count", count).Msgf("Forwarded message: in %d", n.Milliseconds())
					count++
				}
			}

		}
	}()

}

func splitMessage(b []byte) ([][]byte, error) {
	var (
		data []map[string]interface{}
		err  error
		out  [][]byte
	)
	data = make([]map[string]interface{}, 0)
	err = json.Unmarshal(b, &data)
	if err != nil {
		return [][]byte{}, err
	}

	out = make([][]byte, len(data))
	for i := 0; i < len(data); i++ {
		out[i], err = json.Marshal(data[i])
		if err != nil {
			return [][]byte{}, err
		}
	}
	return out, nil
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
