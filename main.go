package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Go-routine-4595/oem-converter/adapters/controller"
	event_hub "github.com/Go-routine-4595/oem-converter/adapters/gateway/event-hub"
	"github.com/Go-routine-4595/oem-converter/adapters/gateway/mqtt"
	"github.com/Go-routine-4595/oem-converter/service"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

const (
	configOpt              = "/opt/oem-util/config.yaml"
	configLocal            = "config.yaml"
	version                = 0.2
	connectionTypeMqtt     = "mqtt"
	connectionTypeEventHub = "event-hub"
)

var CompileDate string
var logLevelStrint map[int]string = map[int]string{
	-2: "trace",
	-1: "debug",
	0:  "info",
	1:  "warn",
	2:  "error",
	3:  "fatal",
	4:  "panic",
	5:  "disabled",
}

type AdapterConfig struct {
	Connection string `yaml:"Connection"`
	Topic      string `yaml:"Topic"`
	Key        string `yaml:"Key"`
	Type       string `yaml:"Type"`
}

type Config struct {
	Controller controller.MqttConf `yaml:"ControllerConfig"`
	Gateway    AdapterConfig       `yaml:"AdapterConfig"`
	LogLevel   int                 `yaml:"LogLevel"`
}

func main() {
	var (
		args   []string
		conf   Config
		ctx    context.Context
		c      *controller.Controller
		svc    *service.Service
		wg     *sync.WaitGroup
		cancel context.CancelFunc
		err    error
		ncpu   int
		gtw    []service.IGateway
		typeG  string
	)

	fmt.Printf("oem converter Version: %.2f \t CompileDate: %s \t LogLevel: %s \n", version, CompileDate, logLevelStrint[conf.LogLevel])
	wg = &sync.WaitGroup{}
	args = os.Args

	// default config file
	if len(args) == 1 {
		conf, err = openConfigFile(configOpt)
		if err != nil {
			conf, err = openConfigFile(configLocal)
			if err != nil {
				fmt.Println("No config file found")
				processError(err)
			}
			fmt.Println("Config file found: ", configLocal)
		} else {
			fmt.Println("Config file found: ", configOpt)
		}
	} else {
		conf, err = openConfigFile(args[1])
		if err != nil {
			fmt.Println("No config file found")
			processError(err)
		}
		fmt.Println("Config file found: ", args[1])
	}

	ctx, cancel = context.WithCancel(context.Background())

	c = controller.NewController(conf.Controller, ctx, conf.LogLevel, wg)
	svc = service.NewService(c, conf.Gateway.Key, conf.LogLevel, wg)

	ncpu = runtime.NumCPU()
	gtw = make([]service.IGateway, ncpu)

	typeG, err = AdapterFromConnection(conf.Gateway)
	if err != nil {
		log.Fatal().Err(err).Msg("Error creating gateway")
	}

	if typeG == connectionTypeEventHub {
		var ehConfig event_hub.Config

		ehConfig.Connection = conf.Gateway.Connection
		ehConfig.NameSpace = conf.Gateway.Topic
		ehConfig.LogLevel = conf.LogLevel

		for i := 0; i < ncpu; i++ {
			gtw[i], err = event_hub.NewEventHub(ehConfig, ctx, wg)
			if err != nil {
				log.Fatal().Err(err).Msg("Error creating gateway")
			}
		}
		svc.Gateway(gtw)
	}

	if typeG == connectionTypeMqtt {
		var mqttConfig mqtt.Config

		mqttConfig.Connection = conf.Gateway.Connection
		mqttConfig.Topic = conf.Gateway.Topic
		mqttConfig.Key = conf.Gateway.Key

		for i := 0; i < ncpu; i++ {
			gtw[i], err = mqtt.NewMqtt(mqttConfig, ctx)
			if err != nil {
				log.Fatal().Err(err).Msg("Error creating gateway")
			}
		}
		svc.Gateway(gtw)
	}

	svc.Start(ctx)

	// Graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
	wg.Wait()
	log.Info().Msg("Shutting down")
	os.Exit(0)
}

func AdapterFromConnection(conf AdapterConfig) (string, error) {
	if conf.Type == connectionTypeMqtt {
		return connectionTypeMqtt, nil
	}
	if conf.Type == connectionTypeEventHub {
		return connectionTypeEventHub, nil
	}
	return "", errors.New("invalid connection string")
}

func openConfigFile(s string) (Config, error) {
	var config Config

	if s == "" {
		s = "config.yaml"
	}

	f, err := os.Open(s)
	if err != nil {
		return config, errors.Join(err, errors.New("open config.yaml file"))
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		processError(err)
	}
	return config, nil

}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}
