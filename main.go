package main

import (
	"Go-routine-4595/oem-converter/adapters/controller"
	"Go-routine-4595/oem-converter/adapters/gateway/mqtt"
	"Go-routine-4595/oem-converter/service"
	"context"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	configOpt   = "/opt/oem-util/config.yaml"
	configLocal = "config.yaml"
	version     = 0.1
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

type Config struct {
	Controller controller.MqttConf `yaml:"ControllerConfig"`
	Gateway    mqtt.MqttConf       `yaml:"AdapterConfig"`
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
	svc = service.NewService(c, conf.Gateway, conf.LogLevel)
	svc.Start(ctx, wg)

	// Graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
	wg.Wait()
	log.Println("Shutting down")
	os.Exit(0)
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
