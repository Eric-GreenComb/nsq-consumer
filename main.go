package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	nsq "github.com/nsqio/go-nsq"
	"github.com/spf13/viper"
)

// NsqConfig NsqConfig
var NsqConfig NsqConfigStruct

// NsqConfigStruct NsqConfigStruct
type NsqConfigStruct struct {
	Host string
}

func init() {
	readConfig()
	initConfig()

}

func readConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.ReadInConfig()
}

func initConfig() {
	NsqConfig.Host = viper.GetString("nsq.host")
}

// Message Message
type Message struct {
	Func   string   `form:"func" json:"func"`     //
	Params []string `form:"params" json:"params"` //
}

// HandleJSONMessage HandleJSONMessage
func HandleJSONMessage(message *nsq.Message) error {

	var msg Message

	err := json.Unmarshal([]byte(message.Body), &msg)
	if err != nil {
		return err
	}

	info := "HandleJsonMessage get a result\n"
	info += "function: " + msg.Func + " \n"
	info += fmt.Sprintf("result: %v\n", msg.Params)

	fmt.Println(info)

	return nil
}

// HandleStringMessage HandleStringMessage
func HandleStringMessage(message *nsq.Message) error {

	fmt.Printf("HandleStringMessage get a message  %v\n\n", string(message.Body))
	return nil
}

// MakeConsumer MakeConsumer
func MakeConsumer(topic, channel string, config *nsq.Config,
	handle func(message *nsq.Message) error) {
	consumer, _ := nsq.NewConsumer(topic, channel, config)
	consumer.AddHandler(nsq.HandlerFunc(handle))

	// 待深入了解
	// 連線到 NSQ 叢集，而不是單個 NSQ，這樣更安全與可靠。
	// err := q.ConnectToNSQLookupd("127.0.0.1:4161")

	err := consumer.ConnectToNSQD(NsqConfig.Host)
	if err != nil {
		log.Panic("Could not connect")
	}
}

func main() {

	config := nsq.NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 20 * time.Millisecond
	config.LookupdPollInterval = 1000 * time.Millisecond
	config.RDYRedistributeInterval = 1000 * time.Millisecond
	config.MaxInFlight = 2500

	MakeConsumer("Topic_string", "ch", config, HandleStringMessage)
	MakeConsumer("Topic_json", "ch", config, HandleJSONMessage)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	fmt.Println("exit")

}
