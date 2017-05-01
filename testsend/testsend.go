package main

import (
	"encoding/json"
	"flag"
	"github.com/nats-io/go-nats"
	"os"
)

func main() {

	natsUrl := flag.String("nats", "nats://zuul:4222", "url to nats server as in nats://host:port")

	flag.Parse()
	if "" == *natsUrl {
		flag.Usage()
		os.Exit(1)
	}

	nc, err := nats.Connect(*natsUrl)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 40; i++ {

		mm := make(map[string]interface{})
		mm["dataIndex"] = i

		md, err := json.Marshal(mm)
		if err != nil {
			panic(err)
		}

		nc.Publish("es.logstash", md)

	}

}
