package main

import (
	"encoding/json"
	"flag"
	"github.com/nats-io/go-nats"
	"log"
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

	for i := 0; i < 600; i++ {

		mm := make(map[string]interface{})
		mm["dataIndex"] = i
		mm["otherdata"] = "otherdata"

		md, err := json.Marshal(mm)
		if err != nil {
			panic(err)
		}

		err = nc.Publish("es.logstash", md)
		if err != nil {
			log.Println(err)
		}
	}
	nc.Flush()
	nc.Close()

}
