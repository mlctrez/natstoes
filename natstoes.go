package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/nats-io/go-nats"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var natsUrl = flag.String("nats", "nats://zuul:4222", "url to nats server like nats://host:port")
var nc *nats.Conn
var esc *elastic.Client
var msgChan = make(chan *nats.Msg, 1000)
var indexes = make(map[string]bool)
var indexesLock = &sync.Mutex{}

const esTimestamp = "2006-01-02T15:04:05.999Z"

func initClients() {
	var err error
	nc, err = nats.Connect(*natsUrl)
	if err != nil {
		panic(err)
	}
	esc, err = elastic.NewClient()
	if err != nil {
		panic(err)
	}
}

func main() {
	log.SetOutput(os.Stdout)
	flag.Parse()
	initClients()

	nc.ChanSubscribe("es.>", msgChan)

	indexRequests := make([]elastic.BulkableRequest, 0)
	indexList := make([]string, 0)
	for {
		select {
		case m := <-msgChan:
			indexName := m.Subject[3:]
			indexTimestamp := indexName + "-2006.01.02"

			mdata := make(map[string]interface{})
			if err := json.Unmarshal(m.Data, &mdata); err != nil {
				mdata["message"] = string(m.Data)
			}

			tNow := time.Now().UTC()
			mdata["@timestamp"] = tNow.Format(esTimestamp)

			index := tNow.Format(indexTimestamp)
			indexList = append(indexList, index)
			bir := elastic.NewBulkIndexRequest()
			bir.Index(index)
			bir.Type("data")
			bir.Doc(mdata)

			indexRequests = append(indexRequests, bir)

		case <-time.After(time.Second * 1):
			if len(indexRequests) > 0 {
				go sendBulk(esc.Bulk().Add(indexRequests...), indexList)
				indexRequests = make([]elastic.BulkableRequest, 0)
				indexList = make([]string, 0)
			}
		}
	}

}

func createIndex(indexName string) error {
	indexesLock.Lock()
	defer indexesLock.Unlock()

	if _, ok := indexes[indexName]; ok {
		return nil
	}
	_, err := esc.CreateIndex(indexName).Do(context.Background())

	if err != nil && strings.Contains(err.Error(), "Error 400") {
		err = nil
	}

	if err == nil {
		indexes[indexName] = true
	}
	return err
}

func sendBulk(b *elastic.BulkService, indexes []string) {
	for _, id := range indexes {
		if err := createIndex(id); err != nil {
			log.Println(err)
		}
	}
	b.Do(context.Background())
}
