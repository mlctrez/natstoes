package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/nats-io/go-nats"
	"github.com/satori/go.uuid"
	"gopkg.in/olivere/elastic.v5"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {

	natsUrl := flag.String("nats", "", "url to nats server as in nats://host:port")

	flag.Parse()
	if "" == *natsUrl {
		flag.Usage()
		os.Exit(1)
	}

	esc, err := NewEsClient()
	if err != nil {
		panic(err)
	}

	nc, err := nats.Connect(*natsUrl)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	nc.Subscribe("es.>", func(m *nats.Msg) {
		mdata := make(map[string]interface{})
		if err = json.Unmarshal(m.Data, &mdata); err != nil {
			mdata["message"] = string(m.Data)
		}
		esc.PostMessage(m.Subject[3:], mdata)
	})

	wg.Wait()
}

type Client struct {
	esClient    *elastic.Client
	indexes     map[string]bool
	indexesLock *sync.Mutex
}

func NewEsClient() (client *Client, err error) {
	c, err := elastic.NewClient()
	if err != nil {
		return nil, err
	}
	im := make(map[string]bool)
	return &Client{
		esClient:    c,
		indexes:     im,
		indexesLock: &sync.Mutex{},
	}, nil
}

func (client *Client) CreateIndex(indexName string) error {
	client.indexesLock.Lock()
	defer client.indexesLock.Unlock()

	if _, ok := client.indexes[indexName]; ok {
		return nil
	}
	_, err := client.esClient.CreateIndex(indexName).Do(context.Background())

	if err != nil && strings.Contains(err.Error(), "Error 400") {
		err = nil
	}

	if err == nil {
		client.indexes[indexName] = true
	}
	return err
}

func (client *Client) PostMessage(indexName string, message interface{}) error {

	esTimestamp := "2006-01-02T15:04:05.999Z"
	indexTimestamp := indexName + "-2006.01.02"

	tNow := time.Now().In(time.UTC)

	index := tNow.Format(indexTimestamp)

	if err := client.CreateIndex(index); err != nil {
		return err
	}

	tdb, err := json.Marshal(message)
	if err != nil {
		return err
	}

	td := make(map[string]interface{})

	err = json.Unmarshal(tdb, &td)
	if err != nil {
		return err
	}

	td["@timestamp"] = tNow.Format(esTimestamp)

	_, err = client.esClient.Index().
		Index(index).
		Type("data").
		Id(uuid.NewV4().String()).
		BodyJson(td).
		Refresh("true").
		Do(context.Background())

	return err

}