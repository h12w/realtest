package elasticsearch

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"

	"h12.me/realtest/container"
)

const (
	internalPort = 9200
)

var (
	containerName = container.ContainerName{
		Name: "realtest-elasticsearch-a6934e21d2084f6d8a97e22220ca105b",
	}
)

type ElasticSearch struct {
	client http.Client
	c      *container.Container
}

func RandomIndexName() string {
	return "idx_" + strconv.Itoa(rand.Int())
}

func New() (*ElasticSearch, error) {
	c, err := containerName.FindOrCreate("elasticsearch:latest")
	if err != nil {
		return nil, err
	}

	return &ElasticSearch{
		c: c,
	}, nil
}

func (m *ElasticSearch) DeleteIndex(indexName string) error {
	req, err := http.NewRequest("DELETE", "http://"+m.Addr()+"/"+indexName, nil)
	if err != nil {
		return err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		return errors.New(strconv.Itoa(resp.StatusCode) + ": " + string(errMsg))
	}
	return nil
}

func (s *ElasticSearch) Addr() string {
	return s.c.Addr(internalPort)
}

func (m *ElasticSearch) DumpIndex(indexName string) (string, error) {
	uri := "http://" + m.Addr() + "/" + indexName + "/_search"
	resp, err := m.client.Get(uri)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	return string(data), err
}
