package mongo

import (
	"encoding/json"

	"gopkg.in/mgo.v2"
)

func (m *Mongo) ImportCollection(dbName, collection, jsonText string) (*mgo.Collection, error) {
	var obj map[string]interface{}
	err := json.Unmarshal([]byte(jsonText), &obj)
	if err != nil {
		return nil, err
	}
	coll := m.Session.DB(dbName).C(collection)
	if err := coll.Insert(obj); err != nil {
		return nil, err
	}
	return coll, nil
}
