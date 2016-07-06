package main

import (
	"fmt"
	"io"
	"time"

	flags "github.com/jessevdk/go-flags"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var CommandArgs struct {
	MongoDBURL string   `long:"url" description:"MongoDB URL" required:"true"`
	Namespaces []string `long:"ns" description:"Which database and collection to monitor"`
	Since      int32    `long:"since" description:"The MongoDB Timestamp to start to monitor"`
	Ordinal    int32    `long:"ordinal" description:"The ordinal operation on the specified MongoDB Timestamp to start to monitor"`
	FastStop   bool     `long:"fast-stop" description:"Exit for MongoDB Tail Timeout"`
}

func main() {
	var (
		oplog *mgo.Collection
		err   error
	)
	_, err = flags.Parse(&CommandArgs)
	if err != nil {
		return
	}

	for {
		oplog, err = getMongoDBCollection()
		if err != nil {
			panic(err)
		}

		err = run(oplog)
		if err != nil {
			if err == io.EOF { // Maybe the primary/slave switch
				continue
			}
			panic(err)
		}
	}
}

func run(oplog *mgo.Collection) error {
	var (
		query  bson.M
		result bson.M
		iter   *mgo.Iter
		sec    bson.MongoTimestamp
		ord    bson.MongoTimestamp
		err    error
	)
	if CommandArgs.Since > 0 {
		sec = bson.MongoTimestamp(CommandArgs.Since)
		ord = bson.MongoTimestamp(CommandArgs.Ordinal)
		query = bson.M{"ts": bson.M{"$gt": sec<<32 + ord}}
	}
	if len(CommandArgs.Namespaces) > 0 {
		query["ns"] = bson.M{"$in": CommandArgs.Namespaces}
	}
	iter = oplog.Find(query).Tail(1 * time.Second)
	for {
		for iter.Next(&result) {
			ts, ok := result["ts"].(bson.MongoTimestamp)
			if ok {
				query["ts"].(bson.M)["$gt"] = ts
				fmt.Printf("new op log: %v\n", result)
			} else {
				panic(fmt.Sprintf("`ts` is not found in result: %v\n", result))
			}
		}
		err = iter.Err()
		if err != nil {
			iter.Close()
			return err
		}
		if iter.Timeout() {
			if CommandArgs.FastStop {
				iter.Close()
				return nil
			}
			continue
		}
		iter = oplog.Find(query).Tail(1 * time.Second)
	}
}

func getMongoDBCollection() (*mgo.Collection, error) {
	session, err := mgo.Dial(CommandArgs.MongoDBURL)
	if err != nil {
		return nil, err
	}
	session.SetMode(mgo.SecondaryPreferred, true)
	database := session.DB("local")
	collection := database.C("oplog.rs")
	return collection, nil
}
