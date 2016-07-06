package main

import (
	"fmt"
	"time"

	flags "github.com/jessevdk/go-flags"
	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

var CommandArgs struct {
	MongoDBURL *string `long:"url" description:"MongoDB URL" required:"true"`
	Only       *string `long:"only" description:"Which database and collection to monitor"`
	Since      *int64  `long:"since" description:"The MongoDB Timestamp to start to monitor"`
	FastStop   bool    `long:"fast-stop" description:"Exit for MongoDB Tail Timeout"`
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

	oplog, err = getMongoDBCollection()
	if err != nil {
		panic(err)
	}

	err = run(oplog)
	if err != nil {
		panic(err)
	}
}

func run(oplog *mgo.Collection) error {
	var (
		query  bson.M
		result bson.M
		iter   *mgo.Iter
		err    error
	)
	if CommandArgs.Since != nil {
		query = bson.M{"ts": bson.M{"$gt": bson.MongoTimestamp(*CommandArgs.Since)}}
	}
	if CommandArgs.Only != nil {
		query["ns"] = *CommandArgs.Only
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
	session, err := mgo.Dial(*CommandArgs.MongoDBURL)
	if err != nil {
		return nil, err
	}
	session.SetMode(mgo.Strong, true)
	database := session.DB("local")
	collection := database.C("oplog.rs")
	return collection, nil
}
