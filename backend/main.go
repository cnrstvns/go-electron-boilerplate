package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
)

func mainSocket(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print("Started Main Socket")

	bkg := context.Background()
	mainSock := zmq4.NewPull(bkg)
	err := mainSock.Listen("tcp://*:7777")

	if err != nil {
		log.Fatalf("Could not listen to %v", err)
	}

	for {
		msg, err := mainSock.Recv()
		if err != nil {
			log.Fatalf("Could not receive %v", err)
		}

		log.Printf("Message: %v", string(msg.Frames[0]))
	}
}

func dataSocket(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print("Started Data Socket")

	bkg := context.Background()
	dataSock := zmq4.NewRep(bkg)
	err := dataSock.Listen("tcp://*:7778")

	if err != nil {
		log.Fatalf("Could not listen to %v", err)
	}

	for {
		msg, err := dataSock.Recv()
		if err != nil {
			log.Fatalf("Could not receive %v", err)
		}
		log.Printf("Message: %v", string(msg.Frames[0]))
		var d messageData
		decodeErr := json.Unmarshal(msg.Frames[0], &d)

		if decodeErr != nil {
			log.Fatalf("Could not decode %v", decodeErr)
		}

		fmt.Printf("%+v\n", d)
		fmt.Printf("Message Type: %+v\n", d["type"])
		fmt.Printf("Message Data: %+v\n", d["data"])
		msgData := zmq4.NewMsgString(string(msg.Frames[0]))
		dataSock.Send(msgData)
	}
}

func statusSocket(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print("Started Status Socket")

	bkg := context.Background()
	statusSock := zmq4.NewPush(bkg)
	err := statusSock.Listen("tcp://*:7779")
	if err != nil {
		log.Fatalf("Could not listen to %v", err)
	}

	for {
		msgData := zmq4.NewMsgString(`{"type": "testing"}`)
		statusSock.Send(msgData)
		time.Sleep(5 * time.Second)
	}
}

type messageData map[string]interface{}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go mainSocket(&wg)
	wg.Add(1)
	go dataSocket(&wg)
	wg.Add(1)
	go statusSocket(&wg)
	wg.Wait()
}
