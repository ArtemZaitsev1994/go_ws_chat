package main

import (
	// "os"
	"context"
	"fmt"
	"net/http"
	"github.com/gorilla/mux"
    "golang.org/x/net/websocket"
	// "github.com/gorilla/sessions"
	// "github.com/streadway/amqp"
	// "github.com/go-redis/redis"
    // "strconv"
    // "strings"
    // "encoding/hex"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    // "go.mongodb.org/mongo-driver/mongo/readpref"
)


type (
	Msg struct {
		clientData ClientData
		text       string
	}

	NewClientEvent struct {
		clientData ClientData
		msgChan    chan *Msg
	}

	JSMess struct {
		MType       string `json:"type"`
		MText       string `json:"msg"`
		Sender      string `json:"from"`
		Company     string `json:"company_id"`
		ToUser      string `json:"to_user"`
		ToUserLogin string `json:"to_user_login"`
		ChatName    string `json:"chat_name"`
	}

	ClientData struct {
		SelfId    string `json:"self_id"`
		CompanyId string `json:"company_id"`
	}
)

var (
	clientRequests    = make(chan *NewClientEvent, 100)
	clientDisconnects = make(chan string, 100)
	messages          = make(chan *Msg, 100)
	notifications     = make(chan *Msg, 100)
)

func routeEvents() {
	// clients := make(map[string]chan *Msg)
	rooms := make(map[ClientData][]chan *Msg)
	notes := make(map[string][]chan *Msg)

	for {
		select {
		case req := <-clientRequests:
			rooms[req.clientData] = append(rooms[req.clientData], req.msgChan)
			notes[req.clientData.SelfId] = append(notes[req.clientData.SelfId], req.msgChan) 
			fmt.Println("Websocket connected: " + req.clientData.SelfId)
		case clientData := <-clientDisconnects:
			// close(rooms[clientData])
			// delete(rooms, clientData)
			fmt.Println("Websocket disconnected: " + clientData)

		case msg := <-messages:
			for _, msgChan := range rooms[msg.clientData] {
				fmt.Println(msg)
				msgChan <- msg
			}
		case note := <-notifications:
			for _, noteChan := range notes[note.clientData.SelfId] {
				fmt.Println(note)
				noteChan <- note
			}
		}
	}
}

func WsChat(ws *websocket.Conn) {

	var clientData ClientData
	websocket.JSON.Receive(ws, &clientData)

	msgChan := make(chan *Msg, 100)
	// clientKey := strings.SplitAfterN(ws.Request().URL.Path, "ws_chat/", 2)[1]
	// _, err := hex.DecodeString(clientKey)
	// FailOnError(err, "Wrong URL")

	clientRequests <- &NewClientEvent{clientData, msgChan}
	defer func() { clientDisconnects <- clientData.SelfId }()

	go func() {
		for msg := range msgChan {
			ws.Write([]byte(string(msg.text)))
		}
	}()

	for {
		var messData JSMess
		// var data map[string]interface{}
		// var data string
		websocket.JSON.Receive(ws, &messData)
		fmt.Println(messData)

		// buf := make([]byte, lenBuf)
		// _, err := ws.Read(buf)

		// if err != nil {
		// 	fmt.Println("Could not read  bytes: ", err.Error())
		// 	return
		// }
		fmt.Println(messData)
		fmt.Println("~~~~~~~~~~~~")
		switch mtype := messData.MType; mtype {
		case "chat_mess":
			messages <- &Msg{clientData, messData.MText}
		case "notification":
			notifications <- &Msg{clientData, messData.MText}
		default:
			fmt.Println("Undefined message type.")
		}
	}
}

func main() {
	// -----------REDIS--------------
	// client := redis.NewClient(&redis.Options{
	// 	Addr:     "localhost:6379",
	// 	Password: "", // no password set
	// 	DB:       0,  // use default DB
	// })

	// pong, err := client.Ping().Result()
	// fmt.Println(pong, err)
	// Output: PONG <nil>
	// -------------------------------

	// -----------MONGO----------------
	// Create client
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	FailOnError(err, "Client creation failed")

	options := options.Find()
	options.SetLimit(2)
	filter := bson.M{}

	// Create connect
	err = client.Connect(context.TODO())
	FailOnError(err, "Connection to Mongo failed")

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	FailOnError(err, "Ping to Mongo failed")

	fmt.Println("Connected to MongoDB!")
	collection := client.Database("chat").Collection("company")

	cur, err := collection.Find(context.TODO(), filter, options)
	FailOnError(err, "Creation cursor failed")

	for cur.Next(context.TODO()) {

	    // create a value into which the single document can be decoded
	    var elem map[string]interface{}
	    err := cur.Decode(&elem)
		FailOnError(err, "Creation cursor failed")

	    fmt.Println(elem)
	}
	FailOnError(err, "Creation cursor failed")

	// Close the cursor once finished
	cur.Close(context.TODO())

	// -----------MONGOEND----------------

	go routeEvents()
	router := mux.NewRouter().StrictSlash(true)

	router.Handle("/go/ws_chat", websocket.Handler(WsChat))
	router.HandleFunc("/ping/", func (w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello Maksim!")
	})

	http.Handle("/", router)





	// router.HandleFunc("/go/ws_chat/{user_id}", func (w http.ResponseWriter, r *http.Request) {
	// 	fmt.Println("Hello World!")
	// 	vars := mux.Vars(r)
 //    	user_id := vars["user_id"]
 //    	fmt.Println(user_id)

	// 	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	// 	ws, err := upgrader.Upgrade(w, r, nil)
	// 	FailOnError(err, "Failed to upgrade to ws")
	// 	defer ws.Close()

	// 	err = client.SAdd("online", user_id, 0).Err()
	// 	FailOnError(err, "Failed to add ws to redis")

	// 	for {
	// 		_, p, err := ws.ReadMessage()
	// 		FailOnError(err, "Fail to read mess from ws")

	// 		fmt.Fprintln(os.Stdout, string(p))

	// 		// err = ws.WriteMessage(messageType, p)
	// 		// FailOnError(err, "Fail to send mess to ws")

	// 		err = rabbit_chat_queue.Publish(
	// 			"",     // exchange
	// 			q_name, // routing key
	// 			false,  // mandatory
	// 			false,  // immediate
	// 			amqp.Publishing {
	// 				ContentType: "text/plain",
	// 			    Body:        []byte(p),
	// 		})
	// 		FailOnError(err, "Failed to publish a message")
	// 	}

	// 	err = client.SRem("online", user_id).Err()
	// 	FailOnError(err, "Failed to remove ws from redis")
	// })


	http.ListenAndServe("localhost:8081", nil)
}
