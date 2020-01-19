package main

import (
	"os"
	"context"
	"fmt"
	"strconv"
	"net/http"
	"github.com/gorilla/mux"
    "golang.org/x/net/websocket"
    "time"
    // "reflect"
	// "github.com/gorilla/sessions"
	// "github.com/streadway/amqp"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
    // "strconv"
    "strings"
    // "encoding/hex"
    "encoding/json"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    // "go.mongodb.org/mongo-driver/mongo/readpref"
)


type (
	// Сообщение чата
	Msg struct {
		clientData ClientData
		messData   JSMess
	}

	// Данные нового подключения
	NewClientEvent struct {
		clientData ClientData
		Channels   Ch
	}

	// Каналы
	Ch struct {
		msgChan   chan *MessToCompany
		noteChan  chan *Notification
	}

	// Структура для парсинга сообщения чата
	JSMess struct {
		MType       string `json:"type"`
		SubType     string `json:"subtype"`
		MText       string `json:"msg"`
		Sender      string `json:"from"`
		SenderLogin string `json:"from_login"`
		Company     string `json:"company_id"`
		CompanyName string `json:"company"`
		ToUser      string `json:"to_user"`
		ToUserLogin string `json:"to_user_login"`
		ChatName    string `json:"chat_name"`

	}

	// Данные подключившегося пользователя
	ClientData struct {
		SelfId    string     `json:"self_id"`
		SelfLogin string     `json:"login"`
		CompanyId string     `json:"company_id"`
		wsUuid    uuid.UUID
	}

	// Структура для записи сообщения в Mongo
	Message struct {
        FromUser   string             `bson:"from_user"`
        Msg        string             `bson:"msg"`
        InsertTime time.Time          `bson:"time"`
        CompanyId  string             `bson:"company_id"`
        UserName   string             `bson:"user_name"`
	}

	// Для чтения из монго
	MessWithId struct {
        FromUser   string             `bson:"from_user"`
        Msg        string             `bson:"msg"`
        InsertTime time.Time          `bson:"time"`
        CompanyId  string             `bson:"company_id"`
        UserName   string             `bson:"user_name"`
		MessageId  primitive.ObjectID `bson:"_id"`
	}

	// Структура сообщения внутри комнаты отправляемого в клиента
	MessToCompany struct {
        FromUser      string    `json:"from_id"`
        Msg           string    `json:"msg"`
        InsertTime    time.Time `json:"time"`
        CompanyId     string    `json:"company_id"`
    	FromUserLogin string    `json:"from"`
    	Type          string    `json:"type"`  
	}

	// Структура оповещения
	Notification struct {
		Text        string `json:"text"`
		Type        string `json:"type"`
		UserID      string `json:"user_id"`
		// UserLogin   string `json:"user_login"`
		Company     string `json:"company_id"`
		FromUser    string `json:"from"`
		CompanyName string `json:"company"`
		SubType     string `json:"subtype"`
		ToUser      string `json:"to_user"`
	}

	NotifInnerStruct struct {
		note       *Notification
		ForCompany string
		wsUuid     uuid.UUID
		Users      []string
	}

	// Структура Компании из базы
	BSCompany struct {
        Users []string `bson:"users"`
	}

	// Структура маркеров непрочитанных сообщений
	UnreadMessage struct {
		MsgID     string   `bson:"msg_id"`
		ToCompany string   `bson:"to_company"`
		ToUser    string   `bson:"to_user"`
		Count     int      `bson:"count"`
	}

	// Сообщение из Redis
	RedisMess struct {
		CompanyId   string `json:"company_id"`
		CompanyName string `json:"company_name"`
		// Type        string `json:"type"`
		UserName    string `json:"self_login"`
		Payload     string `json:"payload"`
	}
)


var (
	// Канал подключений
	clientRequests    = make(chan *NewClientEvent, 100)

	// Канал отключений
	clientDisconnects = make(chan *ClientData, 100)

	// канал сообщений
	messages          = make(chan *MessToCompany, 100)

	// канал оповещений
	notifications     = make(chan *NotifInnerStruct, 100)

	db       *mongo.Database
	redis_cl *redis.Client

	REDIS_HOST = "redis:6379"
	MONGO_HOST = "mongodb://mongodb:27017"
)

const (
	// Типы сообщений
	NEW_USER_IN_COMPANY = "new_user_in_company"
	NEW_EVENT           = "new_event"
	NEW_MESS            = "new_mess"
	CHAT_MESS           = "chat_mess"
	JOINED              = "joined"
	CLOSED              = "closed"
	PART                = "part"
	READ_UNREAD         = "unread"

	// Еще какие-то типы... не помню
	NOTIFICATION         = "notification"
	COMPANY_CHANNEL_NAME = "company"
	EVENT_CHANNEL_NAME   = "events"

	PER_REQ = 20
)


func getMessPart(companyId string, msgId string, mess_c *mongo.Collection) []MessWithId {
	var result []MessWithId
	opts := options.Find().SetSort(bson.D{{"time", -1}}).SetLimit(PER_REQ)
	cursor, err := mess_c.Find(context.TODO(), bson.D{{"company_id", companyId}}, opts)
	FailOnError(err, "Failed while getting messages from MongoDB")

	err = cursor.All(context.TODO(), &result)
	FailOnError(err, "Failed")


	return result
}

func getUnreadCount(companyId string, unr_c *mongo.Collection) int {
	var unread []UnreadMessage
	opts := options.Find().SetSort(bson.D{{"count", -1}})
	cursor, err := unr_c.Find(context.TODO(), bson.D{{"to_company", companyId}}, opts)
	FailOnError(err, "Failed")

	err = cursor.All(context.TODO(), &unread)
	if len(unread) == 0 {
		return 0
	}

	return unread[0].Count
}

func getInitData(companyId, userId string, unr_c, mess_c *mongo.Collection) (map[string]interface{}, string) {
	last_mess_id := "1"
	old_messages := getMessPart(companyId, last_mess_id, mess_c)
	if len(old_messages) > 0 {
		last_mess_id = old_messages[len(old_messages)-1].MessageId.String()
	}

    unread := getUnreadCount(companyId, unr_c)

	init_data := map[string]interface{}{
		"type": PART,
		"messages": old_messages,
		"unr_count": unread,
	}
	return init_data, last_mess_id
}

func sendJoinMess(cl ClientData, comp_c *mongo.Collection) {
	var company BSCompany
	objID, err := primitive.ObjectIDFromHex(cl.CompanyId)
	FailOnError(err, "Creation ObjectID failed")

	err = comp_c.FindOne(
			context.TODO(),
			bson.D{{"_id", objID}},
			options.FindOne(),
		).Decode(&company)
	FailOnError(err, "Searching company in mongo failed")

	n := Notification{
		// Text:        strings.Join(n_text, ""),
		Type:        NOTIFICATION,
		SubType:     READ_UNREAD,
		UserID:      cl.SelfId,
		// UserLogin:   messData.SenderLogin,
		// CompanyName: messData.CompanyName,
		// FromUser:    messData.SenderLogin,
	}
	notifications <- &NotifInnerStruct{
		note:       &n,
		ForCompany: cl.CompanyId,
		wsUuid:     cl.wsUuid,
		Users:      company.Users,
	}

}

func holdNotifications(redis_cl *redis.Client, ch_name string, mess_type string) {
	pubsub := redis_cl.Subscribe(ch_name)
	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive()
	if err != nil {
		panic(err)
	}
	redis_ch := pubsub.Channel()
	for msg := range redis_ch {
		var r_m RedisMess
		comp_c := db.Collection("company")
		// избавляемся от слешей
		s, _ := strconv.Unquote(string([]byte(msg.Payload)))
		err := json.Unmarshal([]byte(s), &r_m)
		FailOnError(err, "Receive message from redis failed")

		var company BSCompany
		objID, err := primitive.ObjectIDFromHex(r_m.CompanyId)
		FailOnError(err, "Creation ObjectID failed")

		// получаем компанию к которой будет относиться оповещение
		err = comp_c.FindOne(
				context.TODO(),
				bson.D{{"_id", objID}},
				options.FindOne(),
			).Decode(&company)
		FailOnError(err, "Searching company in mongo failed")

		n := Notification{
			Text:        r_m.Payload,
			Type:        NOTIFICATION,
			SubType:     mess_type,
			// UserID:      messData.Sender,
			// UserLogin:   messData.SenderLogin,
			CompanyName: r_m.CompanyName,
			FromUser:    r_m.UserName,
		}
		notifications <- &NotifInnerStruct{note:&n, ForCompany: r_m.CompanyId, Users: company.Users}
	}
}

func routeEvents() {

	// содержит комнаты(компании) с вебсокетами пользователей
	rooms   := make(map[string]map[uuid.UUID]Ch)
	// словарь с сокетами пользователей
	sockets := make(map[string]map[uuid.UUID]Ch)

	note_c := db.Collection("notifications")
	unr_c := db.Collection("unread_message")

	for {
		select {

		// Подключение новых пользователей
		case req := <-clientRequests:
			if req.clientData.CompanyId != "" {
				if rooms[req.clientData.CompanyId] == nil {
					rooms[req.clientData.CompanyId] = make(map[uuid.UUID]Ch)
				}
				rooms[req.clientData.CompanyId][req.clientData.wsUuid] = req.Channels
			}
			if sockets[req.clientData.SelfId] == nil {
				sockets[req.clientData.SelfId] = make(map[uuid.UUID]Ch)
			}
			sockets[req.clientData.SelfId][req.clientData.wsUuid] = req.Channels
			fmt.Println("Websocket connected: " + req.clientData.SelfId)

		// Отключение пользователей
		case clientData := <-clientDisconnects:
			// Закрываем сокеты
			if rooms[clientData.CompanyId][clientData.wsUuid].msgChan != nil {
				close(rooms[clientData.CompanyId][clientData.wsUuid].msgChan)
				close(rooms[clientData.CompanyId][clientData.wsUuid].noteChan)
			} else {
				close(sockets[clientData.SelfId][clientData.wsUuid].noteChan)
			}

			// Удаляем сокеты
			delete(rooms[clientData.CompanyId], clientData.wsUuid)
			delete(sockets[clientData.SelfId], clientData.wsUuid)

			fmt.Println("Websocket disconnected: " + clientData.SelfId)

		// Обработка пришедших сообщений из чата
		case msg := <-messages:
			for _, ch := range rooms[msg.CompanyId] {
				ch.msgChan <- msg
			}

		case note := <-notifications:

			n := note.note

			switch t := note.note.SubType; t {
			case JOINED:
				fmt.Println("JOINED")
		    case NEW_MESS:
				if note.ForCompany != "" {
					for _, user := range note.Users {
						n.ToUser = user
						// сохраняем в базе оповещение
						_, err := note_c.InsertOne(context.TODO(), n)
						FailOnError(err, "Insertion notification failed")
						for _, ch := range sockets[user] {
							ch.noteChan <- note.note
						}
					}
				}
			case NEW_EVENT:
				for _, user := range note.Users {
					n.ToUser = user
					// сохраняем в базе оповещение
					_, err := note_c.InsertOne(context.TODO(), n)
					FailOnError(err, "Insertion notification failed")
					for _, ch := range sockets[user] {
						ch.noteChan <- note.note
					}
				}
			case NEW_USER_IN_COMPANY:
				for _, user := range note.Users {
					n.ToUser = user
					// сохраняем в базе оповещение
					_, err := note_c.InsertOne(context.TODO(), n)
					FailOnError(err, "Insertion notification failed")
					for _, ch := range sockets[user] {
						ch.noteChan <- note.note
					}
				}
			case READ_UNREAD:
				 _, err := unr_c.UpdateOne(
			    	context.TODO(),
			    	bson.D{
						{"to_company", note.note.Company},
						{"to_user", note.ForCompany},
					},
					bson.M{"$set": bson.M{"count": 0}},
					options.Update(),
				)
				FailOnError(err, "Drop unread message failed")

				for _, user := range note.Users {
					for _, ch := range sockets[user] {
						ch.noteChan <- note.note
					}
				}
			}
			// sockets[note.clientData.wsUuid].noteChan <- note
		}
	}
}

func WsChat(ws *websocket.Conn) {
	defer ws.Close()

	// коллекции из базы
	mess_c := db.Collection("messages")
	comp_c := db.Collection("company")
	unr_c  := db.Collection("unread_message")
	note_c := db.Collection("notifications")

	// Данные подключившегося пользователя
	var clientData ClientData
	// Сокеты идентифицируем по uuid
	clientData.wsUuid = uuid.New()
	// Первое сообщение из сокета - данные о пользователе
	websocket.JSON.Receive(ws, &clientData)

	// канал сообщений
	msgChan  := make(chan *MessToCompany, 100)
	// канал оповещений
	noteChan := make(chan *Notification, 100)

	clientRequests <- &NewClientEvent{clientData, Ch{msgChan, noteChan}}
	defer func() { clientDisconnects <- &clientData }()

	init_data, _ := getInitData(clientData.CompanyId, clientData.SelfId, unr_c, mess_c)
	// отправка уже существующих сообщений в чат, инициализация чата
	init_bytes, err := json.Marshal(init_data)
	FailOnError(err, "Cant serialize old messages")
	ws.Write(init_bytes)

	// Оповещаем другие чаты что i'm in
	sendJoinMess(clientData, comp_c)

	// Отправка сообщений в сокет
	go func() {
		for m := range msgChan {
			bytes, err := json.Marshal(m)
			FailOnError(err, "Cant serialize message.")
			ws.Write(bytes)
		}
	}()

	// отправка оповещений в сокет
	go func() {
		for msg := range noteChan {
			bytes, err := json.Marshal(msg)
			FailOnError(err, "Cant serialize message.")
			ws.Write(bytes)
		}
	}()

	// получение сообщений из сокета
	L:
		for {
			var messData JSMess
			websocket.JSON.Receive(ws, &messData)

			switch mtype := messData.MType; mtype {
			case CHAT_MESS:
				companyId := clientData.CompanyId

				m := Message{
					FromUser:   clientData.SelfId,
					Msg:        messData.MText,
					CompanyId:  companyId,
					InsertTime: time.Now(),
					UserName:   clientData.SelfLogin,
				}

				// Запись сообщения в базу
				res, err := mess_c.InsertOne(context.TODO(), m)
				FailOnError(err, "Insertion message failed")

				// Получаем ID компании к которой относится сообщение
				// получаем каждый раз ибо вдруг кто новый залетел
				var company BSCompany
				objID, err := primitive.ObjectIDFromHex(companyId)
				FailOnError(err, "Creation ObjectID failed")

				err = comp_c.FindOne(
						context.TODO(),
						bson.D{{"_id", objID}},
						options.FindOne(),
					).Decode(&company)
				FailOnError(err, "Searching company in mongo failed")

				fmt.Println(messData.CompanyName)
				n_text := []string{
					"Новое сообщение в чате ",
					messData.CompanyName,
					" от ",
					clientData.SelfLogin,
					" \"",
					messData.MText,
					"\"",
				}
				n := Notification{
					Text:        strings.Join(n_text, ""),
					Type:        NOTIFICATION,
					SubType:     NEW_MESS,
					UserID:      messData.Sender,
					// UserLogin:   messData.SenderLogin,
					CompanyName: messData.CompanyName,
					FromUser:    messData.SenderLogin,
				}

				for _, user := range company.Users {
					if clientData.SelfId != user {

						n.ToUser = user
						// сохраняем в базе оповещение
						_, err := note_c.InsertOne(context.TODO(), n)
						FailOnError(err, "Insertion notification failed")

						var u UnreadMessage
						err = unr_c.FindOne(
							context.TODO(),
							bson.D{
								{"to_company", companyId},
								{"to_user", user},
							},
							options.FindOne(),
						).Decode(&u)

						if err != nil {
							unread_m := UnreadMessage{
								MsgID:     res.InsertedID.(primitive.ObjectID).Hex(),
								ToCompany: companyId,
								ToUser:    user,
								Count:     1,
							}
							fmt.Println(unread_m)
							_, err := unr_c.InsertOne(context.TODO(), unread_m)
							FailOnError(err, "Creation unread message failed")

						} else {
					        _, err = unr_c.UpdateOne(
					        	context.TODO(),
					        	bson.D{
									{"to_company", companyId},
									{"to_user", user},
								},
								bson.M{"$inc": bson.M{"count": 1}},
								options.Update().SetUpsert(true),
							)
							FailOnError(err, "Updating unread message failed")
						}
					}
				}

				// Отправка сообщений и оповещений в каналы
				msg := MessToCompany{
					FromUser:      clientData.SelfId,
			        Msg:           messData.MText,
			        InsertTime:    time.Now(),
			        CompanyId:     companyId,
			    	FromUserLogin: clientData.SelfLogin,
			    	Type:          CHAT_MESS,     
				}
				messages      <- &msg
				notifications <- &NotifInnerStruct{
					note:       &n,
					ForCompany: companyId,
					wsUuid:     clientData.wsUuid,
					Users:      company.Users,
				}
			// case "notification":
			// 	notifications <- &NotifInnerStruct{note:&n, ForCompany: companyId, wsUuid: clientData.wsUuid}
			case CLOSED:
				fmt.Println("WS closed.")
				break L
			case READ_UNREAD:
				// Получаем ID компании к которой относится сообщение
				// получаем каждый раз ибо вдруг кто новый залетел
				var company BSCompany
				objID, err := primitive.ObjectIDFromHex(clientData.CompanyId)
				FailOnError(err, "Creation ObjectID failed")

				err = comp_c.FindOne(
						context.TODO(),
						bson.D{{"_id", objID}},
						options.FindOne(),
					).Decode(&company)
				FailOnError(err, "Searching company in mongo failed")
				
				n := Notification{
					Type:        NOTIFICATION,
					SubType:     READ_UNREAD,
					UserID:      messData.Sender,
				}
				notifications <- &NotifInnerStruct{
					note:       &n,
					ForCompany: messData.Company,
					wsUuid:     clientData.wsUuid,
					Users:      company.Users,
				}
			default:
				fmt.Println("Undefined message type.")
				break L
			}
		}
}

func WsCommon(ws *websocket.Conn) {
	defer ws.Close()

	// Данные подключившегося пользователя
	var clientData ClientData
	// Сокеты идентифицируем по uuid
	clientData.wsUuid = uuid.New()
	// Первое сообщение из сокета - данные о пользователе
	websocket.JSON.Receive(ws, &clientData)

	// канал оповещений
	noteChan := make(chan *Notification, 100)

	clientRequests <- &NewClientEvent{clientData, Ch{noteChan: noteChan}}
	defer func() { clientDisconnects <- &clientData }()

	// отправка оповещений в сокет
	go func() {
		for msg := range noteChan {
			bytes, err := json.Marshal(msg)
			FailOnError(err, "Cant serialize message.")
			ws.Write(bytes)
		}
	}()

	// получение сообщений из сокета
	L:
		for {
			var messData JSMess
			websocket.JSON.Receive(ws, &messData)

			switch mtype := messData.MType; mtype {
		
			case "closed":
				fmt.Println("WS closed.")
				break L
			default:
				fmt.Println("Undefined message type.")
				break L
			}
		}
}

func main() {
	// Чтение аргументов
	args := os.Args
	if len(args) > 1 {
		local := args[1]
		if local == "local" {
			REDIS_HOST = "localhost:6379"
			MONGO_HOST = "mongodb://127.0.0.1:27017"
		}
	}

	// -----------REDIS--------------
	redis_cl := redis.NewClient(&redis.Options{
		Addr:     REDIS_HOST,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := redis_cl.Ping().Result()
	FailOnError(err, "Redis Client creation failed")

	// слушаем каналы с оповещениями
	go holdNotifications(redis_cl, EVENT_CHANNEL_NAME, NEW_EVENT)
	go holdNotifications(redis_cl, COMPANY_CHANNEL_NAME, NEW_USER_IN_COMPANY)
	// ------------END REDIS-------------------

	// -----------MONGO----------------
	// Create client
	mon_client, err := mongo.NewClient(options.Client().ApplyURI(MONGO_HOST))
	FailOnError(err, "Client creation failed")

	// options := options.Find()
	// options.SetLimit(2)
	// filter := bson.M{}

	// Create connect
	err = mon_client.Connect(context.TODO())
	FailOnError(err, "Connection to Mongo failed")

	// Check the connection
	err = mon_client.Ping(context.TODO(), nil)
	FailOnError(err, "Ping to Mongo failed")

	db = mon_client.Database("chat")

	fmt.Println("Connected to MongoDB!")

	// -----------MONGOEND----------------

	go routeEvents()
	router := mux.NewRouter().StrictSlash(true)

	router.Handle("/go/ws_chat", websocket.Handler(WsChat))
	router.Handle("/go/ws_common", websocket.Handler(WsCommon))
	http.Handle("/", router)

	http.ListenAndServe("localhost:8081", nil)
}
