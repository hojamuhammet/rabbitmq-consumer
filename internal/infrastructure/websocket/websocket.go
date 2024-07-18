package websocket

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	Dst  string
}

type WebSocketServer struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan SMSMessage
	mu         sync.Mutex
}

type SMSMessage struct {
	Source      string `json:"src"`
	Destination string `json:"dst"`
	Text        string `json:"txt"`
	Date        string `json:"date"`
	Parts       int    `json:"parts"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewWebSocketServer() *WebSocketServer {
	return &WebSocketServer{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan SMSMessage),
	}
}

func (server *WebSocketServer) Run() {
	for {
		select {
		case client := <-server.register:
			server.mu.Lock()
			server.clients[client] = true
			server.mu.Unlock()
		case client := <-server.unregister:
			server.mu.Lock()
			if _, ok := server.clients[client]; ok {
				delete(server.clients, client)
				client.Conn.Close()
			}
			server.mu.Unlock()
		case message := <-server.broadcast:
			server.mu.Lock()
			for client := range server.clients {
				if client.Dst == message.Destination {
					err := client.Conn.WriteJSON(message)
					if err != nil {
						client.Conn.Close()
						delete(server.clients, client)
					}
				}
			}
			server.mu.Unlock()
		}
	}
}

func (server *WebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		return
	}

	dst := r.URL.Query().Get("dst")
	if dst == "" {
		log.Printf("Missing 'dst' query parameter")
		conn.Close()
		return
	}

	client := &Client{Conn: conn, Dst: dst}
	server.register <- client

	go server.handleMessages(client)
}

func (server *WebSocketServer) handleMessages(client *Client) {
	defer func() {
		server.unregister <- client
	}()

	for {
		var msg SMSMessage
		err := client.Conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Error reading JSON: %v", err)
			return
		}
		server.BroadcastMessage(msg)
	}
}

func (server *WebSocketServer) BroadcastMessage(message SMSMessage) {
	server.broadcast <- message
}
