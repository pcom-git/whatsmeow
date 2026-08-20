package whatsmeow

import (
	"testing"

	"go.mau.fi/whatsmeow/binary"
)

func TestHandlerQueueForNodeSeparatesMessagesFromRegularEvents(t *testing.T) {
	cli := &Client{
		messageHandlerQueue: make(chan *binary.Node, 1),
		regularHandlerQueue: make(chan *binary.Node, 1),
	}

	tests := []struct {
		name string
		tag  string
		want chan *binary.Node
	}{
		{name: "message node", tag: "message", want: cli.messageHandlerQueue},
		{name: "appdata node", tag: "appdata", want: cli.messageHandlerQueue},
		{name: "receipt node", tag: "receipt", want: cli.regularHandlerQueue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &binary.Node{Tag: tt.tag}
			if got := cli.handlerQueueForNode(node); got != tt.want {
				t.Fatalf("handlerQueueForNode(%q) returned wrong queue", tt.tag)
			}
		})
	}
}
