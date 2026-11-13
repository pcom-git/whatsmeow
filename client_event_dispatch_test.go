package whatsmeow

import (
	"testing"

	"go.mau.fi/whatsmeow/types/events"
)

func TestDispatchMessageEventOnlyCallsMessageHandlers(t *testing.T) {
	cli := &Client{}
	var messageCalls, receiptCalls, regularCalls, legacyCalls int

	cli.AddMessageEventHandler(func(evt any) {
		messageCalls++
	})
	cli.AddReceiptEventHandler(func(evt any) {
		receiptCalls++
	})
	cli.AddRegularEventHandler(func(evt any) {
		regularCalls++
	})
	cli.AddEventHandler(func(evt any) {
		legacyCalls++
	})

	if failed := cli.dispatchMessageEvent(&events.Message{}); failed {
		t.Fatal("dispatchMessageEvent unexpectedly reported handler failure")
	}

	if messageCalls != 1 {
		t.Fatalf("message handler calls = %d, want 1", messageCalls)
	}
	if receiptCalls != 0 {
		t.Fatalf("receipt handler calls = %d, want 0", receiptCalls)
	}
	if regularCalls != 0 {
		t.Fatalf("regular handler calls = %d, want 0", regularCalls)
	}
	if legacyCalls != 0 {
		t.Fatalf("legacy handler calls = %d, want 0", legacyCalls)
	}
}

func TestDispatchMessageEventDoesNotFallbackToLegacyHandlers(t *testing.T) {
	cli := &Client{}
	var legacyCalls int

	cli.AddEventHandler(func(evt any) {
		legacyCalls++
	})

	if failed := cli.dispatchMessageEvent(&events.Message{}); failed {
		t.Fatal("dispatchMessageEvent unexpectedly reported handler failure")
	}

	if legacyCalls != 0 {
		t.Fatalf("legacy handler calls = %d, want 0", legacyCalls)
	}
}

func TestDispatchReceiptEventOnlyCallsReceiptHandlers(t *testing.T) {
	cli := &Client{}
	var messageCalls, receiptCalls, regularCalls, legacyCalls int

	cli.AddMessageEventHandler(func(evt any) {
		messageCalls++
	})
	cli.AddReceiptEventHandler(func(evt any) {
		receiptCalls++
	})
	cli.AddRegularEventHandler(func(evt any) {
		regularCalls++
	})
	cli.AddEventHandler(func(evt any) {
		legacyCalls++
	})

	if failed := cli.dispatchReceiptEvent(&events.Receipt{}); failed {
		t.Fatal("dispatchReceiptEvent unexpectedly reported handler failure")
	}

	if messageCalls != 0 {
		t.Fatalf("message handler calls = %d, want 0", messageCalls)
	}
	if receiptCalls != 1 {
		t.Fatalf("receipt handler calls = %d, want 1", receiptCalls)
	}
	if regularCalls != 0 {
		t.Fatalf("regular handler calls = %d, want 0", regularCalls)
	}
	if legacyCalls != 0 {
		t.Fatalf("legacy handler calls = %d, want 0", legacyCalls)
	}
}

func TestDispatchReceiptEventDoesNotFallbackToLegacyHandlers(t *testing.T) {
	cli := &Client{}
	var legacyCalls int

	cli.AddEventHandler(func(evt any) {
		legacyCalls++
	})

	if failed := cli.dispatchReceiptEvent(&events.Receipt{}); failed {
		t.Fatal("dispatchReceiptEvent unexpectedly reported handler failure")
	}

	if legacyCalls != 0 {
		t.Fatalf("legacy handler calls = %d, want 0", legacyCalls)
	}
}

func TestDispatchRegularEventCallsRegularAndLegacyHandlers(t *testing.T) {
	cli := &Client{}
	var messageCalls, receiptCalls, regularCalls, legacyCalls int

	cli.AddMessageEventHandler(func(evt any) {
		messageCalls++
	})
	cli.AddReceiptEventHandler(func(evt any) {
		receiptCalls++
	})
	cli.AddRegularEventHandler(func(evt any) {
		regularCalls++
	})
	cli.AddEventHandler(func(evt any) {
		legacyCalls++
	})

	if failed := cli.dispatchRegularEvent(&events.Connected{}); failed {
		t.Fatal("dispatchRegularEvent unexpectedly reported handler failure")
	}

	if messageCalls != 0 {
		t.Fatalf("message handler calls = %d, want 0", messageCalls)
	}
	if receiptCalls != 0 {
		t.Fatalf("receipt handler calls = %d, want 0", receiptCalls)
	}
	if regularCalls != 1 {
		t.Fatalf("regular handler calls = %d, want 1", regularCalls)
	}
	if legacyCalls != 1 {
		t.Fatalf("legacy handler calls = %d, want 1", legacyCalls)
	}
}

func TestTypedDispatchPropagatesHandlerFailure(t *testing.T) {
	cli := &Client{}
	cli.AddMessageEventHandlerWithSuccessStatus(func(evt any) bool {
		return false
	})

	if failed := cli.dispatchMessageEvent(&events.Message{}); !failed {
		t.Fatal("dispatchMessageEvent did not report handler failure")
	}
}
