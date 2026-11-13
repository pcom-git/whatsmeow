package appstate

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestBuildContactCreatesCriticalUnblockLowContactMutation(t *testing.T) {
	phoneJID := types.NewJID("905061914300", types.DefaultUserServer)
	lidJID := types.NewJID("87046890758268", types.HiddenUserServer)

	patch := BuildContact(ContactInfo{
		PhoneJID:                 phoneJID,
		LIDJID:                   lidJID,
		Username:                 "loveinrush",
		FirstName:                "Love",
		FullName:                 "Love In Rush",
		SaveOnPrimaryAddressbook: true,
	})

	if patch.Type != WAPatchCriticalUnblockLow {
		t.Fatalf("unexpected patch type %s", patch.Type)
	}
	if len(patch.Mutations) != 1 {
		t.Fatalf("expected one mutation, got %d", len(patch.Mutations))
	}
	mutation := patch.Mutations[0]
	if mutation.Version != 2 {
		t.Fatalf("expected contact mutation version 2, got %d", mutation.Version)
	}
	if len(mutation.Index) != 2 || mutation.Index[0] != IndexContact || mutation.Index[1] != phoneJID.String() {
		t.Fatalf("unexpected mutation index %#v", mutation.Index)
	}
	action := mutation.Value.GetContactAction()
	if action == nil {
		t.Fatal("expected contact action")
	}
	if action.GetFirstName() != "Love" || action.GetFullName() != "Love In Rush" {
		t.Fatalf("unexpected contact names: first=%q full=%q", action.GetFirstName(), action.GetFullName())
	}
	if action.GetLidJID() != lidJID.String() {
		t.Fatalf("unexpected lid jid %q", action.GetLidJID())
	}
	if action.GetPnJID() != phoneJID.String() {
		t.Fatalf("unexpected pn jid %q", action.GetPnJID())
	}
	if action.GetUsername() != "loveinrush" {
		t.Fatalf("unexpected username %q", action.GetUsername())
	}
	if !action.GetSaveOnPrimaryAddressbook() {
		t.Fatal("expected saveOnPrimaryAddressbook to be true")
	}
}

func TestBuildContactUsesLIDContactActionWhenPhoneIsMissing(t *testing.T) {
	lidJID := types.NewJID("47210397962342", types.HiddenUserServer)

	patch := BuildContact(ContactInfo{
		LIDJID:   lidJID,
		Username: "loveinrush",
		FullName: "Love In Rush",
	})

	mutation := patch.Mutations[0]
	if len(mutation.Index) != 2 || mutation.Index[0] != IndexLIDContact || mutation.Index[1] != lidJID.String() {
		t.Fatalf("expected LID contact index, got %#v", mutation.Index)
	}
	if mutation.Value.GetContactAction() != nil {
		t.Fatal("expected no phone contact action")
	}
	action := mutation.Value.GetLidContactAction()
	if action == nil {
		t.Fatal("expected LID contact action")
	}
	if action.GetUsername() != "loveinrush" {
		t.Fatalf("unexpected username %q", action.GetUsername())
	}
	if action.GetFullName() != "Love In Rush" {
		t.Fatalf("unexpected full name %q", action.GetFullName())
	}
}
