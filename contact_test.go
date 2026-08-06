package whatsmeow

import (
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestParsePhoneContactQueryResponse(t *testing.T) {
	list := waBinary.Node{
		Tag: "list",
		Content: []waBinary.Node{{
			Tag: "user",
			Attrs: waBinary.Attrs{
				"jid":    types.NewJID("87046890758268", types.HiddenUserServer),
				"pn_jid": types.NewJID("905061914300", types.DefaultUserServer),
			},
			Content: []waBinary.Node{{
				Tag:     "contact",
				Attrs:   waBinary.Attrs{"type": "in"},
				Content: []byte("905061914300"),
			}},
		}},
	}

	resolved, err := parsePhoneContactQueryResponse(&list, "905061914300")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved.LIDJID.String() != "87046890758268@lid" {
		t.Fatalf("unexpected lid jid %s", resolved.LIDJID)
	}
	if resolved.PhoneJID.String() != "905061914300@s.whatsapp.net" {
		t.Fatalf("unexpected phone jid %s", resolved.PhoneJID)
	}
}

func TestNormalizeAddContactPhoneUsesE164Digits(t *testing.T) {
	phone := normalizeAddContactPhone("+1 (516) 707-2015")
	if phone != "15167072015" {
		t.Fatalf("unexpected normalized phone %q", phone)
	}
}

func TestParsePhoneContactQueryResponseRejectsUnexpectedPNJID(t *testing.T) {
	list := waBinary.Node{
		Tag: "list",
		Content: []waBinary.Node{{
			Tag: "user",
			Attrs: waBinary.Attrs{
				"jid":    types.NewJID("87046890758268", types.HiddenUserServer),
				"pn_jid": types.NewJID("8615167072015", types.DefaultUserServer),
			},
			Content: []waBinary.Node{{
				Tag:     "contact",
				Attrs:   waBinary.Attrs{"type": "in"},
				Content: []byte("15167072015"),
			}},
		}},
	}

	_, err := parsePhoneContactQueryResponse(&list, "15167072015")
	if err == nil {
		t.Fatal("expected unexpected pn_jid error")
	}
}

func TestParsePhoneContactQueryResponseRejectsInvalidContact(t *testing.T) {
	list := waBinary.Node{
		Tag: "list",
		Content: []waBinary.Node{{
			Tag: "user",
			Content: []waBinary.Node{{
				Tag:     "contact",
				Attrs:   waBinary.Attrs{"type": "invalid"},
				Content: []byte("445061914300"),
			}},
		}},
	}

	_, err := parsePhoneContactQueryResponse(&list, "445061914300")
	if err == nil {
		t.Fatal("expected invalid phone error")
	}
}

func TestParsePhoneContactDeltaResponseRequiresIntegrityPass(t *testing.T) {
	list := waBinary.Node{
		Tag: "list",
		Content: []waBinary.Node{{
			Tag: "user",
			Attrs: waBinary.Attrs{
				"jid":    types.NewJID("87046890758268", types.HiddenUserServer),
				"pn_jid": types.NewJID("905061914300", types.DefaultUserServer),
			},
			Content: []waBinary.Node{{
				Tag:     "contact",
				Attrs:   waBinary.Attrs{"type": "in"},
				Content: []byte("905061914300"),
			}},
		}},
	}
	result := waBinary.Node{
		Tag: "result",
		Content: []waBinary.Node{{
			Tag:   "contact",
			Attrs: waBinary.Attrs{"integrity": "pass", "version": "1785987365470048", "addressing_mode": "lid"},
		}},
	}

	if err := parsePhoneContactDeltaResponse(&list, &result, "905061914300", types.NewJID("87046890758268", types.HiddenUserServer)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseUsernameContactDeltaResponseRequiresActiveUsername(t *testing.T) {
	list := waBinary.Node{
		Tag: "list",
		Content: []waBinary.Node{{
			Tag:   "user",
			Attrs: waBinary.Attrs{"jid": types.NewJID("47210397962342", types.HiddenUserServer)},
			Content: []waBinary.Node{
				{Tag: "username", Attrs: waBinary.Attrs{"state": "active"}, Content: []byte("loveinrush")},
				{Tag: "contact", Attrs: waBinary.Attrs{"type": "in"}},
			},
		}},
	}
	result := waBinary.Node{
		Tag: "result",
		Content: []waBinary.Node{{
			Tag:   "contact",
			Attrs: waBinary.Attrs{"integrity": "pass", "version": "1785987604391065", "addressing_mode": "lid"},
		}},
	}

	if err := parseUsernameContactDeltaResponse(&list, &result, "loveinrush", types.NewJID("47210397962342", types.HiddenUserServer)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
