package whatsmeow

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type fakeAddContactLIDStore struct {
	lid      types.JID
	pn       types.JID
	mappings []store.LIDMapping
	err      error
}

func (f *fakeAddContactLIDStore) PutManyLIDMappings(_ context.Context, mappings []store.LIDMapping) error {
	f.mappings = append(f.mappings, mappings...)
	return nil
}

func (f *fakeAddContactLIDStore) PutLIDMapping(_ context.Context, lid, pn types.JID) error {
	f.lid = lid
	f.pn = pn
	return f.err
}

func (f *fakeAddContactLIDStore) GetPNForLID(context.Context, types.JID) (types.JID, error) {
	return types.EmptyJID, nil
}

func (f *fakeAddContactLIDStore) GetLIDForPN(context.Context, types.JID) (types.JID, error) {
	return types.EmptyJID, nil
}

func (f *fakeAddContactLIDStore) GetManyLIDsForPNs(context.Context, []types.JID) (map[types.JID]types.JID, error) {
	return nil, nil
}

type fakeAppStateContactStore struct {
	entries []store.ContactEntry
	jid     types.JID
}

func (f *fakeAppStateContactStore) PutPushName(context.Context, types.JID, string) (bool, string, error) {
	return false, "", nil
}

func (f *fakeAppStateContactStore) PutBusinessName(context.Context, types.JID, string) (bool, string, error) {
	return false, "", nil
}

func (f *fakeAppStateContactStore) PutContactName(_ context.Context, jid types.JID, _, _ string, _ bool) error {
	f.jid = jid
	return nil
}

func (f *fakeAppStateContactStore) PutAllContactNames(_ context.Context, entries []store.ContactEntry) error {
	f.entries = append(f.entries, entries...)
	return nil
}

func (f *fakeAppStateContactStore) PutManyRedactedPhones(context.Context, []store.RedactedPhoneEntry) error {
	return nil
}

func (f *fakeAppStateContactStore) GetContact(context.Context, types.JID) (types.ContactInfo, error) {
	return types.ContactInfo{}, nil
}

func (f *fakeAppStateContactStore) GetAllContacts(context.Context) (map[types.JID]types.ContactInfo, error) {
	return nil, nil
}

func (f *fakeAppStateContactStore) GetContactListPage(context.Context, store.ContactListPageOptions) (store.ContactListPage, error) {
	return store.ContactListPage{}, nil
}

func newContactMutation(pn, lid types.JID) appstate.Mutation {
	return appstate.Mutation{
		Index:     []string{appstate.IndexContact, pn.String()},
		Operation: waServerSync.SyncdMutation_SET,
		Action: &waSyncAction.SyncActionValue{
			ContactAction: &waSyncAction.ContactAction{
				FirstName: proto.String("Love"),
				FullName:  proto.String("Love In Rush"),
				LidJID:    proto.String(lid.String()),
				PnJID:     proto.String(pn.String()),
			},
		},
	}
}

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

func TestCacheAddContactLIDMappingStoresResolvedPhoneAndLID(t *testing.T) {
	lidStore := &fakeAddContactLIDStore{}
	cli := &Client{Store: &store.Device{LIDs: lidStore}}
	identity := addContactIdentity{
		PhoneJID: types.NewJID("15167072015", types.DefaultUserServer),
		LIDJID:   types.NewJID("101395722203279", types.HiddenUserServer),
	}

	if err := cli.cacheAddContactLIDMapping(context.Background(), identity); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lidStore.lid != identity.LIDJID || lidStore.pn != identity.PhoneJID {
		t.Fatalf("unexpected cached mapping: lid=%s pn=%s", lidStore.lid, lidStore.pn)
	}
}

func TestCacheAddContactLIDMappingSkipsUsernameOnlyContact(t *testing.T) {
	lidStore := &fakeAddContactLIDStore{}
	cli := &Client{Store: &store.Device{LIDs: lidStore}}
	identity := addContactIdentity{
		LIDJID: types.NewJID("101395722203279", types.HiddenUserServer),
	}

	if err := cli.cacheAddContactLIDMapping(context.Background(), identity); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !lidStore.lid.IsEmpty() || !lidStore.pn.IsEmpty() {
		t.Fatalf("expected no cached mapping, got lid=%s pn=%s", lidStore.lid, lidStore.pn)
	}
}

func TestCacheAddContactLIDMappingReturnsStoreError(t *testing.T) {
	expectedErr := errors.New("store failed")
	lidStore := &fakeAddContactLIDStore{err: expectedErr}
	cli := &Client{Store: &store.Device{LIDs: lidStore}}
	identity := addContactIdentity{
		PhoneJID: types.NewJID("15167072015", types.DefaultUserServer),
		LIDJID:   types.NewJID("101395722203279", types.HiddenUserServer),
	}

	err := cli.cacheAddContactLIDMapping(context.Background(), identity)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected store error, got %v", err)
	}
}

func TestDispatchContactAppStateStoresLIDMapping(t *testing.T) {
	lidStore := &fakeAddContactLIDStore{}
	contactStore := &fakeAppStateContactStore{}
	cli := &Client{Store: &store.Device{Contacts: contactStore, LIDs: lidStore}}
	pn := types.NewJID("15167072015", types.DefaultUserServer)
	lid := types.NewJID("101395722203279", types.HiddenUserServer)

	cli.dispatchAppState(context.Background(), appstate.WAPatchCriticalUnblockLow, newContactMutation(pn, lid), false)

	if lidStore.lid != lid || lidStore.pn != pn {
		t.Fatalf("expected LID mapping %s -> %s, got %s -> %s", lid, pn, lidStore.lid, lidStore.pn)
	}
}

func TestFullSyncContactSnapshotStoresLIDMappings(t *testing.T) {
	lidStore := &fakeAddContactLIDStore{}
	contactStore := &fakeAppStateContactStore{}
	cli := &Client{Store: &store.Device{Contacts: contactStore, LIDs: lidStore}, Log: waLog.Noop}
	pn := types.NewJID("15167072015", types.DefaultUserServer)
	lid := types.NewJID("101395722203279", types.HiddenUserServer)

	err := cli.collectEventsToDispatch(
		context.Background(),
		appstate.WAPatchCriticalUnblockLow,
		[]appstate.Mutation{newContactMutation(pn, lid)},
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected collect error: %v", err)
	}

	if len(lidStore.mappings) != 1 || lidStore.mappings[0].LID != lid || lidStore.mappings[0].PN != pn {
		t.Fatalf("expected full sync LID mapping %s -> %s, got %#v", lid, pn, lidStore.mappings)
	}
}
