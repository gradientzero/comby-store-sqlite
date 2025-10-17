package store_test

import (
	"context"
	"testing"

	store "github.com/gradientzero/comby-store-sqlite"
	"github.com/gradientzero/comby/v2"
)

func TestEventStore1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	eventStore := store.NewEventStoreSQLite("./eventStore.db")
	if err = eventStore.Init(ctx,
		comby.EventStoreOptionWithAttribute("key1", "value"),
	); err != nil {
		t.Fatal(err)
	}

	// check if the attribute is set
	if v := eventStore.Options().Attributes.Get("key1"); v != nil {
		if v != "value" {
			t.Fatalf("wrong value: %q", v)
		}
	} else {
		t.Fatalf("missing key")
	}

	// check info with empty store
	if m, err := eventStore.Info(ctx); err != nil {
		t.Fatalf("failed to get info: %v", err)
	} else {
		if m.LastItemCreatedAt != 0 {
			t.Fatalf("wrong last item created at %d", m.LastItemCreatedAt)
		}
	}

	// check totals
	if eventStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// Create test domain data
	type TestDomainData struct {
		Name  string
		Value int
	}

	// Create values
	evt1 := &comby.BaseEvent{
		InstanceId:    1,
		EventUuid:     comby.NewUuid(),
		TenantUuid:    "TenantUuid_1",
		CommandUuid:   "CommandUuid_1",
		AggregateUuid: "AggregateUuid_1",
		Domain:        "Domain_1",
		CreatedAt:     1000,
		Version:       1,
		DomainEvt: &TestDomainData{
			Name:  "TestEvent1",
			Value: 100,
		},
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}
	evt2 := &comby.BaseEvent{
		InstanceId:    1,
		EventUuid:     comby.NewUuid(),
		TenantUuid:    "TenantUuid_2",
		CommandUuid:   "CommandUuid_2",
		AggregateUuid: "AggregateUuid_2",
		Domain:        "Domain_2",
		CreatedAt:     1000,
		Version:       1,
		DomainEvt: &TestDomainData{
			Name:  "TestEvent2",
			Value: 200,
		},
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt2),
		comby.EventStoreCreateOptionWithAttribute("anyKey1", "anyValue1"),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if eventStore.Total(ctx) != 2 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// Get a value and verify all fields
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		if _evt1.GetAggregateUuid() != "AggregateUuid_1" {
			t.Fatalf("wrong aggregate uuid: %q", _evt1.GetAggregateUuid())
		}
		if _evt1.GetTenantUuid() != "TenantUuid_1" {
			t.Fatalf("wrong tenant uuid: %q", _evt1.GetTenantUuid())
		}
		if _evt1.GetCommandUuid() != "CommandUuid_1" {
			t.Fatalf("wrong command uuid: %q", _evt1.GetCommandUuid())
		}
		if _evt1.GetDomain() != "Domain_1" {
			t.Fatalf("wrong domain: %q", _evt1.GetDomain())
		}
		if _evt1.GetVersion() != 1 {
			t.Fatalf("wrong version: %d", _evt1.GetVersion())
		}
		if _evt1.GetInstanceId() != 1 {
			t.Fatalf("wrong instance id: %d", _evt1.GetInstanceId())
		}
		// Verify data_bytes is not empty
		if len(_evt1.GetDomainEvtBytes()) == 0 {
			t.Fatalf("data_bytes is empty")
		}
		// Verify data_type is set
		if len(_evt1.GetDomainEvtName()) == 0 {
			t.Fatalf("data_type is empty")
		}
	}

	// List all events
	if evts, total, err := eventStore.List(ctx); err == nil {
		if len(evts) != 2 {
			t.Fatalf("wrong number of events: %d", len(evts))
		}
		if int64(len(evts)) != total {
			t.Fatalf("wrong number of totals: %d", total)
		}
	}

	// Delete an event
	if err := eventStore.Delete(ctx,
		comby.EventStoreDeleteOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if eventStore.Total(ctx) != 1 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// reset database
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := eventStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestEventStoreWithEncrypted(t *testing.T) {
	var err error
	ctx := context.Background()

	// create crypto service
	key := []byte("12345678901234567890123456789012")
	cryptoService, _ := comby.NewCryptoService(key)

	// setup and init store
	eventStore := store.NewEventStoreSQLite("./eventStore-encrypted.db")
	if err = eventStore.Init(ctx,
		comby.EventStoreOptionWithCryptoService(cryptoService),
	); err != nil {
		t.Fatal(err)
	}

	// create domain data to encrypt/decrypt
	type MyDomainEvent struct {
		String  string
		Int     int64
		Boolean bool
	}
	domainData := &MyDomainEvent{
		String:  "string",
		Int:     123,
		Boolean: true,
	}

	// Create values
	evt1 := &comby.BaseEvent{
		EventUuid:     comby.NewUuid(),
		AggregateUuid: "AggregateUuid_1",
		Domain:        "Domain_1",
		CreatedAt:     1000,
		Version:       1,
		DomainEvt:     domainData,
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainEvent{}
		_domainData, _ = comby.Deserialize(_evt1.GetDomainEvtBytes(), _domainData)
		if _domainData.String != "string" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 123 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != true {
			t.Fatalf("wrong value")
		}
	}

	// List all events
	if evts, _, err := eventStore.List(ctx); err == nil {
		_evt1 := evts[0]
		_domainData := &MyDomainEvent{}
		_domainData, _ = comby.Deserialize(_evt1.GetDomainEvtBytes(), _domainData)
		if _domainData.String != "string" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 123 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != true {
			t.Fatalf("wrong value")
		}
	}

	// Update event
	domainData.String = "string2"
	domainData.Int = 456
	domainData.Boolean = false
	evt1.DomainEvt = domainData

	if err := eventStore.Update(ctx,
		comby.EventStoreUpdateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainEvent{}
		_domainData, _ = comby.Deserialize(_evt1.GetDomainEvtBytes(), _domainData)
		if _domainData.String != "string2" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 456 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != false {
			t.Fatalf("wrong value")
		}
	}

	// reset database
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := eventStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestEventStoreFieldLoading(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	eventStore := store.NewEventStoreSQLite("./eventStore-field-test.db")
	if err = eventStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Define a domain event type with various fields
	type ComplexDomainEvent struct {
		StringField  string
		IntField     int64
		BoolField    bool
		FloatField   float64
		ArrayField   []string
		NestedObject struct {
			Name  string
			Value int
		}
	}

	// Create a comprehensive test event with all fields populated
	testData := &ComplexDomainEvent{
		StringField: "test-string-value",
		IntField:    42,
		BoolField:   true,
		FloatField:  3.14159,
		ArrayField:  []string{"item1", "item2", "item3"},
		NestedObject: struct {
			Name  string
			Value int
		}{
			Name:  "nested",
			Value: 999,
		},
	}

	evt := &comby.BaseEvent{
		InstanceId:    123,
		EventUuid:     comby.NewUuid(),
		TenantUuid:    "tenant-uuid-123",
		CommandUuid:   "command-uuid-456",
		AggregateUuid: "aggregate-uuid-789",
		Domain:        "test-domain",
		Version:       5,
		CreatedAt:     1234567890,
		DomainEvt:     testData,
	}

	// Create the event
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt),
	); err != nil {
		t.Fatal(err)
	}

	// Test 1: Get the event and verify ALL fields are loaded
	t.Run("Get - Verify all fields loaded", func(t *testing.T) {
		loadedEvt, err := eventStore.Get(ctx,
			comby.EventStoreGetOptionWithEventUuid(evt.EventUuid),
		)
		if err != nil {
			t.Fatalf("failed to get event: %v", err)
		}
		if loadedEvt == nil {
			t.Fatal("loaded event is nil")
		}

		// Verify all base fields
		if loadedEvt.GetInstanceId() != 123 {
			t.Errorf("InstanceId: expected 123, got %d", loadedEvt.GetInstanceId())
		}
		if loadedEvt.GetEventUuid() != evt.EventUuid {
			t.Errorf("EventUuid: expected %s, got %s", evt.EventUuid, loadedEvt.GetEventUuid())
		}
		if loadedEvt.GetTenantUuid() != "tenant-uuid-123" {
			t.Errorf("TenantUuid: expected 'tenant-uuid-123', got %s", loadedEvt.GetTenantUuid())
		}
		if loadedEvt.GetCommandUuid() != "command-uuid-456" {
			t.Errorf("CommandUuid: expected 'command-uuid-456', got %s", loadedEvt.GetCommandUuid())
		}
		if loadedEvt.GetAggregateUuid() != "aggregate-uuid-789" {
			t.Errorf("AggregateUuid: expected 'aggregate-uuid-789', got %s", loadedEvt.GetAggregateUuid())
		}
		if loadedEvt.GetDomain() != "test-domain" {
			t.Errorf("Domain: expected 'test-domain', got %s", loadedEvt.GetDomain())
		}
		if loadedEvt.GetVersion() != 5 {
			t.Errorf("Version: expected 5, got %d", loadedEvt.GetVersion())
		}
		if loadedEvt.GetCreatedAt() != 1234567890 {
			t.Errorf("CreatedAt: expected 1234567890, got %d", loadedEvt.GetCreatedAt())
		}

		// CRITICAL: Verify data_bytes is not empty
		dataBytes := loadedEvt.GetDomainEvtBytes()
		if len(dataBytes) == 0 {
			t.Fatal("CRITICAL: data_bytes is empty after loading")
		}
		t.Logf("data_bytes length: %d bytes", len(dataBytes))
		t.Logf("data_bytes content (first 100 chars): %s", string(dataBytes[:min(100, len(dataBytes))]))

		// Verify data_type is set
		dataType := loadedEvt.GetDomainEvtName()
		if len(dataType) == 0 {
			t.Fatal("CRITICAL: data_type is empty after loading")
		}
		t.Logf("data_type: %s", dataType)

		// Deserialize and verify the domain data
		loadedData := &ComplexDomainEvent{}
		loadedData, err = comby.Deserialize(dataBytes, loadedData)
		if err != nil {
			t.Fatalf("failed to deserialize domain data: %v", err)
		}

		// Verify all domain data fields
		if loadedData.StringField != "test-string-value" {
			t.Errorf("StringField: expected 'test-string-value', got %s", loadedData.StringField)
		}
		if loadedData.IntField != 42 {
			t.Errorf("IntField: expected 42, got %d", loadedData.IntField)
		}
		if loadedData.BoolField != true {
			t.Errorf("BoolField: expected true, got %v", loadedData.BoolField)
		}
		if loadedData.FloatField != 3.14159 {
			t.Errorf("FloatField: expected 3.14159, got %f", loadedData.FloatField)
		}
		if len(loadedData.ArrayField) != 3 {
			t.Errorf("ArrayField length: expected 3, got %d", len(loadedData.ArrayField))
		}
		if loadedData.NestedObject.Name != "nested" {
			t.Errorf("NestedObject.Name: expected 'nested', got %s", loadedData.NestedObject.Name)
		}
		if loadedData.NestedObject.Value != 999 {
			t.Errorf("NestedObject.Value: expected 999, got %d", loadedData.NestedObject.Value)
		}
	})

	// Test 2: List events and verify ALL fields are loaded
	t.Run("List - Verify all fields loaded", func(t *testing.T) {
		evts, total, err := eventStore.List(ctx)
		if err != nil {
			t.Fatalf("failed to list events: %v", err)
		}
		if total != 1 {
			t.Fatalf("expected 1 event, got %d", total)
		}
		if len(evts) != 1 {
			t.Fatalf("expected 1 event in list, got %d", len(evts))
		}

		loadedEvt := evts[0]

		// Verify all base fields
		if loadedEvt.GetInstanceId() != 123 {
			t.Errorf("InstanceId: expected 123, got %d", loadedEvt.GetInstanceId())
		}
		if loadedEvt.GetTenantUuid() != "tenant-uuid-123" {
			t.Errorf("TenantUuid: expected 'tenant-uuid-123', got %s", loadedEvt.GetTenantUuid())
		}
		if loadedEvt.GetCommandUuid() != "command-uuid-456" {
			t.Errorf("CommandUuid: expected 'command-uuid-456', got %s", loadedEvt.GetCommandUuid())
		}

		// CRITICAL: Verify data_bytes is not empty in List
		dataBytes := loadedEvt.GetDomainEvtBytes()
		if len(dataBytes) == 0 {
			t.Fatal("CRITICAL: data_bytes is empty after List()")
		}
		t.Logf("List - data_bytes length: %d bytes", len(dataBytes))

		// Verify data_type is set
		dataType := loadedEvt.GetDomainEvtName()
		if len(dataType) == 0 {
			t.Fatal("CRITICAL: data_type is empty after List()")
		}

		// Deserialize and verify the domain data
		loadedData := &ComplexDomainEvent{}
		loadedData, err = comby.Deserialize(dataBytes, loadedData)
		if err != nil {
			t.Fatalf("failed to deserialize domain data from List: %v", err)
		}

		// Verify critical fields
		if loadedData.StringField != "test-string-value" {
			t.Errorf("List - StringField: expected 'test-string-value', got %s", loadedData.StringField)
		}
		if loadedData.IntField != 42 {
			t.Errorf("List - IntField: expected 42, got %d", loadedData.IntField)
		}
	})

	// reset database
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := eventStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
