package whatever

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var (
	addrs      = [...]string{"10.129.193.165:9336", "10.129.194.172:9336"}
	client     *Client
	randSource = []byte("012345678abcdef")
)

const (
	itemCount          = 5 * 1024
	keyLength          = 10
	valueLength        = 1024
	requestCount       = 10 * 1024
	lightQueryDuration = 500 * time.Microsecond
	heavyQueryDuration = 5000 * time.Microsecond
)

func setup() {
	client = NewClient()
	for _, addr := range addrs {
		client.AddServer(addr)
	}

	rand.Seed(time.Now().UTC().UnixNano())
}

func TestWhatever(t *testing.T) {
	setup()

	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	key := []byte("foo")
	cvalue, cflags := []byte("bar"), uint64(0)

	if err := client.Set(key, 0, cflags, 0, cvalue); err != nil {
		t.Error("«Set» command failed")
		t.Error(err)
		return
	}

	svalue, sflags, err := client.Get(key)
	if err != nil {
		t.Error("«Get» command failed")
		return
	}

	if !bytes.Equal(cvalue, svalue) || cflags != sflags {
		t.Error(fmt.Sprintf("«Get» command returned value mismatch: expected %s, got %s", cvalue, svalue))
	}

	if cflags != sflags {
		t.Error(fmt.Sprintf("«Get» command returned flags mismatch: expected %d, got %d", cflags, sflags))
	}

	svalue, sflags, casid, err := client.Gets(key)
	if err != nil {
		t.Error("«Gets» command failed")
		return
	}

	if err = client.Cas(key, 0, cflags, 0, casid, cvalue); err != nil {
		t.Error("«Cas» command failed")
	}

	if err = client.Add(key, 0, cflags, 0, cvalue); err == nil {
		t.Error("«Add» command expected to fail")
	}

	if err = client.Delete(key); err != nil {
		t.Error("«Delete» command expected to succeed")
	}

	if svalue, sflags, err = client.Get(key); len(svalue) > 0 {
		t.Error("«Get» command expected to return empty value")
	}
}

func BenchmarkSimpleAccess(b *testing.B) {
	setup()

	keys := make([][]byte, itemCount)
	for i := range keys {
		keys[i] = make([]byte, keyLength)
		for j := range keys[i] {
			keys[i][j] = randSource[rand.Int()%len(randSource)]
		}
	}

	values := make([][]byte, itemCount)
	for i := range values {
		values[i] = make([]byte, valueLength)
		for j := range values[i] {
			values[i][j] = randSource[rand.Int()%len(randSource)]
		}
	}

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		for i := 0; i < requestCount; i++ {
			j := rand.Intn(itemCount)
			if value, _, _ := client.Get(keys[j]); value == nil {
				// emulate database query
				time.Sleep(heavyQueryDuration)
				client.Set(keys[j], 0, 0, 0, values[j])
			}
		}
	}
}

func BenchmarkRandomAccess(b *testing.B) {
	setup()

	keys := make([][]byte, itemCount)
	for i := range keys {
		keys[i] = make([]byte, keyLength)
		for j := range keys[i] {
			keys[i][j] = randSource[rand.Int()%len(randSource)]
		}
	}

	values := make([][]byte, itemCount)
	for i := range values {
		values[i] = make([]byte, valueLength)
		for j := range values[i] {
			values[i][j] = randSource[rand.Int()%len(randSource)]
		}
	}

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		for i := 0; i < requestCount; i++ {
			j := rand.Intn(itemCount)
			if value, _, _ := client.Get(keys[j]); value == nil {
				// emulate database query
				if j > itemCount/2 {
					time.Sleep(heavyQueryDuration)
					client.Set(keys[j], 0, 0, 0, values[j])
				} else {
					time.Sleep(lightQueryDuration)
					client.Set(keys[j], 10, 0, 0, values[j])
				}
			}
		}
	}
}

func BenchmarkCircleAccess(b *testing.B) {
	setup()

	keys := make([][]byte, itemCount)
	for i := range keys {
		keys[i] = make([]byte, keyLength)
		for j := range keys[i] {
			keys[i][j] = randSource[rand.Int()%len(randSource)]
		}
	}

	values := make([][]byte, itemCount)
	for i := range values {
		values[i] = make([]byte, valueLength)
		for j := range values[i] {
			values[i][j] = randSource[rand.Int()%len(randSource)]
		}
	}

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		for i := 0; i < requestCount; i++ {
			j := i % itemCount
			if value, _, _ := client.Get(keys[j]); value == nil {
				// emulate database query
				time.Sleep(heavyQueryDuration)
				if j < itemCount-10 {
					client.Set(keys[j], 10, 0, 0, values[j])
				} else {
					client.Set(keys[j], 0, 0, 0, values[j])
				}
			}
		}
	}
}
