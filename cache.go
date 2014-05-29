package whatever

import (
	"container/list"
	"sync"
)

type Cache struct {
	maxLength int
	l         *list.List
	m         map[string]*list.Element
	mutex     sync.Mutex
	counter   uint64
	length    int
}

type Entry struct {
	key      string
	value    []byte
	priority uint64
	flags    uint64
	casid    uint64
}

func NewCache(maxLength int) *Cache {
	cache := new(Cache)
	cache.maxLength = maxLength
	cache.m = make(map[string]*list.Element)
	cache.l = list.New()

	return cache
}

func (this *Cache) Set(key string, value []byte, priority uint64, flags uint64, exptime uint64) {
	this.mutex.Lock()
	defer this.evict()
	defer this.mutex.Unlock()

	if element, ok := this.m[key]; ok {
		entry := element.Value.(*Entry)
		this.length -= len(entry.value)
		entry.value = value
		this.length += len(value)
		entry.priority = priority
		entry.casid = this.counter
	} else {
		var position *list.Element
		for i := this.l.Front(); i != nil && i.Value.(*Entry).priority < priority; i = i.Next() {
			position = i
		}

		if position == nil {
			this.m[key] = this.l.PushFront(&Entry{key: key, value: value, priority: priority, flags: flags, casid: this.counter})
		} else {
			this.m[key] = this.l.InsertAfter(&Entry{key: key, value: value, priority: priority, flags: flags, casid: this.counter}, position)
		}
		this.length += len(value)
	}

	this.counter++
}

func (this *Cache) Add(key string, value []byte, priority uint64, flags uint64, exptime uint64) (ok bool) {
	this.mutex.Lock()
	defer this.evict()
	defer this.mutex.Unlock()

	_, ok = this.m[key]
	if !ok {
		var position *list.Element
		for i := this.l.Front(); i != nil && i.Value.(*Entry).priority < priority; i = i.Next() {
			position = i
		}

		if position == nil {
			this.m[key] = this.l.PushFront(&Entry{key: key, value: value, priority: priority, flags: flags, casid: this.counter})
		} else {
			this.m[key] = this.l.InsertAfter(&Entry{key: key, value: value, priority: priority, flags: flags, casid: this.counter}, position)
		}
		this.length += len(value)

		this.counter++
	}

	return !ok
}

func (this *Cache) Replace(key string, value []byte, priority uint64, flags uint64, exptime uint64) (ok bool) {
	this.mutex.Lock()
	defer this.evict()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry := element.Value.(*Entry)
		this.length -= len(entry.value)
		entry.value = value
		this.length += len(value)
		entry.priority = priority
		entry.casid = this.counter

		this.counter++
	}

	return
}

func (this *Cache) Append(key string, value []byte, priority uint64, flags uint64, exptime uint64) (ok bool) {
	this.mutex.Lock()
	defer this.evict()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry := element.Value.(*Entry)
		entry.value = append(entry.value, value...)
		this.length += len(value)
		entry.priority = priority
		entry.casid = this.counter

		this.counter++
	}

	return
}

func (this *Cache) Prepend(key string, value []byte, priority uint64, flags uint64, exptime uint64) (ok bool) {
	this.mutex.Lock()
	defer this.evict()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry := element.Value.(*Entry)
		entry.value = append(value, entry.value...)
		this.length += len(value)
		entry.priority = priority
		entry.casid = this.counter

		this.counter++
	}

	return
}

func (this *Cache) CheckAndStore(key string, value []byte, priority uint64, flags uint64, exptime uint64, casid uint64) (entry *Entry, ok bool) {
	this.mutex.Lock()
	defer this.evict()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry = element.Value.(*Entry)
		if entry.casid == casid {
			this.length -= len(entry.value)
			entry.value = value
			this.length += len(value)
			entry.priority = priority
			entry.casid = this.counter

			this.counter++
		} else {
			ok = false
		}
	}

	return
}

func (this *Cache) Get(key string) (value []byte, flags uint64, size uint64, ok bool) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry := element.Value.(*Entry)
		value = entry.value
		flags = entry.flags
		size = uint64(len(value))
	}

	return
}

func (this *Cache) Gets(key string) (value []byte, flags uint64, size uint64, casid uint64, ok bool) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry := element.Value.(*Entry)
		value = entry.value
		flags = entry.flags
		size = uint64(len(value))
		casid = entry.casid
	}

	return
}

func (this *Cache) Delete(key string) bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	element, ok := this.m[key]
	if ok {
		entry := this.l.Remove(element).(*Entry)
		this.length -= len(entry.value)
		delete(this.m, key)
	}

	return ok
}

func (this *Cache) evict() {
	for i := this.l.Front(); i != nil && this.length > this.maxLength; i = i.Next() {
		entry := this.l.Remove(i).(*Entry)
		this.length -= len(entry.value)
		delete(this.m, entry.key)
	}
}
