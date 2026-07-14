package main

import (
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// Очередь хранит сообщения и ожидающие GET-запросы.
// И сообщения, и ожидающие запросы обрабатываются в порядке FIFO.

type waiter struct {
	ch   chan string
	done bool // читать/писать только под q.mu
}

type queue struct {
	mu      sync.Mutex
	msgs    []string
	waiters []*waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) get(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *broker) put(name, v string) {
	q := b.get(name)

	q.mu.Lock()
	for len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]

		if w.done {
			continue
		}

		w.done = true
		q.mu.Unlock()

		w.ch <- v
		return
	}

	q.msgs = append(q.msgs, v)
	q.mu.Unlock()
}

// fetch возвращает (message, ok).
// Если ok == false, сообщение не получено,
// вызывающий код должен вернуть HTTP 404.
func (b *broker) fetch(name string, timeout time.Duration) (string, bool) {
	q := b.get(name)

	q.mu.Lock()

	// Если сообщение уже есть — сразу отдаем.
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		q.mu.Unlock()
		return msg, true
	}

	// Без ожидания.
	if timeout <= 0 {
		q.mu.Unlock()
		return "", false
	}

	// Регистрируем ожидателя.
	w := &waiter{
		ch: make(chan string, 1),
	}
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true

	case <-timer.C:
		q.mu.Lock()

		// Если PUT уже победил, сообщение должно прийти по каналу.
		if w.done {
			q.mu.Unlock()
			msg := <-w.ch
			return msg, true
		}

		// Победил timeout.
		w.done = true

		for i, x := range q.waiters {
			if x == w {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}

		q.mu.Unlock()

		return "", false

		// Сообщение уже было передано конкурентным PUT.

	}
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	b := newBroker()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[1:]
		if name == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.put(name, v)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			var timeout time.Duration
			if t := r.URL.Query().Get("timeout"); t != "" {
				if secs, err := strconv.Atoi(t); err == nil && secs > 0 {
					timeout = time.Duration(secs) * time.Second
				}
			}
			msg, ok := b.fetch(name, timeout)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write([]byte(msg))

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.ListenAndServe(":"+port, nil)
}
