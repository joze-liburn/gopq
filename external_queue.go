package gopq

import (
	"database/sql"
	"fmt"

	"github.com/mattdeak/gopq/internal"
)

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalQueue(db *sql.DB, opts ...QueueOptions) (*Queue, error) {
	qo := Opts{}
	qo.Apply(opts...)

	dequeue := map[DequeueAction]string{
		DequeueMark:   "call gopq_pop_store()",
		DequeueDelete: "call gopq_pop_delete()",
	}
	q := baseQueries{
		enqueue:    "call gopq_push(?)",
		tryDequeue: dequeue[qo.DequeueAction],
		len:        "call gopq_len()",
	}
	return NewExternalQueueWithQueries(db, q)
}

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalQueueWithQueries(db *sql.DB, q baseQueries) (*Queue, error) {
	err := internal.PrepareDB(db, "", q.enqueue, q.tryDequeue, q.len)
	if err != nil {
		return nil, fmt.Errorf("failed to create external queue: %w", err)
	}

	return &Queue{
		db:           db,
		pollInterval: defaultPollInterval,
		notifyChan:   internal.MakeNotifyChan(),
		queries:      q,
	}, nil

}
