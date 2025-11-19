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
		DequeueMark:   Flavour("call gopq_pop_store()", qo.CallConvention),
		DequeueDelete: Flavour("call gopq_pop_delete()", qo.CallConvention),
	}
	q := BaseQueries{
		enqueue:    Flavour("call gopq_push(?)", qo.CallConvention),
		tryDequeue: Flavour(dequeue[qo.DequeueAction], qo.CallConvention),
		len:        Flavour("call gopq_len()", qo.CallConvention),
	}
	return NewExternalQueueWithQueries(db, q, opts...)
}

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalQueueWithQueries(db *sql.DB, q BaseQueries, opts ...QueueOptions) (*Queue, error) {
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
