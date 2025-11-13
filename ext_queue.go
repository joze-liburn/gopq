package gopq

import (
	"database/sql"

	"github.com/mattdeak/gopq/internal"
)

const (
	extEnqueueQuery    = "call push(?)"   /* item */
	extTryDequeueQuery = "call pop(?, ?)" /* now, new-deadline */
	extAckQuery        = "call ack(?)"    /* id */
	extLenQuery        = "call len(?)"    /* now */
)

// NewExtQueue creates a new queue based on external database. The database
// implementation determines the behaivour of the queue.
func NewExtQueue(db *sql.DB, opts AckOpts) (*AcknowledgeableQueue, error) {
	return &AcknowledgeableQueue{
		Queue: Queue{
			db:           db,
			name:         "gopq",
			pollInterval: defaultPollInterval,
			notifyChan:   internal.MakeNotifyChan(),
			queries: baseQueries{
				enqueue:    extEnqueueQuery,
				tryDequeue: extTryDequeueQuery,
				len:        extLenQuery,
			},
		},
		AckOpts: opts,
		ackQueries: ackQueries{
			ack: extAckQuery,
		},
	}, nil
}
