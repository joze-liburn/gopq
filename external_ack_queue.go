package gopq

import (
	"database/sql"
	"fmt"

	"github.com/mattdeak/gopq/internal"
)

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalAckQueue(db *sql.DB, ackOpts AckOpts, opts ...QueueOptions) (*AcknowledgeableQueue, error) {
	qo := Opts{}
	qo.Apply(opts...)

	ack := map[DequeueAction]string{
		DequeueMark:   "call gopq_ack_store(?, ?)",
		DequeueDelete: "call gopq_ack_delete(?, ?)",
	}
	bq := baseQueries{
		enqueue:    "call gopq_push_ack(?)",
		tryDequeue: "call gopq_pop_ack(?, ?)",
		len:        "call gopq_len_ack(?)",
	}
	aq := ackQueries{
		ackUtilsQueries: ackUtilsQueries{
			details:  "call gopq_selectItemDetails(?)",
			delete:   "call gopq_deleteItem(?)",
			forRetry: "call gopq_updateForRetry(?, ?)",
			expire:   "call gopq_expireAckDeadline(?, ?)",
		},
		ack: ack[qo.DequeueAction],
	}
	return NewExternalAckQueueWithQueries(db, bq, aq, ackOpts, opts...)
}

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalAckQueueWithQueries(db *sql.DB, bq baseQueries, aq ackQueries, ackOpts AckOpts, opts ...QueueOptions) (*AcknowledgeableQueue, error) {
	err := internal.PrepareDB(db, "", bq.enqueue, bq.tryDequeue, bq.len, aq.ack, aq.delete, aq.details, aq.forRetry, aq.expire)
	if err != nil {
		return nil, fmt.Errorf("failed to create external queue: %w", err)
	}

	return &AcknowledgeableQueue{
		Queue: Queue{
			db:           db,
			pollInterval: defaultPollInterval,
			notifyChan:   internal.MakeNotifyChan(),
			queries:      bq,
		},
		AckOpts:    ackOpts,
		ackQueries: aq,
	}, nil
}
