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
		DequeueMark:   Flavour("call gopq_ack_store(?, ?)", qo.CallConvention),
		DequeueDelete: Flavour("call gopq_ack_delete(?, ?)", qo.CallConvention),
	}
	bq := BaseQueries{
		enqueue:    Flavour("call gopq_push_ack(?)", qo.CallConvention),
		tryDequeue: Flavour("call gopq_pop_ack(?, ?)", qo.CallConvention),
		len:        Flavour("call gopq_len_ack(?)", qo.CallConvention),
	}
	aq := AckQueries{
		ackUtilsQueries: ackUtilsQueries{
			details:  Flavour("call gopq_selectItemDetails(?)", qo.CallConvention),
			delete:   Flavour("call gopq_deleteItem(?)", qo.CallConvention),
			forRetry: Flavour("call gopq_updateForRetry(?, ?)", qo.CallConvention),
			expire:   Flavour("call gopq_expireAckDeadline(?, ?)", qo.CallConvention),
		},
		ack: Flavour(ack[qo.DequeueAction], qo.CallConvention),
	}
	return NewExternalAckQueueWithQueries(db, bq, aq, ackOpts, opts...)
}

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalAckQueueWithQueries(db *sql.DB, bq BaseQueries, aq AckQueries, ackOpts AckOpts, opts ...QueueOptions) (*AcknowledgeableQueue, error) {
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
