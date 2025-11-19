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
		DequeueMark:   Flavour("call gopq_ack_store(:p_id, :now)", qo.CallConvention),
		DequeueDelete: Flavour("call gopq_ack_delete(:p_id, :now)", qo.CallConvention),
	}
	bq := BaseQueries{
		enqueue:    Flavour("call gopq_push_ack(:it)", qo.CallConvention),
		tryDequeue: Flavour("call gopq_pop_ack(:now, :deadline)", qo.CallConvention),
		len:        Flavour("call gopq_len_ack(:now)", qo.CallConvention),
	}
	aq := AckQueries{
		ackUtilsQueries: ackUtilsQueries{
			details:  Flavour("call gopq_selectItemDetails(:p_id)", qo.CallConvention),
			delete:   Flavour("call gopq_deleteItem(:p_id)", qo.CallConvention),
			forRetry: Flavour("call gopq_updateForRetry(:p_deadline, :p_id)", qo.CallConvention),
			expire:   Flavour("call gopq_expireAckDeadline(:p_deadline, :p_id)", qo.CallConvention),
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
