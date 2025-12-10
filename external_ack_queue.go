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
		Enqueue:    Flavour("call gopq_push_ack(:it)", qo.CallConvention),
		TryDequeue: Flavour("call gopq_pop_ack(:now, :deadline)", qo.CallConvention),
		Len:        Flavour("call gopq_len_ack(:now)", qo.CallConvention),
	}
	aq := AckQueries{
        BaseQueries: bq,
		AckUtilsQueries: AckUtilsQueries{
			Details:  Flavour("call gopq_selectItemDetails(:p_id)", qo.CallConvention),
			Delete:   Flavour("call gopq_deleteItem(:p_id)", qo.CallConvention),
			ForRetry: Flavour("call gopq_updateForRetry(:p_deadline, :p_id)", qo.CallConvention),
			Expire:   Flavour("call gopq_expireAckDeadline(:p_deadline, :p_id)", qo.CallConvention),
		},
		Ack: Flavour(ack[qo.DequeueAction], qo.CallConvention),
	}
	return NewExternalAckQueueWithQueries(db, aq, ackOpts, opts...)
}

// NewExternalQueue creates a new queue based on external database. The
// behaivour of the queue is based on database implementation details.
func NewExternalAckQueueWithQueries(db *sql.DB, aq AckQueries, ackOpts AckOpts, opts ...QueueOptions) (*AcknowledgeableQueue, error) {
	err := internal.PrepareDB(db, "", aq.Enqueue, aq.TryDequeue, aq.Len, aq.Ack, aq.Delete, aq.Details, aq.ForRetry, aq.Expire)
	if err != nil {
		return nil, fmt.Errorf("failed to create external queue: %w", err)
	}

	return &AcknowledgeableQueue{
		Queue: Queue{
			db:           db,
			pollInterval: defaultPollInterval,
			notifyChan:   internal.MakeNotifyChan(),
			queries:      aq.BaseQueries,
		},
		AckOpts:    ackOpts,
		ackQueries: aq,
	}, nil
}

func NewQueriesAck(tableName, enqueue, tryDequeue, len, ack, details, delete, forRetry, expire string) (BaseQueries, AckQueries) {
	return NewQueries(tableName, enqueue, tryDequeue, len),
		AckQueries{
			AckUtilsQueries: AckUtilsQueries{
				Details:  details,
				Delete:   delete,
				ForRetry: forRetry,
				Expire:   expire,
			},
			Ack: ack,
		}
}
