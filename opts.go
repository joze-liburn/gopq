package gopq

// Opts represents the queue-level settings for how dequeue is handled.
const (
	DequeueMark DequeueAction = iota
	DequeueDelete
)

type (
	DequeueAction int

	Opts struct {
		// DequeueAction determines the fate of the related database record when
		// the message is dequeued.
		// - DequeueMark (default behaivour) marks the record as done
		// - DequeueDelete deletes the record.
		DequeueAction DequeueAction
	}

	QueueOptions func(*Opts) error
)

func (co *Opts) Apply(opts ...QueueOptions) error {
	for _, op := range opts {
		if err := op(co); err != nil {
			return err
		}
	}
	return nil
}

func DeleteOnDeququq(o *Opts) QueueOptions {
	return func(o *Opts) error {
		o.DequeueAction = DequeueDelete
		return nil
	}
}

func MarkOnDeququq(o *Opts) QueueOptions {
	return func(o *Opts) error {
		o.DequeueAction = DequeueMark
		return nil
	}
}
