package gopq

// Opts represents the queue-level settings for how dequeue is handled.
const (
	DequeueMark DequeueAction = iota
	DequeueDelete
)
const (
	CallAuto        CallConvention = iota // Examine the list of drivers
	CallPlaceholder                       // call proc(?,?,?)
	CallPositional                        // call proc($1, $2, $3)
	CallNamed                             // call proc(:id, :now)
)

type (
	DequeueAction  int
	CallConvention int

	Opts struct {
		// DequeueAction determines the fate of the related database record when
		// the message is dequeued.
		// - DequeueMark (default behaivour) marks the record as done
		// - DequeueDelete deletes the record.
		DequeueAction DequeueAction

		//
		CallConvention CallConvention
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

//
// Behaivour on dequeue/ack
//

func DeleteOnDeququq() QueueOptions {
	return func(o *Opts) error {
		o.DequeueAction = DequeueDelete
		return nil
	}
}

func MarkOnDeququq() QueueOptions {
	return func(o *Opts) error {
		o.DequeueAction = DequeueMark
		return nil
	}
}

//
// how to specify parameters for stored procedures calls
//

func UsingPlaceholder() QueueOptions {
	return func(o *Opts) error {
		o.CallConvention = CallPlaceholder
		return nil
	}
}

func UsingPositional() QueueOptions {
	return func(o *Opts) error {
		o.CallConvention = CallPositional
		return nil
	}
}

func UsingNamed() QueueOptions {
	return func(o *Opts) error {
		o.CallConvention = CallNamed
		return nil
	}
}

func UsingMysql() QueueOptions {
	return func(o *Opts) error {
		o.CallConvention = CallPlaceholder
		return nil
	}
}

func UsingPostgres() QueueOptions {
	return func(o *Opts) error {
		o.CallConvention = CallPositional
		return nil
	}
}
