package gopq

type (
	UtilsQueryFactory interface {
		TableName() string
		SelectItemDetails() string
		DeleteItem() string
		UpdateItemForRetry() string
		ExpireAckDeadline() string
	}
	QueryFactory interface {
		UtilsQueryFactory
		Create() string
		Enqueue() string
		TryDequeue() string
		Len() string
	}
	AckableQueryFactory interface {
		QueryFactory
		Ack() string
		Nack() string
	}

	InternalQueryFactory string
)

var SqliteUtils InternalQueryFactory

func (qf InternalQueryFactory) TableName() string {
	return string(qf)
}

func (qf InternalQueryFactory) SelectItemDetails() string {
	return "SELECT retry_count, ack_deadline FROM %s WHERE id = ?"
}

func (qf InternalQueryFactory) DeleteItem() string {
	return "DELETE FROM %s WHERE id = ? RETURNING item"
}

func (qf InternalQueryFactory) UpdateItemForRetry() string {
	return `
		UPDATE %s 
		SET ack_deadline = ?, retry_count = retry_count + 1
		WHERE id = ?
	`
}

func (qf InternalQueryFactory) ExpireAckDeadline() string {
	return `
		UPDATE %s 
		SET ack_deadline = ?
		WHERE id = ?
	`
}

type QueryFactoryImpl struct {
	UtilsQueryFactory
	baseQueries
}

func NewInternalQueryFactory(path, create, push, pop, len string) QueryFactory {
	return QueryFactoryImpl{
		UtilsQueryFactory: InternalQueryFactory(path),
		baseQueries: baseQueries{
			createTable: create,
			enqueue:     push,
			tryDequeue:  pop,
			len:         len,
		},
	}
}

func (qf QueryFactoryImpl) Create() string {
	return qf.createTable
}
func (qf QueryFactoryImpl) Enqueue() string {
	return qf.enqueue
}
func (qf QueryFactoryImpl) TryDequeue() string {
	return qf.tryDequeue
}
func (qf QueryFactoryImpl) Len() string {
	return qf.len
}

/*
func (qf QueryFactoryImpl) Ack() string {
	return qf.Ack()
}
*/
