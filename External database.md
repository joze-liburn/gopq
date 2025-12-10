# External database

## Database API

Gopq allows user to provide own external database. This can assist with the
database maintenance and opens the queue to other uses (and abuses).

Two types of queue are supported: `Queue` and `AcknowledgeableQueue`. The latter
is not only a functional extension but also uses extended calls (additional
parameters) to the basic operations, so there are essentially two different APIs.

### Store procedure names

While the signature of these procedures is fixed, names can differ. The names
from the table above are recognised out of the box, however calling something
like

```go
q := gopq.func NewExternalQueueWithQueries(db,
    gopq.BaseQueries {
        enqueue:    "call InsertIntoQueue(?)",
        tryDequeue: "call GetTopElement()",
        len:        "call GetQueueLength()",
    })
```

would work with procedures of different names. Name of the underlying table is
not required for queue operation.

### Store or delete processed elements

When the external_queue is initialized, it defaults to keep the processed record
(just mark it as such). This requires additional maintenance with long running
setups, so it is possible to request to remove the processed records from the
table. Thus API provides for two distinct "pop" procedures. This is mostly to
maintain compatibility with internal Sqlite implementation. The decision which
one to use is made at the queue initialization. If you only use one (or another),
you can pass the same procedure implementation afor both; and if you provide
your own names, you can at any rate provide only a single "pop" procedure name.

The following queues will behave the same:

```go
q1 := gopq.func NewExternalQueue(db, DeleteOnDequeue())
q2 := gopq.func NewExternalQueueWithQueries(db,
    gopq.BaseQueries {
        enqueue:    "call gopq_push(?)",
        tryDequeue: "call gopq_pop_delete()",
        len:        "call gopq_len()",
    })
```

and, providing the following stored procedure:

```sql
create procedure gopq_pop_store()
begin
    call gopq_pop_delete();
end
```

there's also the third option:

```go
q3 := gopq.func NewExternalQueue(db)
```

The queue `q1` selected behaivour upon initialization - it calls
`gopq_pop_delete`. The queue `q2` also selected behaivour upon initialization -
it provided `gopq_pop_delete` as the "pop" procedure. The final, `q3`, should be
avoided wherever possible because the name (promise) and behaivour of the stored
procedure differ. However at the end of the day, `q3` uses `gopq_pop_store`
(default) and this behind the scene deletes the processed element.

With **unique** queues, you have to select between two definitions of uniqueness:

- at most one copy of a given element is active at the time
- a given copy accurs at most once in the queue for the lifespan of the queue

For the second interpretation, one must not delete elements when processed.

### API: Queue

The external database must satisfy the following stored procedures:

| Function                | SQL header                 | result set                | records |
|-------------------------|----------------------------|---------------------------|:-------:|
| enqueue                 | `gopq_push(it blob(1024))` |                           |    0    |
| dequeue (store record)  | `gopq_pop_store()`         | `id int, item blob(1024)` | 0 or 1  |
| dequeue (delete record) | `gopq_pop_delete()`        | `id int, item blob(1024)` | 0 or 1  |
| length                  | `gopq_len()`               | `int`                     |    1    |

Obviously the behaivour of the queue depends heavily on the implementation of
these SQL procedures.

### API: AcknoweledgabeQueue

The external database must satisfy the following stored procedures:

| Function            | SQL header                                     | result set                                | records |
|---------------------|------------------------------------------------|-------------------------------------------|:-------:|
| enqueue             | `gopq_push_ack(it blob(1024))`                 |                                           |    0    |
| dequeue             | `gopq_pop_ack(int now, int deadline)`          | `id as int, item as blob(1024)`           | 0 or 1  |
| ack (store record)  | `gopq_ack_store(int id, int now)`              |                                           |    0    |
| ack (delete record) | `gopq_ack_delete(int id, int now)`             |                                           |    0    |
| length              | `gopq_len(now int)`                            | `int`                                     |    1    |
| details             | `gopq_selectItemDetails(id int)`               | `retry_count as int, ack_deadline as int` | 0 or 1  |
| delete              | `gopq_deleteItem(id int)`                      | `item as blob(1024)`                      |    1    |
| forRetry            | `gopq_updateForRetry(deadline int, id int)`    |                                           |    0    |
| expire              | `gopq_expireAckDeadline(deadline int, id int)` |                                           |    0    |

