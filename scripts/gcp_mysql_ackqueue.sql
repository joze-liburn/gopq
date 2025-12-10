-- AcknowelageableQueue implementation for Google Cloud MySql
--
-- One important issue is the absence of delete ... returning statement
--
-- 2025-11-17
create table gopq_ackqueue (
    id integer not null auto_increment primary key,
    item blob(1024) not null,
    enqueued_at timestamp default current_timestamp,
    processed_at timestamp,
    ack_deadline int,
    retry_count int default 0
);

-- Inserts the item into the table.
create procedure gopq_push_ack(it blob(1024))
begin
    insert into gopq_ackqueue (item) value (it) on duplicate key update item = it;
end;

-- Dequeue element from the queue. Until `deadline˙ passes this element will not
-- be considered for dequeueing.
create procedure gopq_pop_ack(now int, deadline int)
begin
    select id, item
    into @id, @item
    from gopq_ackqueue
    where 
            (coalesce(ack_deadline, 0) < now)
        and processed_at is null
    order by enqueued_at asc
    limit 1;

    if found_rows() = 1 then
        update gopq_ackqueue
        set ack_deadline = deadline 
        where id = @id;

        select @id as id, @item as item
        where @id is not null;
    end if;
end;

-- Ack processing of the element. Record is removed from the table.
create procedure gopq_ack_delete(id int, now int)
begin
    delete from gopq_ackqueue
    where
            gopq_ackqueue.id = id
        and ack_deadline >= now;
end;

-- Ack processing of the element. Record is marked as processed.
create procedure gopq_ack_store(id int, now int)
begin
    update gopq_ackqueue
    set processed_at = current_timestamp
    where
            gopq_ackqueue.id = id
        and ack_deadline >= now;
end;

-- Return the number of elements in the queue. If the deadline is in the future
-- then the record doesn't count.
create procedure gopq_len_ack(now int)
begin
    select count(1) from gopq_ackqueue
    where 
            (coalesce(ack_deadline, 0) < now)
        and processed_at is null;
end;

-- Return the internal processing details of the record.
create procedure gopq_selectItemDetails(id int)
begin
    select
        retry_count
        , ack_deadline
    from
        gopq_ackqueue
    where
        gopq_ackqueue.id = id;
end
    
-- Remove the record from the table. Used on recorde that failed to ack more
-- than allowed number of times.
create procedure gopq_deleteItem(id int)
begin
  select item
    into @item
    from gopq_ackqueue
    where 
        gopq_ackqueue.id = id;

    delete from gopq_ackqueue
    where gopq_ackqueue.id = id;

    select @item as item;
end

-- Moves the record's deadline (into the future, deadline > now), thus putting
-- it back to the queue.
create procedure gopq_updateForRetry(deadline int, id int)
begin
    update gopq_ackqueue
    set 
        deadline = deadline
        , retry_count = retry_count + 1
    where 
        gopq_ackqueue.id = id;
end

-- Moves the record's deadline (into the future, deadline > now) but keeps the
-- retry counter.
create procedure gopq_expireAckDeadline(deadline int, id int)
begin
    update gopq_ackqueue
    set 
        gopq_ackqueue.deadline = deadline
    where 
        gopq_ackqueue.id = id;
end