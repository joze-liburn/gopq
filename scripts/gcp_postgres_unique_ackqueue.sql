-- AcknowelageableQueue unique queue implementation for Google Cloud PostgreSQL
--
-- Mixture of SQL and PLPGSQL
--
-- 2025-11-18

create table gopq_ackqueue (
    id bigserial primary key
    , item text not null
    , enqueued_at timestamp default current_timestamp
    , processed_at timestamp
    , ack_deadline int
    , retry_count int default 0
);
create unique index gopq_idxunique on gopq_ackqueue (md5(item));

-- Inserts the item into the table.
create or replace procedure gopq_push_ack(it text)
begin atomic
    insert into gopq_ackqueue (item) values (it) on conflict do nothing;
end;

-- Dequeue element from the queue. Until `deadline˙ passes this element will not
-- be considered for dequeueing.
create or replace procedure gopq_pop_ack(now int, deadline int)
language plpgsql as 
$$
declare
    new_id gopq_ackqueue.id%type;
    new_item gopq_ackqueue.item%type;
begin 
    select id, item
    into new_id, new_item
    from gopq_ackqueue
    where 
            (coalesce(ack_deadline, 0) < now)
        and processed_at is null
    order by enqueued_at asc
    limit 1;

  if found then
        update gopq_ackqueue
        set ack_deadline = deadline 
        where id = new_id;

        select new_id as id, new_item as item from dual;
    end if;
end;
$$

-- Ack processing of the element. Record is removed from the table.
create or replace procedure gopq_ack_delete(p_id int, now int)
language plpgsql as 
$$
begin
    delete from gopq_ackqueue
    where
            id = p_id
        and ack_deadline >= now;
end;
$$;

-- Ack processing of the element. Record is marked as processed.
create or replace procedure gopq_ack_store(p_id int, now int)
language plpgsql as 
$$
begin
    update gopq_ackqueue
    set processed_at = current_timestamp
    where
            id = p_id
        and ack_deadline >= now;
end;
$$;

-- Return the number of elements in the queue. If the deadline is in the future
-- then the record doesn't count.
create or replace procedure gopq_len_ack(now int)
language sql
begin atomic
    select count(1) from gopq_ackqueue
    where 
            (coalesce(ack_deadline, 0) < now)
        and processed_at is null;
end;

-- Return the internal processing details of the record.
create or replace procedure gopq_selectItemDetails(p_id int)
language sql
begin atomic    select
        retry_count
        , ack_deadline
    from
        gopq_ackqueue
    where
        id = p_id;
end;

-- Remove the record from the table. Used on recorde that failed to ack more
-- than allowed number of times.
create or replace procedure gopq_deleteItem(p_id int)
language plpgsql as
$$
declare
    x_item gopq_ackqueue.item%type;
begin
    select item
    into x_item
    from gopq_ackqueue
    where 
        id = p_id;

    delete from gopq_ackqueue
    where id = p_id;

    select @item as item from dual;
end;
$$;

-- Moves the record's deadline (into the future, deadline > now), thus putting
-- it back to the queue.
create or replace procedure gopq_updateForRetry(p_deadline int, p_id int)
begin atomic
    update gopq_ackqueue
    set 
        ack_deadline = p_deadline
        , retry_count = retry_count + 1
    where 
        id = p_id;
end;

-- Moves the record's deadline (into the future, deadline > now) but keeps the
-- retry counter.
create or replace procedure gopq_expireAckDeadline(p_deadline int, p_id int)
begin atomic
    update gopq_ackqueue
    set 
        ack_deadline = p_deadline
    where 
        id = p_id;
end;
