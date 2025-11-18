-- Queue implementation for Google Cloud MySql
--
-- One important issue is the absence of delete ... returning statement
--
-- 2025-11-17
create table gopq_queue (
    id integer not null auto_increment primary key,
    item blob(1024) not null,
    enqueued_at timestamp default current_timestamp,
    processed_at timestamp
);

-- Inserts the item into the table.
create procedure gopq_push(it blob(1024))
begin
    insert into gopq_queue (item) value (it);
end;

-- Dequeue element from the queue. Element is left in the table but no longer
-- considered for any queue operation.
create procedure gopq_pop_store()
begin
    select id, item
    into @id, @item
    from gopq_queue
    where 
        processed_at is null
    order by enqueued_at asc
    limit 1;

    if found_rows() = 1 then
        update gopq_queue
        set processed_at = current_timestamp
        where id = @id;

        select @id as id, @item as item;
    end if;
end;

-- Dequeue element from the queue and deletes the record from the table. 
create procedure gopq_pop_delete()
begin
    select id, item
    into @id, @item
    from gopq_queue
    where 
        processed_at is null
    order by enqueued_at asc
    limit 1;

    if found_rows() = 1 then
        delete from gopq_queue
        where id = @id;

        select @id as id, @item as item;
    end if;
end;

-- Return the number of elements in the queue.
create procedure gopq_len()
begin
    select count(1) 
    from gopq_queue
    where processed_at is null;
end;
