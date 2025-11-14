-- Unique queue implementation for Google Cloud MySql
--
-- One important issue is the absence of delete ... returning statement
--
-- 2025-11-14
create table gopq_queue (
    id integer not null auto_increment primary key,
    item blob(1024) not null,
    itemmd5 binary(16) as (unhex(md5(item))) stored,
    itemsha varchar(64) as (sha2(item, 256)) stored,
    enqueued_at timestamp default current_timestamp,
    processed_at timestamp,
    unique(itemmd5),
    unique(itemsha)
);

create procedure gopq_push(it blob(1024))
begin
    insert into gopq_queue (item) value (it) on duplicate key update item = it;
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

    update gopq_queue
    set processed_at = current_timestamp
    where id = @id;

    select @id as id, @item as item;
end;

-- Dequeue element from the queue and deletes the record. Helps keeping database
-- small and thus lowers the maintenance costs.
create procedure gopq_pop_delete()
begin
    select id, item
    into @id, @item
    from gopq_queue
    where 
        processed_at is null
    order by enqueued_at asc
    limit 1;

    delete from gopq_queue
    where id = @id;

    select @id as id, @item as item;
end;

create procedure gopq_len()
begin
    select count(1) from gopq_queue
    where processed_at is null;
end;
