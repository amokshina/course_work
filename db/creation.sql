create database azs;

CREATE SCHEMA IF NOT EXISTS sandbox;
CREATE SCHEMA IF NOT EXISTS testing;
CREATE SCHEMA IF NOT EXISTS buffer;
CREATE SCHEMA IF NOT EXISTS ctr;

select * from buffer.azs_review;

create table if not exists testing.s_azs_address(
    coordinates UUID PRIMARY key not null,
    address JSON not null,
    hist_file_load_id int,
    dt_ins timestamptz default now());       

create table if not exists testing.azs_info(
    object_id BIGINT not null,
    title text not null,
    rating NUMERIC (2, 1) null,
    reviews_num INT null,
    work_hours_status text null,
    address text null,
    region text null,
    coordinates UUID not null,
    date_of_pars TIMESTAMPTZ not null,
    hist_file_load_id int); 

create table if not exists testing.azs_review(
    object_id BIGINT not null,
    comment_type text null,
    comment_time TIMESTAMP not null,
    author_id text not null,
    profession_level_num INT null,
    rating INT null,
    comment_text text null,
    likes_num INT null,
    dislikes_num INT null,
    date_of_pars TIMESTAMPTZ not null,
    hist_file_load_id int);
 
CREATE TABLE if not exists testing.s_azs_rev_users 
(
	author_id text not null primary key,
	author_name text not NULL,
	verified bool null,
	hist_file_load_id int,
	crc_change_row uuid,
	dt_ins timestamptz default now()
);


create table if not exists testing.s_azs_categ(
    categ_id BIGINT PRIMARY key not null,
    title text not null,
    hist_file_load_id int,
    dt_ins timestamptz default now());

create table if not exists testing.azs_rev_categ(
    object_id BIGINT not null,
    categ_id BIGINT not null,
    reviews_num INT null,
    pos_rev_num INT null,
    neg_rev_num INT null,
    date_of_pars TIMESTAMPTZ not null,
    hist_file_load_id int);      

-- буферные таблицы
   select * from buffer.azs_review;
  
  INSERT INTO buffer.azs_review
(column1)
VALUES('');
  
  commit;
  SELECT * FROM information_schema.tables;
 TRUNCATE TABLE buffer.s_azs_address CASCADE;
TRUNCATE TABLE buffer.s_azs_address cascade;
SELECT current_schema();
SELECT has_schema_privilege('buffer', 'USAGE');
SELECT has_table_privilege('buffer.s_azs_address', 'SELECT');
SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'buffer';
drop table buffer.s_azs_address;
SELECT grantee, privilege_type 
    FROM information_schema.role_table_grants 
    WHERE table_schema = 'buffer';
   
   SELECT * FROM information_schema.tables WHERE table_schema = 'buffer';
 SELECT *
FROM information_schema.role_table_grants
WHERE grantee = 'postgres' AND table_schema = 'buffer';

TRUNCATE TABLE "buffer"."azs_info" CASCADE

INSERT INTO buffer.s_azs_address (coordinates, address, file_name_with_ts)
VALUES (
    '550e8400-e29b-41d4-a716-446655440000',  -- UUID
    '{"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"}',  -- JSON адрес
    'data_550e8400-e29b-41d4-a716-446655440000_1234567890.json'  -- Имя файла с меткой времени
);

create table if not exists buffer.s_azs_address(
    coordinates UUID not null,
    address JSON not null,
    file_name_with_ts text);      
  
create table if not exists buffer.azs_info(
    object_id BIGINT not null,
    title text not null,
    rating NUMERIC (2, 1) null,
    reviews_num INT null,
    work_hours_status text null,
    address text null,
    region text null,
    coordinates UUID not null,
    date_of_pars TIMESTAMPTZ not null,
    file_name_with_ts text, 
	src_id int default 713); 
  
create table if not exists buffer.azs_review(
    object_id BIGINT not null,
    comment_type text null,
    comment_time TIMESTAMP not null,
    author_id text not null,
    profession_level_num INT null,
    rating INT null,
    comment_text text null,
    likes_num INT null,
    dislikes_num INT null,
    date_of_pars TIMESTAMPTZ not null,
    file_name_with_ts text);

CREATE TABLE if not exists buffer.s_azs_rev_users 
(
	author_id text not null,
	author_name text not NULL,
	verified bool null,
	file_name_with_ts text
);

create table if not exists buffer.s_azs_categ(
    categ_id BIGINT PRIMARY key not null,
    title text not null,
    file_name_with_ts text);

create table if not exists buffer.azs_rev_categ(
    object_id BIGINT not null,
    categ_id BIGINT not null,
    reviews_num INT null,
    pos_rev_num INT null,
    neg_rev_num INT null,
    date_of_pars TIMESTAMPTZ not null,
    file_name_with_ts text -- возможно надо оставить только в review
);      
   
create table if not exists ctr.reviews_file (
    file_id SERIAL PRIMARY KEY,          -- Уникальный идентификатор файла
    file_name TEXT NOT NULL,             -- Имя файла
    file_path TEXT NOT NULL,             -- Путь к файлу на диске
    date_pars TIMESTAMP,                 -- Дата парсинга
    date_added TIMESTAMP DEFAULT NOW(),  -- Дата добавления файла
    date_modified TIMESTAMP,             -- Дата изменения файла
    file_size BIGINT,                    -- Размер файла (опционально)
    file_type TEXT,                      -- Тип файла (например, CSV, JSON)
    UNIQUE (file_path, file_name)        -- Уникальность по имени и пути файла
);


CREATE OR REPLACE FUNCTION testing.fn_load__info()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare _hist_file_load_id int;
			
begin	
	select 	file_name_with_ts
	from 	buffer.azs_info buf left join ctr.reviews_file ctr on buf.file_name_with_ts = ctr.file_name
	into	_hist_file_load_id
	;
	
	WITH UniqueRecsAddr AS
	(
	    SELECT *, row_number () over(
	    	partition by buf.coordinates
	    ) as uni
	    FROM buffer.s_azs_address AS buf
	)
	insert into testing.s_azs_address(coordinates, address, hist_file_load_id)
	select buf.coordinates, buf.address, _hist_file_load_id
	from UniqueRecsAddr as buf 
	left join testing.s_azs_address as adr on buf.coordinates = adr.coordinates -- проверка на дублирование
	where buf.uni = 1 and adr.coordinates is null;
	
	
	WITH UniqueRecsInfo AS
	(
	    SELECT *, row_number () over(
	    	partition by buf.object_id, buf.date_of_pars
	    ) as uni
	    FROM buffer.azs_info AS buf
	)
	insert into testing.azs_info(object_id, title, rating, reviews_num, work_hours_status, address, region, coordinates, date_of_pars, hist_file_load_id)
	select buf.object_id, buf.title, buf.rating, buf.reviews_num, buf.work_hours_status, 
				buf.address, buf.region, buf.coordinates, buf.date_of_pars, _hist_file_load_id
	from UniqueRecsInfo as buf 
	join testing.s_azs_address as adr on buf.coordinates = adr.coordinates -- проверка на внешний ключ
	left join testing.azs_info as inf on inf.object_id = buf.object_id and inf.date_of_pars = buf.date_of_pars
	where uni = 1 and (inf.object_id is null and inf.date_of_pars is null);
		
end; 
$$
  
 CREATE OR REPLACE FUNCTION testing.fn_load__reviews()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
       
declare _hist_file_load_id int;
			
begin	
	select 	file_name_with_ts
	from 	buffer.azs_info buf left join ctr.reviews_file ctr on buf.file_name_with_ts = ctr.file_name
	into	_hist_file_load_id
	;

	-- Добавление в таблицу с информацией о пользователях
	perform ctr.fn_get_hash_row_v2('{author_id}'::text[],'{hist_file_load_id}'::text[],'buffer','s_azs_rev_users','_yrbebj');
	
	with cte_j
	as
	(
		select 	("_json_res"->>'pk_flds') as j_author_id
				,("_json_res"->>'hash_row')::uuid as crc_change_row
		from 	_yrbebj
	)
	,UniUsers 
	as 
	(
		SELECT *, row_number () over(partition by buf.author_id, j.crc_change_row) as uni
		FROM buffer.s_azs_rev_users AS buf
		left join cte_j j on j.j_author_id = buf.author_id
	)
	-- сначала delete тех которые изменились 
	delete from testing.s_azs_rev_users tt
	using UniUsers as u
	where u.author_id = tt.author_id
	and uni = 1 and tt.crc_change_row is distinct from u.crc_change_row and tt.author_id is not null; -- существует author_id и hash_row отличается
	
	with cte_j
	as
	(
		select 	("_json_res"->>'pk_flds') as j_author_id
				,("_json_res"->>'hash_row')::uuid as crc_change_row
		from 	_yrbebj
	)
	,UniUsers 
	as 
	(
		SELECT *, row_number () over(partition by buf.author_id) as uni
		FROM buffer.s_azs_rev_users AS buf
		left join cte_j j on j.j_author_id = buf.author_id
	)
	insert into testing.s_azs_rev_users
	(
		author_name
			,author_id
			,verified
			,hist_file_load_id
			,crc_change_row
	)
	select 	u.author_name
			,u.author_id
			,u.verified
			,_hist_file_load_id 
			,u.crc_change_row
	from  	UniUsers u
			left join testing._s_azs_rev_users tt
				on u.author_id = tt.author_id
	where 	uni = 1 and tt.author_id is null -- если не существует записи с таким id
	;	

	-- Добавление в таблицу со всеми отзывами
	WITH UniqueRecsRev AS
	(
	    SELECT *, row_number () over(
	    	partition by buf.object_id, buf.comment_time, buf.author_id, buf.date_of_pars 
	    ) as uni
	    FROM buffer.azs_review AS buf
	)
	insert into testing.azs_review(object_id, comment_type, comment_time, author_id, profession_level_num,
											rating, comment_text, likes_num, dislikes_num, date_of_pars, hist_file_load_id)
	select buf.object_id, buf.comment_type, buf.comment_time, buf.author_id, buf.profession_level_num,
			buf.rating, buf.comment_text, buf.likes_num, buf.dislikes_num, buf.date_of_pars, _hist_file_load_id
	from UniqueRecsRev as buf 
	join testing.фzs_info as inf on buf.object_id = inf.object_id -- 
	left join testing.s_azs_rev_users as us on buf.author_id = us.author_id 
	left join testing.azs_review as rev on (rev.object_id = buf.object_id and
												 rev.comment_time = buf.comment_time and 
												 rev.author_id = buf.author_id and
												 rev.date_of_pars = buf.date_of_pars)
	where uni = 1 and (rev.object_id is null and rev.comment_time is null and rev.author_id is null and rev.date_of_pars is null)
				  and (us.author_id is not null or copy_buffer.author_id = '0000'); -- что такое копи буф


	-- Добавление в таблицу со справочником категорий
	WITH UniqueRecsCat AS
	(
	    SELECT *, row_number () over(
	    	partition by buf.categ_id
	    ) as uni
	    FROM buffer.s_azs_categ AS buf
	)
	insert into testing.s_azs_categ(categ_id, title, hist_file_load_id) 
	select buf.categ_id, buf.title, _hist_file_load_id
	from UniqueRecsCat as buf
	left join testing.s_azs_categ as cat on cat.categ_id = buf.categ_id
	where uni = 1 and cat.categ_id is null;
	
	-- Добавление в таблицу со статистикой по категориям
	WITH UniqueRecsRevCat AS
	(
	    SELECT *, row_number () over(
	    	partition by buf.object_id, buf.categ_id, buf.date_of_pars
	    ) as uni
	    FROM buffer.azs_rev_categ AS buf
	)
	insert into testing.azs_rev_categ(object_id, categ_id, reviews_num, pos_rev_num, neg_rev_num, date_of_pars, hist_file_load_id) 
	select buf.object_id, buf.categ_id, buf.reviews_num, buf.pos_rev_num, buf.neg_rev_num, buf.date_of_pars, _hist_file_load_id
	from UniqueRecsRevCat as buf
	join testing.azs_info as inf on buf.object_id = inf.object_id
	join testing.s_azs_categ as cat on buf.categ_id = cat.categ_id
	left join testing.azs_rev_categ as r_cat on (r_cat.object_id = buf.object_id and 
													  r_cat.categ_id = buf.categ_id and
													  r_cat.date_of_pars = buf.date_of_pars)
	where uni = 1 and (r_cat.object_id is null and r_cat.categ_id is null and r_cat.date_of_pars is null);
	
end;
$$
 
