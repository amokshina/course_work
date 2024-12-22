create table if not exists sandbox.testing_s_AZS_address(
    coordinates UUID PRIMARY key not null,
    address JSON not null,
    hist_file_load_id int,
    dt_ins timestamptz default now())
   distributed by(coordinates);       

create table if not exists sandbox.testing_AZS_info(
    object_id BIGINT not null,
    title text not null,
    rating NUMERIC (2, 1) null,
    reviews_num INT null,
    work_hours_status text null,
    address text null,
    region text null,
    coordinates UUID not null,
    date_of_pars TIMESTAMPTZ not null,
    hist_file_load_id int)
   distributed by(object_id); 

create table if not exists sandbox.testing_AZS_review(
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
    hist_file_load_id int)
   distributed by(object_id);
 
CREATE TABLE if not exists sandbox.testing_s_azs_rev_users 
(
	author_id text not null primary key,
	author_name text not NULL,
	verified bool null,
	hist_file_load_id int,
	crc_change_row uuid,
	dt_ins timestamptz default now()
)
DISTRIBUTED BY (author_id);


create table if not exists sandbox.testing_s_AZS_categ(
    categ_id BIGINT PRIMARY key not null,
    title text not null,
    hist_file_load_id int,
    dt_ins timestamptz default now())
   distributed by(categ_id);

create table if not exists sandbox.testing_AZS_rev_categ(
    object_id BIGINT not null,
    categ_id BIGINT not null,
    reviews_num INT null,
    pos_rev_num INT null,
    neg_rev_num INT null,
    date_of_pars TIMESTAMPTZ not null,
    hist_file_load_id int)
   distributed by(object_id);      

-- буферные таблицы
create table if not exists sandbox.buf_s_AZS_address(
    coordinates UUID not null,
    address JSON not null,
    file_name_with_ts text, -- возможно надо оставить только в info
	src_id int default 713)
   distributed by(coordinates);      
  
create table if not exists sandbox.buf_AZS_info(
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
	src_id int default 713)
   distributed by(object_id); 
  
create table if not exists sandbox.buf_AZS_review(
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
    file_name_with_ts text, 
	src_id int default 713)
   distributed by(object_id);

CREATE TABLE if not exists sandbox.buf_s_azs_rev_users 
(
	author_id text not null,
	author_name text not NULL,
	verified bool null,
	file_name_with_ts text, -- возможно надо оставить только в review
	src_id int default 713
)
DISTRIBUTED BY (author_id);

create table if not exists sandbox.buf_s_AZS_categ(
    categ_id BIGINT PRIMARY key not null,
    title text not null,
    file_name_with_ts text, -- возможно надо оставить только в review
	src_id int default 713)
   distributed by(categ_id);


create table if not exists sandbox.buf_AZS_rev_categ(
    object_id BIGINT not null,
    categ_id BIGINT not null,
    reviews_num INT null,
    pos_rev_num INT null,
    neg_rev_num INT null,
    date_of_pars TIMESTAMPTZ not null,
    file_name_with_ts text, -- возможно надо оставить только в review
	src_id int default 713)
   distributed by(object_id);      
   

CREATE OR REPLACE FUNCTION sandbox.fn_load_testing_info()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare _hist_file_load_id int;
		_src_parent_id int;
		_file_path text;
        _file_name_with_ts text;
        _file_name text;
			
begin	
	select 	 buf.src_id			as src_parent_id 
			,file_name_with_ts	as file_name
			,file_name_with_ts	as file_name_with_ts
			,dsfp.file_path 	as file_path 
	from 	sandbox.buf_AZS_info buf
			left join ctr.data_source_file_path dsfp 
				on dsfp.src_parent_id  = buf.src_id 
	into	_src_parent_id,_file_name,_file_name_with_ts,_file_path
	;

	select ctr.fn_set_hist_file_load(
                                        _src_parent_id
                                        ,_file_path
                                        ,_file_name_with_ts
                                        ,null
                                        ,null
                                        ,null
                                        ,_file_name
                                    )
	into	_hist_file_load_id;
	
	WITH UniqueRecsAddr AS
	(
	    SELECT *, row_number () over(
	    	partition by buffer.coordinates
	    ) as uni
	    FROM sandbox.buf_s_AZS_address AS buffer
	)
	insert into sandbox.testing_s_AZS_address(coordinates, address, hist_file_load_id)
	select buffer.coordinates, buffer.address, _hist_file_load_id
	from UniqueRecsAddr as buffer 
	left join sandbox.testing_s_AZS_address as adr on buffer.coordinates = adr.coordinates -- проверка на дублирование
	where buffer.uni = 1 and adr.coordinates is null;
	
	
	WITH UniqueRecsInfo AS
	(
	    SELECT *, row_number () over(
	    	partition by buffer.object_id, buffer.date_of_pars
	    ) as uni
	    FROM sandbox.buf_AZS_info AS buffer
	)
	insert into sandbox.testing_AZS_info(object_id, title, rating, reviews_num, work_hours_status, address, region, coordinates, date_of_pars, hist_file_load_id)
	select buffer.object_id, buffer.title, buffer.rating, buffer.reviews_num, buffer.work_hours_status, 
				buffer.address, buffer.region, buffer.coordinates, buffer.date_of_pars, _hist_file_load_id
	from UniqueRecsInfo as buffer 
	join sandbox.testing_s_AZS_address as adr on buffer.coordinates = adr.coordinates -- проверка на внешний ключ
	left join sandbox.testing_AZS_info as inf on inf.object_id = buffer.object_id and inf.date_of_pars = buffer.date_of_pars
	where uni = 1 and (inf.object_id is null and inf.date_of_pars is null);
		
end; 
$$
  
  CREATE OR REPLACE FUNCTION sandbox.fn_load_testing_reviews()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare _hist_file_load_id int;
		_src_parent_id int;
		_file_path text;
        _file_name_with_ts text;
        _file_name text;
       
begin
    
	select 	 buf.src_id			as src_parent_id 
			,file_name_with_ts	as file_name
			,file_name_with_ts	as file_name_with_ts
			,dsfp.file_path 	as file_path 
	from 	sandbox.buf_AZS_review buf
			left join ctr.data_source_file_path dsfp -- ?
				on dsfp.src_parent_id  = buf.src_id 
	into	_src_parent_id,_file_name,_file_name_with_ts,_file_path
	;

	select ctr.fn_set_hist_file_load(
                                        _src_parent_id
                                        ,_file_path
                                        ,_file_name_with_ts
                                        ,null
                                        ,null
                                        ,null
                                        ,_file_name
                                    )
	into	_hist_file_load_id;

	-- Добавление в таблицу с информацией о пользователях
	perform ctr.fn_get_hash_row_v2('{author_id}'::text[],'{hist_file_load_id}'::text[],'sandbox','buf_s_azs_rev_users','_yrbebj');
	
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
		SELECT *, row_number () over(partition by buffer.author_id, j.crc_change_row) as uni
		FROM sandbox.buf_s_azs_rev_users  AS buffer
		left join cte_j j on j.j_author_id = buffer.author_id
	)
	-- сначала delete тех которые изменились 
	delete from sandbox.testing_s_azs_rev_users tt
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
		SELECT *, row_number () over(partition by buffer.author_id) as uni
		FROM sandbox.buf_s_azs_rev_users  AS buffer
		left join cte_j j on j.j_author_id = buffer.author_id
	)
	insert into sandbox.testing_s_azs_rev_users
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
			left join sandbox.testing_s_azs_rev_users tt
				on u.author_id = tt.author_id
	where 	uni = 1 and tt.author_id is null -- если не существует записи с таким id
	;	

	-- Добавление в таблицу со всеми отзывами
	WITH UniqueRecsRev AS
	(
	    SELECT *, row_number () over(
	    	partition by buffer.object_id, buffer.comment_time, buffer.author_id, buffer.date_of_pars 
	    ) as uni
	    FROM sandbox.buf_AZS_review AS buffer
	)
	insert into sandbox.testing_AZS_review(object_id, comment_type, comment_time, author_id, profession_level_num,
											rating, comment_text, likes_num, dislikes_num, date_of_pars, hist_file_load_id)
	select buffer.object_id, buffer.comment_type, buffer.comment_time, buffer.author_id, buffer.profession_level_num,
			buffer.rating, buffer.comment_text, buffer.likes_num, buffer.dislikes_num, buffer.date_of_pars, _hist_file_load_id
	from UniqueRecsRev as buffer 
	join sandbox.testing_AZS_info as inf on buffer.object_id = inf.object_id -- 
	left join sandbox.testing_s_AZS_rev_users as us on buffer.author_id = us.author_id 
	left join sandbox.testing_AZS_review as rev on (rev.object_id = buffer.object_id and
												 rev.comment_time = buffer.comment_time and 
												 rev.author_id = buffer.author_id and
												 rev.date_of_pars = buffer.date_of_pars)
	where uni = 1 and (rev.object_id is null and rev.comment_time is null and rev.author_id is null and rev.date_of_pars is null)
				  and (us.author_id is not null or copy_buffer.author_id = '0000');


	-- Добавление в таблицу со справочником категорий
	WITH UniqueRecsCat AS
	(
	    SELECT *, row_number () over(
	    	partition by buffer.categ_id
	    ) as uni
	    FROM sandbox.buf_s_AZS_categ AS buffer
	)
	insert into sandbox.testing_s_AZS_categ(categ_id, title, hist_file_load_id) 
	select buffer.categ_id, buffer.title, _hist_file_load_id
	from UniqueRecsCat as buffer
	left join sandbox.testing_s_AZS_categ as cat on cat.categ_id = buffer.categ_id
	where uni = 1 and cat.categ_id is null;
	
	-- Добавление в таблицу со статистикой по категориям
	WITH UniqueRecsRevCat AS
	(
	    SELECT *, row_number () over(
	    	partition by buffer.object_id, buffer.categ_id, buffer.date_of_pars
	    ) as uni
	    FROM sandbox.buf_AZS_rev_categ AS buffer
	)
	insert into sandbox.testing_AZS_rev_categ(object_id, categ_id, reviews_num, pos_rev_num, neg_rev_num, date_of_pars, hist_file_load_id) 
	select buffer.object_id, buffer.categ_id, buffer.reviews_num, buffer.pos_rev_num, buffer.neg_rev_num, buffer.date_of_pars, _hist_file_load_id
	from UniqueRecsRevCat as buffer
	join sandbox.testing_AZS_info as inf on buffer.object_id = inf.object_id
	join sandbox.testing_s_AZS_categ as cat on buffer.categ_id = cat.categ_id
	left join sandbox.testing_AZS_rev_categ as r_cat on (r_cat.object_id = buffer.object_id and 
													  r_cat.categ_id = buffer.categ_id and
													  r_cat.date_of_pars = buffer.date_of_pars)
	where uni = 1 and (r_cat.object_id is null and r_cat.categ_id is null and r_cat.date_of_pars is null);
	
end;
$$
 
