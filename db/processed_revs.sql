 
  CREATE TABLE IF NOT EXISTS buffer.azs_review_analysis (
    review_hash TEXT,  -- Хэш отзыва
    comment_text text not null,    -- Ссылка на original отзыв
    clean_text text null,
    sentiment TEXT NULL,           -- Тональность (например, положительная, нейтральная, негативная)
    sentiment_score INT NULL,      -- Оценка тональности (2, 1, 0)
    keywords TEXT[] null,          -- Ключевые слова (массив)
    processed_at TIMESTAMPTZ NOT null default now()  -- Время последней обработки
);
  CREATE TABLE IF NOT EXISTS testing.azs_review_analysis (
    review_hash TEXT PRIMARY KEY,  -- Хэш отзыва
    comment_text text not null,    -- Ссылка на original отзыв
    clean_text text null,
    sentiment TEXT NULL,           -- Тональность (например, положительная, нейтральная, негативная)
    sentiment_score INT NULL,      -- Оценка тональности (2, 1, 0)
    keywords TEXT[] null,          -- Ключевые слова (массив)
    processed_at TIMESTAMPTZ NOT null default now()  -- Время последней обработки
);

select count(*) from buffer.azs_review_analysis;

select * from testing.azs_review ar ;
select * from testing.azs_review ar ;

select testing.fn_load_analyzed_reviews();

CREATE OR REPLACE FUNCTION testing.fn_load_analyzed_reviews()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
			
begin	

	WITH UniqueHashReviews AS
	(
	    SELECT *, row_number () over(
	    	partition by buf.review_hash
	    ) as uni
	    FROM buffer.azs_review_analysis AS buf
	)
	insert into testing.azs_review_analysis
	(
			review_hash, 
			comment_text, 
			clean_text, 
			sentiment, 
			sentiment_score, 
			keywords
	)
	select buf.review_hash, buf.comment_text, buf.clean_text, buf.sentiment, buf.sentiment_score, buf.keywords
	from UniqueHashReviews as buf
	left join testing.azs_review_analysis as ara on buf.review_hash = ara.review_hash
	where uni = 1 and ara.review_hash is null ;
	
end;
$$

select count(*) from buffer.azs_review_analysis;
select count(*) from testing.azs_review_analysis;

	