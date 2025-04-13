CREATE TABLE IF NOT EXISTS "sample".sample (
    id          BIGINT,
    CONSTRAINT pk_sample PRIMARY KEY (id)
);

--테이블 코멘트
COMMENT ON TABLE sample.sample IS '예시';

-- 컬럼 코멘트
COMMENT ON COLUMN sample.sample.id        IS '예시 아이디';