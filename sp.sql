ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';


CREATE OR REPLACE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA (
    CREDIT_DOC_ID          VARCHAR(100) PRIMARY KEY,
    ORIGINAL_DOCUMENT_ID   VARCHAR(100),                   -- optional link to the original upload id
    SECTION_TYPE           VARCHAR(10) NOT NULL ,
    SOURCE_PAGE_RANGES     VARCHAR(200),                   -- e.g., '1-3,5,7-9'
    ORIGINAL_FILENAME      VARCHAR(500),
    UPLOAD_TIMESTAMP       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_SIZE_BYTES        NUMBER,
    FILE_TYPE              VARCHAR(50),
    PROCESSING_STATUS      VARCHAR(50) DEFAULT 'UPLOADED',
    STAGE_PATH             VARCHAR(500)                    -- e.g., credit_uploads/CAM/xyz.pdf
);


CREATE OR REPLACE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT (
EXTRACTION_ID VARCHAR(100) PRIMARY KEY,
CREDIT_DOC_ID VARCHAR(100),
SECTION_TYPE VARCHAR(10) NOT NULL,
EXTRACTION_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
EXTRACTION_MODE VARCHAR(20),
EXTRACTED_CONTENT TEXT,
PAGE_COUNT NUMBER,
TOKEN_COUNT NUMBER,
PROCESSING_COST_ESTIMATE NUMBER(10,2),
METADATA VARIANT,
DOCUMENT_NAME VARCHAR(500),
FOREIGN KEY (CREDIT_DOC_ID) REFERENCES DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA(CREDIT_DOC_ID)
);

CREATE OR REPLACE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON (
OUTPUT_ID VARCHAR(100) PRIMARY KEY,
CREDIT_DOC_ID VARCHAR(100),
SECTION_TYPE VARCHAR(10),
DOCUMENT_NAME VARCHAR(500),
AGENT_NAME VARCHAR(200),
RAW_JSON VARIANT,
CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
FOREIGN KEY (CREDIT_DOC_ID) REFERENCES DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA(CREDIT_DOC_ID)
);

-- Optional: a convenience VIEW that mirrors your original tables' shape
CREATE OR REPLACE VIEW DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT_VW AS
SELECT 
    e.EXTRACTION_ID,
    e.CREDIT_DOC_ID AS DOCUMENT_ID,
    e.EXTRACTION_TIMESTAMP,
    e.EXTRACTION_MODE,
    e.EXTRACTED_CONTENT,
    e.PAGE_COUNT,
    e.TOKEN_COUNT,
    e.PROCESSING_COST_ESTIMATE,
    e.METADATA,
    m.SECTION_TYPE,
    m.SOURCE_PAGE_RANGES,
    m.STAGE_PATH,
    m.ORIGINAL_FILENAME
FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT e
JOIN DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA m
  ON m.CREDIT_DOC_ID = e.CREDIT_DOC_ID;

-- ─────────────────────────────────────────────────────────────────────────────
-- STORED PROCEDURE
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE DOC_PROCESSING_DB.DOC_AI_SCHEMA.PROCESS_CREDIT_DOC(
CREDIT_DOC_ID VARCHAR,
PROCESSING_MODE VARCHAR DEFAULT 'LAYOUT'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
v_stage_path STRING;
v_mode STRING;
v_result VARIANT;
v_extracted_content STRING;
v_page_count NUMBER;
v_token_estimate NUMBER;
v_cost_estimate NUMBER;
v_extraction_id STRING;
v_section_type STRING;
v_document_name STRING;
BEGIN
SELECT STAGE_PATH, SECTION_TYPE, ORIGINAL_FILENAME
INTO v_stage_path, v_section_type, v_document_name
FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA
WHERE CREDIT_DOC_ID = :CREDIT_DOC_ID;


IF (v_stage_path IS NULL) THEN
RETURN 'Credit document not found in metadata table';
END IF;


v_mode := IFF(UPPER(:PROCESSING_MODE) IN ('OCR','LAYOUT'), UPPER(:PROCESSING_MODE), 'LAYOUT');


SELECT AI_PARSE_DOCUMENT(
TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_stage_path),
OBJECT_CONSTRUCT('mode', :v_mode, 'page_split', FALSE)
)::VARIANT
INTO v_result;


v_extracted_content := v_result:"content"::STRING;
v_page_count := COALESCE(v_result:"metadata":"pageCount"::NUMBER, 1);
v_token_estimate := v_page_count * 970;
v_cost_estimate := v_token_estimate * 0.0001;
v_extraction_id := UUID_STRING();


INSERT INTO DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT
(EXTRACTION_ID, CREDIT_DOC_ID, SECTION_TYPE, EXTRACTION_MODE, EXTRACTED_CONTENT,
PAGE_COUNT, TOKEN_COUNT, PROCESSING_COST_ESTIMATE, METADATA, EXTRACTION_TIMESTAMP,
DOCUMENT_NAME)
SELECT :v_extraction_id, :CREDIT_DOC_ID, :v_section_type, :v_mode, :v_extracted_content,
:v_page_count, :v_token_estimate, :v_cost_estimate, :v_result, CURRENT_TIMESTAMP(),
:v_document_name;


UPDATE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA
SET PROCESSING_STATUS = 'PROCESSED'
WHERE CREDIT_DOC_ID = :CREDIT_DOC_ID;


RETURN 'Processed OK (CREDIT): ' || :v_page_count || ' pages, ~' || :v_token_estimate || ' tokens.';
EXCEPTION
WHEN OTHER THEN
UPDATE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA
SET PROCESSING_STATUS = 'ERROR'
WHERE CREDIT_DOC_ID = :CREDIT_DOC_ID;
RETURN 'Error: ' || SQLERRM;
END;
$$;


CREATE OR REPLACE PROCEDURE DOC_PROCESSING_DB.DOC_AI_SCHEMA.SAVE_CREDIT_SECTION_JSON(
CREDIT_DOC_ID STRING,
SECTION_TYPE STRING,
DOCUMENT_NAME STRING,
AGENT_NAME STRING,
JSON_TEXT STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
v_json VARIANT;
v_out_id STRING;
BEGIN
v_json := TRY_PARSE_JSON(JSON_TEXT);
IF (v_json IS NULL) THEN
RETURN 'ERROR: invalid JSON';
END IF;


v_out_id := UUID_STRING();


INSERT INTO DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON
(OUTPUT_ID, CREDIT_DOC_ID, SECTION_TYPE, DOCUMENT_NAME, AGENT_NAME, RAW_JSON)
SELECT :v_out_id, :CREDIT_DOC_ID, :SECTION_TYPE, :DOCUMENT_NAME, :AGENT_NAME, :v_json;


RETURN 'OK:' || :v_out_id;
END;
$$;


-- Privileges (adjust role names)
GRANT USAGE, EXECUTE ON PROCEDURE DOC_PROCESSING_DB.DOC_AI_SCHEMA.SAVE_CREDIT_SECTION_JSON(STRING, STRING, STRING, STRING, STRING) TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT ON TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON TO ROLE ACCOUNTADMIN;

SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT
TRUNCATE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT
SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON
TRUNCATE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON



CREATE OR REPLACE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_VALIDATION (
  VALIDATION_ID     VARCHAR(100) PRIMARY KEY,
  CREDIT_DOC_ID     VARCHAR(100),
  SECTION_TYPE      VARCHAR(10),                -- 'CAM' or 'CVF'
  DOCUMENT_NAME     VARCHAR(500),
  IMAGE_STAGE_PATHS VARIANT,                    -- array of stage-relative image paths
  RAW_JSON          VARIANT,                    -- the original extracted JSON you validated
  RESULT_JSON       VARIANT,                    -- validation outcome JSON (model-produced)
  PASS_COUNT        NUMBER,                     -- optional summary metrics parsed from RESULT_JSON
  FAIL_COUNT        NUMBER,                     -- optional summary metrics parsed from RESULT_JSON
  CREATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (CREDIT_DOC_ID) REFERENCES DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_DOCUMENT_METADATA(CREDIT_DOC_ID)
);

CREATE OR REPLACE PROCEDURE DOC_PROCESSING_DB.DOC_AI_SCHEMA.RUN_CREDIT_VALIDATION(
  CREDIT_DOC_ID      STRING,
  SECTION_TYPE       STRING,             -- 'CAM' or 'CVF'
  DOCUMENT_NAME      STRING,
  IMAGE_PATHS_JSON   STRING,             -- stringified JSON array of stage-relative paths under DOC_INPUT
  RAW_JSON_TEXT      STRING              -- stringified JSON to validate
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_n             INTEGER;
  v_placeholders  STRING;
  v_prompt_tmpl   STRING;
  v_arglist       STRING;
  v_sql           STRING;
  v_rs            RESULTSET;
  v_resp          VARIANT;
  v_text          STRING;
  v_result        VARIANT;
  v_id            STRING;
  v_pass          NUMBER;
  v_fail          NUMBER;
BEGIN
  -- Count images safely from the INPUT param
  SELECT IFF(TRY_PARSE_JSON(:IMAGE_PATHS_JSON) IS NULL, 0,
             ARRAY_SIZE(TRY_PARSE_JSON(:IMAGE_PATHS_JSON)))
    INTO :v_n;

  IF (v_n = 0) THEN
    RETURN OBJECT_CONSTRUCT('status','ERROR',
                            'reason','IMAGE_PATHS_JSON must be a non-empty JSON array of stage-relative paths');
  END IF;

  -- Build "{0}, {1}, ..., {N-1}"
  SELECT LISTAGG('{' || TO_VARCHAR(seq4()) || '}', ', ')
           WITHIN GROUP (ORDER BY seq4())
    INTO :v_placeholders
    FROM TABLE(GENERATOR(ROWCOUNT => :v_n));

  -- Prompt text (matches your working query)
  v_prompt_tmpl :=
      'Validate documents ' || v_placeholders ||
      ' against the JSON data in {' || v_n::STRING || '}. ' ||
      'Analyze all images and cross-reference the data. ' ||
      'Return validation results as JSON with pass/fail counts.';

  -- Build TO_FILE(...) list for each image, then append the JSON string
  SELECT LISTAGG(
           'TO_FILE(''' || '@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT' || ''', ''' ||
           REPLACE(value::string, '''', '''''') || ''')',
           ', '
         )
    INTO :v_arglist
    FROM TABLE(FLATTEN(INPUT => TRY_PARSE_JSON(:IMAGE_PATHS_JSON)));

  v_arglist := v_arglist || ', ' || '''' || REPLACE(RAW_JSON_TEXT, '''', '''''') || '''';

  -- Call AI_COMPLETE with PROMPT (exact shape you tested)
  v_sql := 'SELECT AI_COMPLETE(''openai-gpt-4.1'', ' ||
           'PROMPT(''' || REPLACE(v_prompt_tmpl, '''', '''''') || ''', ' ||
            v_arglist || '))::VARIANT';

  v_rs := (EXECUTE IMMEDIATE :v_sql);
  LET c CURSOR FOR v_rs; OPEN c; FETCH c INTO v_resp; CLOSE c;

  -- Extract text and parse JSON
  v_text := COALESCE(
              v_resp:"response"::STRING,
              v_resp:"choices"[0]:"message":"content"::STRING,
              v_resp::STRING
            );

  v_result := TRY_PARSE_JSON(v_text);
  IF (v_result IS NULL) THEN
    v_result := OBJECT_CONSTRUCT('raw_text', v_text);
  END IF;

  v_pass := TRY_TO_NUMBER(v_result:"pass_count");
  v_fail := TRY_TO_NUMBER(v_result:"fail_count");

  -- Persist (store parsed image list straight from the param)
  v_id := UUID_STRING();

  INSERT INTO DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_VALIDATION
    (VALIDATION_ID, CREDIT_DOC_ID, SECTION_TYPE, DOCUMENT_NAME,
     IMAGE_STAGE_PATHS, RAW_JSON, RESULT_JSON, PASS_COUNT, FAIL_COUNT)
  SELECT :v_id, :CREDIT_DOC_ID, :SECTION_TYPE, :DOCUMENT_NAME,
         TRY_PARSE_JSON(:IMAGE_PATHS_JSON), TRY_PARSE_JSON(:RAW_JSON_TEXT),
         :v_result, :v_pass, :v_fail;

  RETURN OBJECT_CONSTRUCT('status','OK','validation_id', v_id, 'result', v_result);
END;
$$;


SELECT AI_COMPLETE( 'openai-gpt-4.1', PROMPT( 'Validate documents {0}, {1}, and {2} against the JSON data in {3}. Analyze all three images and cross-reference the data. Return validation results as JSON with pass/fail counts.', TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', 'credit_images/CAM/Baldos_Melani_B_000-400-00086-1_250K_-_Credit_Files__CAM_p2.png'), TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', 'credit_images/CAM/Baldos_Melani_B_000-400-00086-1_250K_-_Credit_Files__CAM_p3.png'), TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', 'credit_images/CAM/Baldos_Melani_B_000-400-00086-1_250K_-_Credit_Files__CAM_p4.png'), '{"Basic Info":{"borrower_name":"MELANI BELMONTE BALDOS"},"Loan Application":{"Amount recommended by analyst":"P250K@24Mos.","loan_purpose":"Personal Consumption - financial service activities. except insurance and pension funding activities","ltv":"67%","type_of_loan":"Car Refinancing"}}' ) ) AS validation_result_3_images;


select relative_path, size, last_modified
from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT)
where relative_path ilike 'credit_images/%'
order by relative_path;

select
  csj.CREDIT_DOC_ID,
  csj.SECTION_TYPE,
  csj.DOCUMENT_NAME,
  d.RELATIVE_PATH
from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON csj
join directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
  on upper(split_part(d.relative_path, '/', 2)) = upper(csj.SECTION_TYPE)
where d.relative_path ilike 'credit_images/%'
order by csj.CREDIT_DOC_ID, csj.SECTION_TYPE, d.relative_path;


with files as (
  select
      d.relative_path,
      split_part(d.relative_path,'/',2) as section_type,          -- CAM / CVF
      split_part(d.relative_path,'/',3) as filename,
      regexp_replace(upper(split_part(d.relative_path,'/',3)),'[^A-Z0-9]','') as file_norm
  from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
  where d.relative_path ilike 'credit_images/%'
),
docs as (
  select
      csj.CREDIT_DOC_ID,
      csj.SECTION_TYPE,
      csj.DOCUMENT_NAME,
      regexp_replace(upper(csj.DOCUMENT_NAME),'[^A-Z0-9]','') as doc_norm
  from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON csj
)
select
  d.CREDIT_DOC_ID,
  d.SECTION_TYPE,
  d.DOCUMENT_NAME,
  f.RELATIVE_PATH
from docs d
join files f
  on upper(f.section_type) = upper(d.section_type)
 and f.file_norm like '%' || substr(d.doc_norm, 1, 18) || '%'   -- flexible match
order by d.CREDIT_DOC_ID, d.SECTION_TYPE, f.RELATIVE_PATH;

with files as (
  select
      d.relative_path,
      split_part(d.relative_path,'/',2) as section_type,
      split_part(d.relative_path,'/',3) as filename,
      regexp_replace(upper(split_part(d.relative_path,'/',3)),'[^A-Z0-9]','') as file_norm
  from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
  where d.relative_path ilike 'credit_images/%'
),
docs as (
  select
      csj.CREDIT_DOC_ID,
      csj.SECTION_TYPE,
      csj.DOCUMENT_NAME,
      regexp_replace(upper(csj.DOCUMENT_NAME),'[^A-Z0-9]','') as doc_norm
  from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON csj
)
select *
from (
  select
    d.CREDIT_DOC_ID,
    d.SECTION_TYPE,
    d.DOCUMENT_NAME,
    f.RELATIVE_PATH,
    row_number() over (
      partition by d.CREDIT_DOC_ID, d.SECTION_TYPE
      order by f.filename
    ) as rn
  from docs d
  join files f
    on upper(f.section_type) = upper(d.section_type)
   and f.file_norm like '%' || substr(d.doc_norm, 1, 18) || '%'
)
where rn <= 3
order by CREDIT_DOC_ID, SECTION_TYPE, RELATIVE_PATH;


with files as (
  select
      d.relative_path,
      split_part(d.relative_path,'/',2) as section_type,
      split_part(d.relative_path,'/',3) as filename,
      regexp_replace(upper(split_part(d.relative_path,'/',3)),'[^A-Z0-9]','') as file_norm
  from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
  where d.relative_path ilike 'credit_images/%'
),
docs as (
  select
      csj.CREDIT_DOC_ID,
      csj.SECTION_TYPE,
      csj.DOCUMENT_NAME,
      csj.RAW_JSON,
      regexp_replace(upper(csj.DOCUMENT_NAME),'[^A-Z0-9]','') as doc_norm
  from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON csj
),
picked as (
  select *
  from (
    select
      d.CREDIT_DOC_ID,
      d.SECTION_TYPE,
      d.DOCUMENT_NAME,
      d.RAW_JSON,
      f.relative_path,
      row_number() over (
        partition by d.CREDIT_DOC_ID, d.SECTION_TYPE
        order by f.filename
      ) as rn
    from docs d
    join files f
      on upper(f.section_type) = upper(d.section_type)
     and f.file_norm like '%' || substr(d.doc_norm, 1, 18) || '%'
  )
  where rn <= 3
),
pvt as (
  select
    CREDIT_DOC_ID,
    SECTION_TYPE,
    max(case when rn=1 then relative_path end) as f1,
    max(case when rn=2 then relative_path end) as f2,
    max(case when rn=3 then relative_path end) as f3,
    any_value(RAW_JSON) as RAW_JSON
  from picked
  group by CREDIT_DOC_ID, SECTION_TYPE
)
select
  CREDIT_DOC_ID,
  SECTION_TYPE,                 -- CAM / CVF (validated separately)
  AI_COMPLETE(
    'openai-gpt-4.1',
    PROMPT(
      'Validate documents {0}, {1}, and {2} against the JSON data in {3}. ' ||
      'Analyze all three images and cross-reference the data. ' ||
      'Return validation results as JSON with pass/fail counts.',
      TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', f1),
      TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', f2),
      TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', f3),
      TO_VARCHAR(RAW_JSON)
    )
  ) as validation_result
from pvt
where f1 is not null and f2 is not null and f3 is not null
order by CREDIT_DOC_ID, SECTION_TYPE;

create or replace procedure DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION(
    P_CREDIT_DOC_ID   string,          -- which doc to process
    P_SECTION_TYPE    string,          -- 'CAM' or 'CVF'
    P_MAX_IMAGES      number           -- use up to N images (1..5)
)
returns variant
language sql
execute as caller
as
$$
declare
  v_json         variant;
  v_doc_name     string;
  v_cnt          number;
  v_f1           string;
  v_f2           string;
  v_f3           string;
  v_f4           string;
  v_f5           string;
  v_result       variant;
begin
  -- 0) Guardrails
  if (P_MAX_IMAGES is null or P_MAX_IMAGES < 1) then
     set P_MAX_IMAGES := 5;
  elseif (P_MAX_IMAGES > 5) then
     set P_MAX_IMAGES := 5;
  end if;

  -- 1) Pull the JSON + document name for the requested doc/section
  select RAW_JSON, DOCUMENT_NAME
    into :v_json, :v_doc_name
  from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON
  where CREDIT_DOC_ID = :P_CREDIT_DOC_ID
    and upper(SECTION_TYPE) = upper(:P_SECTION_TYPE)
  limit 1;

  if (v_json is null) then
    return object_construct(
      'status','not_found',
      'message','No row in CREDIT_SECTION_JSON for given CREDIT_DOC_ID and SECTION_TYPE'
    );
  end if;

  -- 2) Build a normalized pattern from DOCUMENT_NAME and grab up to N matching files
  create or replace temp table _picked as
  with files as (
    select
      d.relative_path,
      split_part(d.relative_path,'/',2) as section_type,          -- CAM/CVF
      split_part(d.relative_path,'/',3) as filename,
      regexp_replace(upper(split_part(d.relative_path,'/',3)),'[^A-Z0-9]','') as file_norm
    from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
    where d.relative_path ilike 'credit_images/%'
  )
  , doc as (
    select
      regexp_replace(upper(:v_doc_name),'[^A-Z0-9]','') as doc_norm
  )
  select relative_path, filename,
         row_number() over (order by filename) as rn
  from files f
  join doc d
    on upper(f.section_type) = upper(:P_SECTION_TYPE)
   and f.file_norm like '%' || substr(d.doc_norm, 1, 18) || '%'
  qualify rn <= :P_MAX_IMAGES;

  -- 3) Count and pivot to f1..f5
  select count(*) into :v_cnt from _picked;

  if (v_cnt = 0) then
    return object_construct(
      'status','no_files',
      'credit_doc_id', :P_CREDIT_DOC_ID,
      'section_type',  :P_SECTION_TYPE,
      'message','No matching files found in stage for this doc/section'
    );
  end if;

  select
    max(case when rn=1 then relative_path end),
    max(case when rn=2 then relative_path end),
    max(case when rn=3 then relative_path end),
    max(case when rn=4 then relative_path end),
    max(case when rn=5 then relative_path end)
  into :v_f1, :v_f2, :v_f3, :v_f4, :v_f5
  from _picked;

  -- 4) Call AI_COMPLETE with the right arity (1..5 images)
  if (v_cnt = 1) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate document {0} against the JSON data in {1}. Return validation results as JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 2) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0} and {1} against the JSON data in {2}. Return validation results as JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 3) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0}, {1}, and {2} against the JSON data in {3}. Analyze all images and return JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 4) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0}, {1}, {2}, and {3} against the JSON data in {4}. Analyze all images and return JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f4),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  else  -- 5 or more (we capped P_MAX_IMAGES at 5)
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0}, {1}, {2}, {3}, and {4} against the JSON data in {5}. Analyze all images and return JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f4),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f5),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;
  end if;

  -- 5) Return structured output
  return object_construct(
    'status','ok',
    'credit_doc_id', :P_CREDIT_DOC_ID,
    'section_type',  :P_SECTION_TYPE,
    'images_used',   :v_cnt,
    'files', array_construct_compact(:v_f1,:v_f2,:v_f3,:v_f4,:v_f5),
    'result', :v_result
  );
end;
$$;

-- Example: process CAM for this CREDIT_DOC_ID (up to 5 images)
call DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION(
  'c35c24abc48c418189c7eccfbf185dfc',  -- CREDIT_DOC_ID to process
  'CVF',                                -- 'CAM' or 'CVF'
  5                                      -- max images to use (1..5)
);

-- Example: process CAM for this CREDIT_DOC_ID (up to 5 images)
call DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION(
  'cea7ef931799489fb34307bf3bbdb97e',  -- CREDIT_DOC_ID to process
  'CAM',                                -- 'CAM' or 'CVF'
  5                                      -- max images to use (1..5)
);

SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON

-- Example: process CVF
call DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION('c35c24abc48c418189c7eccfbf185dfc','CVF',5);

alter user HARI
  set default_namespace = 'DOC_PROCESSING_DB.DOC_AI_SCHEMA';


create or replace procedure DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION(
    P_CREDIT_DOC_ID   string,   -- doc to process
    P_SECTION_TYPE    string,   -- 'CAM' or 'CVF'
    P_MAX_IMAGES      number    -- 1..5
)
returns variant
language sql
execute as caller
as
$$
declare
  v_json     variant;
  v_doc_name string;
  v_cnt      number;
  v_f1       string;
  v_f2       string;
  v_f3       string;
  v_f4       string;
  v_f5       string;
  v_result   variant;
begin
  -- cap images 1..5
  if (P_MAX_IMAGES is null or P_MAX_IMAGES < 1) then set P_MAX_IMAGES := 5;
  elseif (P_MAX_IMAGES > 5) then set P_MAX_IMAGES := 5; end if;

  -- fetch JSON + doc name
  select RAW_JSON, DOCUMENT_NAME
    into :v_json, :v_doc_name
  from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON
  where CREDIT_DOC_ID = :P_CREDIT_DOC_ID
    and upper(SECTION_TYPE) = upper(:P_SECTION_TYPE)
  limit 1;

  if (v_json is null) then
    return object_construct('status','not_found',
                            'message','No CREDIT_SECTION_JSON row for given CREDIT_DOC_ID/SECTION_TYPE');
  end if;

  -- collect up to N files into array, then unpack into v_f1..v_f5
  with files as (
    select
      d.relative_path,
      split_part(d.relative_path,'/',2) as section_type,
      split_part(d.relative_path,'/',3) as filename,
      regexp_replace(upper(split_part(d.relative_path,'/',3)),'[^A-Z0-9]','') as file_norm
    from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
    where d.relative_path ilike 'credit_images/%'
  ),
  doc as (
    select regexp_replace(upper(:v_doc_name),'[^A-Z0-9]','') as doc_norm
  ),
  picked as (
    select relative_path
    from files f
    join doc d
      on upper(f.section_type) = upper(:P_SECTION_TYPE)
     and f.file_norm like '%' || substr(d.doc_norm, 1, 18) || '%'
    order by f.filename
    limit :P_MAX_IMAGES
  ),
  pack as (
    select
      array_agg(relative_path) as paths,
      array_size(array_agg(relative_path)) as cnt
    from picked
  )
  select
    cnt,
    iff(cnt>=1, paths[0], null),
    iff(cnt>=2, paths[1], null),
    iff(cnt>=3, paths[2], null),
    iff(cnt>=4, paths[3], null),
    iff(cnt>=5, paths[4], null)
  into :v_cnt, :v_f1, :v_f2, :v_f3, :v_f4, :v_f5
  from pack;

  if (v_cnt = 0) then
    return object_construct('status','no_files',
                            'credit_doc_id', :P_CREDIT_DOC_ID,
                            'section_type',  :P_SECTION_TYPE,
                            'message','No matching files found in stage');
  end if;

  -- call AI_COMPLETE with correct arity
  if (v_cnt = 1) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate document {0} against the JSON data in {1}. Return validation results as JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 2) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0} and {1} against the JSON data in {2}. Return validation results as JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 3) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0}, {1}, and {2} against the JSON data in {3}. Analyze all images and return JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 4) then
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0}, {1}, {2}, and {3} against the JSON data in {4}. Analyze all images and return JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f4),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  else
    select AI_COMPLETE(
      'openai-gpt-4.1',
      PROMPT(
        'Validate documents {0}, {1}, {2}, {3}, and {4} against the JSON data in {5}. Analyze all images and return JSON with pass/fail counts.',
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f4),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f5),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;
  end if;

  return object_construct(
    'status','ok',
    'credit_doc_id', :P_CREDIT_DOC_ID,
    'section_type',  :P_SECTION_TYPE,
    'images_used',   :v_cnt,
    'files', array_construct_compact(:v_f1,:v_f2,:v_f3,:v_f4,:v_f5),
    'result', :v_result
  );
end;
$$;


-- CAM values + per-field reasons/issues (as JSON maps) + raw result
create or replace table DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CAM (
  CREDIT_DOC_ID string not null,
  SECTION_TYPE  string default 'CAM',
  loan_type string,
  agent_name string,
  handling_loan_manager string,
  handling_loan_evaluator string,
  loan_facility string,
  loan_industry string,
  amount_recommended_loan_manager string,
  amount_recommended_loan_evaluator string,
  amount_approved_by_analyst string,
  date_of_birth string,
  civil_status string,
  highest_educational_attainment_borrower string,
  highest_educational_attainment_spouse string,
  occupation_history_borrower string,
  occupation_history_spouse string,
  related_to_business_soi_borrower string,
  related_to_business_soi_spouse string,
  email_address string,
  landline_mobile_no string,
  other_gov_issued_id string,
  name_of_spouse string,
  spouse_date_of_birth string,
  mothers_maiden_name string,
  no_of_dependents string,
  children_name_age string,
  parent_siblings_name string,
  parent_siblings_address string,
  REASONS variant,   -- map: field -> reason text
  ISSUES  variant,   -- map: field -> array of strings
  RESULT_JSON variant,
  CREATED_AT timestamp_ltz default current_timestamp()
);

-- CVF
create or replace table DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CVF (
  CREDIT_DOC_ID string not null,
  SECTION_TYPE  string default 'CVF',
  borrower_type string,
  Amount_recommended_by_analyst string,
  loan_purpose string,
  cic_contract_type string,
  type_of_loan string,
  ltv string,
  borrower_name string,
  REASONS variant,   -- map: field -> reason text
  ISSUES  variant,   -- map: field -> array of strings
  RESULT_JSON variant,
  CREATED_AT timestamp_ltz default current_timestamp()
);


create or replace procedure DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION(
    P_CREDIT_DOC_ID   string,   -- doc to process
    P_SECTION_TYPE    string,   -- 'CAM' or 'CVF'
    P_MAX_IMAGES      number    -- 1..5
)
returns variant
language sql
execute as caller
as
$$
declare
  v_json       variant;
  v_doc_name   string;
  v_cnt        number;
  v_f1         string;
  v_f2         string;
  v_f3         string;
  v_f4         string;
  v_f5         string;
  v_result     variant;   -- raw response from AI_COMPLETE
  v_parsed     variant;   -- parsed JSON returned by the model
  v_prompt     string;    -- section-specific instruction
begin
  -- cap images 1..5
  if (P_MAX_IMAGES is null or P_MAX_IMAGES < 1) then set P_MAX_IMAGES := 5;
  elseif (P_MAX_IMAGES > 5) then set P_MAX_IMAGES := 5; end if;

  -- fetch JSON + doc name to drive file matching
  select RAW_JSON, DOCUMENT_NAME
    into :v_json, :v_doc_name
  from DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON
  where CREDIT_DOC_ID = :P_CREDIT_DOC_ID
    and upper(SECTION_TYPE) = upper(:P_SECTION_TYPE)
  limit 1;

  if (v_json is null) then
    return object_construct('status','not_found',
                            'message','No CREDIT_SECTION_JSON row for given CREDIT_DOC_ID/SECTION_TYPE');
  end if;

  -- section-specific instructions WITHOUT any literal curly braces
  if (upper(P_SECTION_TYPE) = 'CAM') then
    set v_prompt := '
You will receive up to five page images from a Credit Application Memorandum and one reference JSON.
Extract only these keys (use exact key names): 
loan_type, agent_name, handling_loan_manager, handling_loan_evaluator, loan_facility, loan_industry,
amount_recommended_loan_manager, amount_recommended_loan_evaluator, amount_approved_by_analyst,
date_of_birth, civil_status, highest_educational_attainment_borrower, highest_educational_attainment_spouse,
occupation_history_borrower, occupation_history_spouse, related_to_business_soi_borrower, related_to_business_soi_spouse,
email_address, landline_mobile_no, other_gov_issued_id, name_of_spouse, spouse_date_of_birth, mothers_maiden_name,
no_of_dependents, children_name_age, parent_siblings_name, parent_siblings_address.

Use these cues when reading the images:
- loan_type: from the label Credit facility 2nd line (values like New, Additional, Restructured, Renewal).
- agent_name: from labels Referred By, Agent Name, Loan Agent, or Name (the agent context).
- handling_loan_manager: from Loan Manager, Loan Officer, or Marketing Officer.
- handling_loan_evaluator: from Loan Evaluator.
- loan_facility: from Credit Facility.
- loan_industry: from Loan Industry.
- amount_recommended_loan_manager: from Amount Recommended (LM) or a Loan Officer amount.
- amount_recommended_loan_evaluator: from Amount Recommended (LE).
- amount_approved_by_analyst: from CRECOM A or B or C handwritten approval amount.
- mothers_maiden_name: note the apostrophe in Mother''s is part of the label.

For each key output four fields:
- value: value extracted from the images or null if not visible.
- expected: value for the same key from the provided reference JSON if present, otherwise null.
- reason: short explanation of where you found it (label or region) and how it was interpreted.
- issues: array of short strings describing anomalies such as missing, unreadable, ambiguous, or mismatch with expected.

Return one compact JSON object only, no prose. 
Top-level keys must be: section with value CAM, and fields which is a map from key name to the object described above.';
  else
    set v_prompt := '
You will receive up to five page images from a Credit Verification Form and one reference JSON.
Extract only these keys (use exact key names): 
borrower_type, Amount_recommended_by_analyst, loan_purpose, cic_contract_type, type_of_loan, ltv, borrower_name.

Use these cues:
- borrower_type: from Borrower type.
- Amount_recommended_by_analyst: from Amount Recommended.
- loan_purpose: combine Specific Loan Purpose and Loan Purpose to Industry into one concise value.
- cic_contract_type: from CIC Contract type.
- type_of_loan: from Type of loan.
- ltv: from Loan value; prefer the Collateral section. If absent, set null.
- borrower_name: from Borrower''s Name.

For each key output four fields:
- value, expected (from the provided JSON if present, otherwise null), reason, and issues (array).
Do not output pass or fail flags.

Return one compact JSON object only, no prose.
Top-level keys must be: section with value CVF, and fields which is a map from key name to the object described above.';
  end if;

  -- collect up to N files, then unpack into variables
  with files as (
    select
      d.relative_path,
      split_part(d.relative_path,'/',2) as section_type,
      split_part(d.relative_path,'/',3) as filename,
      regexp_replace(upper(split_part(d.relative_path,'/',3)),'[^A-Z0-9]','') as file_norm
    from directory(@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT) d
    where d.relative_path ilike 'credit_images/%'
  ),
  doc as (
    select regexp_replace(upper(:v_doc_name),'[^A-Z0-9]','') as doc_norm
  ),
  picked as (
    select relative_path
    from files f
    join doc d
      on upper(f.section_type) = upper(:P_SECTION_TYPE)
     and f.file_norm like '%' || substr(d.doc_norm, 1, 18) || '%'
    order by f.filename
    limit :P_MAX_IMAGES
  ),
  pack as (
    select
      array_agg(relative_path) as paths,
      array_size(array_agg(relative_path)) as cnt
    from picked
  )
  select
    cnt,
    iff(cnt>=1, paths[0], null),
    iff(cnt>=2, paths[1], null),
    iff(cnt>=3, paths[2], null),
    iff(cnt>=4, paths[3], null),
    iff(cnt>=5, paths[4], null)
  into :v_cnt, :v_f1, :v_f2, :v_f3, :v_f4, :v_f5
  from pack;

  if (v_cnt = 0) then
    return object_construct('status','no_files',
                            'credit_doc_id', :P_CREDIT_DOC_ID,
                            'section_type',  :P_SECTION_TYPE,
                            'message','No matching files found in stage');
  end if;

  -- call AI_COMPLETE (1..5 images) with numbered placeholders ONLY in the header
  if (v_cnt = 1) then
    select AI_COMPLETE('openai-gpt-4.1',
      PROMPT('INPUTS: image {0} | json {1}\n' || :v_prompt,
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 2) then
    select AI_COMPLETE('openai-gpt-4.1',
      PROMPT('INPUTS: images {0} {1} | json {2}\n' || :v_prompt,
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 3) then
    select AI_COMPLETE('openai-gpt-4.1',
      PROMPT('INPUTS: images {0} {1} {2} | json {3}\n' || :v_prompt,
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  elseif (v_cnt = 4) then
    select AI_COMPLETE('openai-gpt-4.1',
      PROMPT('INPUTS: images {0} {1} {2} {3} | json {4}\n' || :v_prompt,
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f4),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;

  else
    select AI_COMPLETE('openai-gpt-4.1',
      PROMPT('INPUTS: images {0} {1} {2} {3} {4} | json {5}\n' || :v_prompt,
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f1),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f2),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f3),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f4),
        TO_FILE('@DOC_PROCESSING_DB.DOC_AI_SCHEMA.DOC_INPUT', :v_f5),
        TO_VARCHAR(:v_json)
      )
    ) into :v_result;
  end if;

  -- parse the model output as JSON (we asked for JSON only)
  select try_parse_json(:v_result::string) into :v_parsed;

  -- persist into the appropriate table (expects fields.<key>.value / reason / issues)
  if (upper(P_SECTION_TYPE) = 'CAM') then
    insert into DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CAM (
      CREDIT_DOC_ID, SECTION_TYPE,
      loan_type, agent_name, handling_loan_manager, handling_loan_evaluator,
      loan_facility, loan_industry,
      amount_recommended_loan_manager, amount_recommended_loan_evaluator, amount_approved_by_analyst,
      date_of_birth, civil_status,
      highest_educational_attainment_borrower, highest_educational_attainment_spouse,
      occupation_history_borrower, occupation_history_spouse,
      related_to_business_soi_borrower, related_to_business_soi_spouse,
      email_address, landline_mobile_no, other_gov_issued_id,
      name_of_spouse, spouse_date_of_birth, mothers_maiden_name,
      no_of_dependents, children_name_age, parent_siblings_name, parent_siblings_address,
      REASONS, ISSUES, RESULT_JSON
    )
    select
      :P_CREDIT_DOC_ID, :P_SECTION_TYPE,
      r:fields:loan_type:value::string,
      r:fields:agent_name:value::string,
      r:fields:handling_loan_manager:value::string,
      r:fields:handling_loan_evaluator:value::string,
      r:fields:loan_facility:value::string,
      r:fields:loan_industry:value::string,
      r:fields:amount_recommended_loan_manager:value::string,
      r:fields:amount_recommended_loan_evaluator:value::string,
      r:fields:amount_approved_by_analyst:value::string,
      r:fields:date_of_birth:value::string,
      r:fields:civil_status:value::string,
      r:fields:highest_educational_attainment_borrower:value::string,
      r:fields:highest_educational_attainment_spouse:value::string,
      r:fields:occupation_history_borrower:value::string,
      r:fields:occupation_history_spouse:value::string,
      r:fields:related_to_business_soi_borrower:value::string,
      r:fields:related_to_business_soi_spouse:value::string,
      r:fields:email_address:value::string,
      r:fields:landline_mobile_no:value::string,
      r:fields:other_gov_issued_id:value::string,
      r:fields:name_of_spouse:value::string,
      r:fields:spouse_date_of_birth:value::string,
      r:fields:mothers_maiden_name:value::string,
      r:fields:no_of_dependents:value::string,
      r:fields:children_name_age:value::string,
      r:fields:parent_siblings_name:value::string,
      r:fields:parent_siblings_address:value::string,
      object_construct(
        'loan_type', r:fields:loan_type:reason,
        'agent_name', r:fields:agent_name:reason,
        'handling_loan_manager', r:fields:handling_loan_manager:reason,
        'handling_loan_evaluator', r:fields:handling_loan_evaluator:reason,
        'loan_facility', r:fields:loan_facility:reason,
        'loan_industry', r:fields:loan_industry:reason,
        'amount_recommended_loan_manager', r:fields:amount_recommended_loan_manager:reason,
        'amount_recommended_loan_evaluator', r:fields:amount_recommended_loan_evaluator:reason,
        'amount_approved_by_analyst', r:fields:amount_approved_by_analyst:reason,
        'date_of_birth', r:fields:date_of_birth:reason,
        'civil_status', r:fields:civil_status:reason,
        'highest_educational_attainment_borrower', r:fields:highest_educational_attainment_borrower:reason,
        'highest_educational_attainment_spouse', r:fields:highest_educational_attainment_spouse:reason,
        'occupation_history_borrower', r:fields:occupation_history_borrower:reason,
        'occupation_history_spouse', r:fields:occupation_history_spouse:reason,
        'related_to_business_soi_borrower', r:fields:related_to_business_soi_borrower:reason,
        'related_to_business_soi_spouse', r:fields:related_to_business_soi_spouse:reason,
        'email_address', r:fields:email_address:reason,
        'landline_mobile_no', r:fields:landline_mobile_no:reason,
        'other_gov_issued_id', r:fields:other_gov_issued_id:reason,
        'name_of_spouse', r:fields:name_of_spouse:reason,
        'spouse_date_of_birth', r:fields:spouse_date_of_birth:reason,
        'mothers_maiden_name', r:fields:mothers_maiden_name:reason,
        'no_of_dependents', r:fields:no_of_dependents:reason,
        'children_name_age', r:fields:children_name_age:reason,
        'parent_siblings_name', r:fields:parent_siblings_name:reason,
        'parent_siblings_address', r:fields:parent_siblings_address:reason
      ),
      object_construct(
        'loan_type', r:fields:loan_type:issues,
        'agent_name', r:fields:agent_name:issues,
        'handling_loan_manager', r:fields:handling_loan_manager:issues,
        'handling_loan_evaluator', r:fields:handling_loan_evaluator:issues,
        'loan_facility', r:fields:loan_facility:issues,
        'loan_industry', r:fields:loan_industry:issues,
        'amount_recommended_loan_manager', r:fields:amount_recommended_loan_manager:issues,
        'amount_recommended_loan_evaluator', r:fields:amount_recommended_loan_evaluator:issues,
        'amount_approved_by_analyst', r:fields:amount_approved_by_analyst:issues,
        'date_of_birth', r:fields:date_of_birth:issues,
        'civil_status', r:fields:civil_status:issues,
        'highest_educational_attainment_borrower', r:fields:highest_educational_attainment_borrower:issues,
        'highest_educational_attainment_spouse', r:fields:highest_educational_attainment_spouse:issues,
        'occupation_history_borrower', r:fields:occupation_history_borrower:issues,
        'occupation_history_spouse', r:fields:occupation_history_spouse:issues,
        'related_to_business_soi_borrower', r:fields:related_to_business_soi_borrower:issues,
        'related_to_business_soi_spouse', r:fields:related_to_business_soi_spouse:issues,
        'email_address', r:fields:email_address:issues,
        'landline_mobile_no', r:fields:landline_mobile_no:issues,
        'other_gov_issued_id', r:fields:other_gov_issued_id:issues,
        'name_of_spouse', r:fields:name_of_spouse:issues,
        'spouse_date_of_birth', r:fields:spouse_date_of_birth:issues,
        'mothers_maiden_name', r:fields:mothers_maiden_name:issues,
        'no_of_dependents', r:fields:no_of_dependents:issues,
        'children_name_age', r:fields:children_name_age:issues,
        'parent_siblings_name', r:fields:parent_siblings_name:issues,
        'parent_siblings_address', r:fields:parent_siblings_address:issues
      ),
      r
    from (select :v_parsed as r);

  else  -- CVF
    insert into DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CVF (
      CREDIT_DOC_ID, SECTION_TYPE,
      borrower_type, Amount_recommended_by_analyst, loan_purpose, cic_contract_type, type_of_loan, ltv, borrower_name,
      REASONS, ISSUES, RESULT_JSON
    )
    select
      :P_CREDIT_DOC_ID, :P_SECTION_TYPE,
      r:fields:borrower_type:value::string,
      r:fields:"Amount_recommended_by_analyst":value::string,
      r:fields:loan_purpose:value::string,
      r:fields:cic_contract_type:value::string,
      r:fields:type_of_loan:value::string,
      r:fields:ltv:value::string,
      r:fields:borrower_name:value::string,
      object_construct(
        'borrower_type', r:fields:borrower_type:reason,
        'Amount_recommended_by_analyst', r:fields:"Amount_recommended_by_analyst":reason,
        'loan_purpose', r:fields:loan_purpose:reason,
        'cic_contract_type', r:fields:cic_contract_type:reason,
        'type_of_loan', r:fields:type_of_loan:reason,
        'ltv', r:fields:ltv:reason,
        'borrower_name', r:fields:borrower_name:reason
      ),
      object_construct(
        'borrower_type', r:fields:borrower_type:issues,
        'Amount_recommended_by_analyst', r:fields:"Amount_recommended_by_analyst":issues,
        'loan_purpose', r:fields:loan_purpose:issues,
        'cic_contract_type', r:fields:cic_contract_type:issues,
        'type_of_loan', r:fields:type_of_loan:issues,
        'ltv', r:fields:ltv:issues,
        'borrower_name', r:fields:borrower_name:issues
      ),
      r
    from (select :v_parsed as r);
  end if;

  -- return a structured envelope with files used + the JSON
  return object_construct(
    'status','ok',
    'credit_doc_id', :P_CREDIT_DOC_ID,
    'section_type',  :P_SECTION_TYPE,
    'images_used',   :v_cnt,
    'files', array_construct_compact(:v_f1,:v_f2,:v_f3,:v_f4,:v_f5),
    'result', :v_parsed
  );
end;
$$;



call DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION('c35c24abc48c418189c7eccfbf185dfc','CVF',5);

call DOC_PROCESSING_DB.DOC_AI_SCHEMA.VALIDATE_SECTION(
  'd542a37c7cdc4328a4b667d07117a760',  -- CREDIT_DOC_ID to process
  'CVF',                                -- 'CAM' or 'CVF'
  5                                      -- max images to use (1..5)
);


SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT
TRUNCATE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_EXTRACTED_TEXT
SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON
TRUNCATE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_SECTION_JSON

SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CAM
TRUNCATE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CAM

SELECT * FROM DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CVF
TRUNCATE TABLE DOC_PROCESSING_DB.DOC_AI_SCHEMA.CREDIT_VALIDATION_CVF
