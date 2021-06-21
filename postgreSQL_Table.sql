-- tb_m8b_log_bas01
CREATE TABLE tb_log_bas01
(
    access_time character varying(12) NOT NULL,
    chain_id character varying(80) NOT NULL,
    app_id character varying(80) NOT NULL,
    page_name character varying(200) NOT NULL,
    page_type character varying(80),
    CONSTRAINT tb_m8b log_bas01_pk PRIMARY KEY (chain_id, app_id)
)

-- tb_m8b_log_stat10
CREATE TABLE tb_log_stat10
(
    aggr_dt character varying(12) NOT NULL,
    chain_id character varying(80) NOT NULL,
    app_id character varying(80) NOT NULL,
    stat_tp character varying(1) NOT NULL DEFAULT 'D'::character varying,
    fact character varying(10) NOT NULL DEFAULT 'A'::character varying,
    cnt integer,
    mean_v numeric,
    sd_v numeric,
    var_v  numeric,
    min_v numeric,
    max_v numeric,
    medi numeric,
    qt1 numeric,
    qt3 numeric,
    creation_timestamp timestamp without time zone NOT NULL DEFAULT now(),
    last_update_timestamp timestamp without time zone NOT NULL DEFAULT now()
)

-- tb_m8b_log_qcc_stat
CREATE TABLE tb_log_qcc_stat
(
    aggr_dt character varying(12) NOT NULL,   -- 집계 일자
    chain_id character varying(80) NOT NULL,  -- 체인 ID
    app_id character varying(80) NOT NULL,    -- 화면 ID
    page_type character varying(80) NOT NULL, -- page Type
    use_tp character varying(1) NOT NULL DEFAULT 'A'::character varying, -- A:해석용 관리도, S:관리용 관리도
    qcc_tp character varying(1) NOT NULL DEFAULT 'X'::character varying, -- X:Xbar관리도 S:S관리도 R:R관리도
    ucl numeric -- 관리 상한선
    cl numeric, -- 중앙선
    lcl numeric, -- 관리 하한선
    sample_size numeric, -- sample size
    creation_timestamp timestamp without time zone NOT NULL DEFAULT now(),
    last_update_timestamp timestamp without time zone NOT NULL DEFAULT now()
    CONSTRAINT tb_log_qcc_stat_pk PRIMARY KEY (aggr_dt, chain_id, app_id, page_type, qcc_tp, use_tp)
)

-- tb_m8b_log_qcc_plot
CREATE TABLE tb_log_qcc_plot
(
     aggr_dt character varying(12) NOT NULL,
     chain_id character varying(80) NOT NULL,
     app_id character varying(80) NOT NULL,
     use_tp character varying(1) NOT NULL DEFAULT 'A'::character varying, -- A:해석용 관리도, S:관리용 관리도
     qcc_tp character varying(1) NOT NULL DEFAULT 'X'::character varying, -- X:Xbar관리도 S:S관리도 R:R관리도
     plot_nm character varying(120)
     plot_file_nm character varying(120),
     plot_content character varying(1000000)
     creation_timestamp timestamp without time zone NOT NULL DEFAULT now(),
     last_update_timestamp timestamp without time zone NOT NULL DEFAULT now()
)