CREATE TABLE IF NOT EXISTS analysis.dm_rfm_segments (
	user_id int4 NOT NULL PRIMARY KEY,
	recency int4 CHECK ((recency >= 1) AND (recency <= 5)),
	frequency int4 CHECK ((frequency >= 1) AND (frequency <= 5)),
	monetary_value int4 CHECK ((monetary_value >= 1) AND (monetary_value <= 5))
);