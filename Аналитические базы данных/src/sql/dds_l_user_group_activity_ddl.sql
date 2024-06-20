DROP TABLE IF EXISTS STV230533__DWH.l_user_group_activity CASCADE;

CREATE TABLE STV230533__DWH.l_user_group_activity (
	hk_l_user_group_activity bigint NOT NULL PRIMARY KEY,
	hk_user_id bigint NOT NULL CONSTRAINT l_user_group_activity_user REFERENCES STV230533__DWH.h_users (hk_user_id),
	hk_group_id bigint NOT NULL CONSTRAINT l_user_group_activity_group REFERENCES STV230533__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);