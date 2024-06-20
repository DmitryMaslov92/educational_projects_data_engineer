DROP TABLE IF EXISTS STV230533__DWH.s_auth_history;

CREATE TABLE STV230533__DWH.s_auth_history(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV230533__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from INT,
	event VARCHAR(20),
	event_dt TIMESTAMP,
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV230533__DWH.s_auth_history(hk_l_user_group_activity,user_id_from,event,event_dt,load_dt,load_src)
SELECT luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.event_dt,
	now() as load_dt,
	's3' as load_src
FROM STV230533__STAGING.group_log gl
LEFT JOIN  STV230533__DWH.h_groups hg on gl.group_id = hg.group_id
LEFT JOIN  STV230533__DWH.h_users hu on gl.user_id = hu.user_id
LEFT JOIN  STV230533__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id; 