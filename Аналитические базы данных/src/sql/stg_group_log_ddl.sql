DROP TABLE IF EXISTS STV230533__STAGING.group_log_rej CASCADE;
DROP TABLE IF EXISTS STV230533__STAGING.group_log CASCADE;
CREATE TABLE STV230533__STAGING.group_log (
	group_id int NOT NULL,
	user_id int NOT NULL,
	user_id_from int,
	event VARCHAR(20),
	event_dt TIMESTAMP ,
	CONSTRAINT groups_log_group_id_fkey FOREIGN KEY (group_id) REFERENCES STV230533__STAGING.groups(id),
	CONSTRAINT groups_log_user_id_fkey FOREIGN KEY (user_id) REFERENCES STV230533__STAGING.users(id),
	CONSTRAINT groups_log_user_id_from_fkey FOREIGN KEY (user_id_from) REFERENCES STV230533__STAGING.users(id)	
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id, user_id) all nodes
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2)
;