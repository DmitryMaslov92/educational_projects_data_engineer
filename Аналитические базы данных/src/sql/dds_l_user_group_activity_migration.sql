INSERT INTO STV230533__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
SELECT DISTINCT hash(hu.hk_user_id, hg.hk_group_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now(),
	's3'
FROM STV230533__STAGING.group_log as gl
LEFT JOIN STV230533__DWH.h_users hu on gl.user_id = hu.user_id 
LEFT JOIN STV230533__DWH.h_groups hg on gl.group_id = hg.group_id;