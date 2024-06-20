WITH user_group_messages AS (
SELECT hg.hk_group_id, COUNT(DISTINCT(lum.hk_user_id)) cnt_users_in_group_with_messages
FROM STV230533__DWH.h_groups hg 
LEFT JOIN STV230533__DWH.l_groups_dialogs lgd on hg.hk_group_id = lgd.hk_group_id 
LEFT JOIN STV230533__DWH.l_user_message lum on lum.hk_message_id = lgd.hk_message_id 
GROUP BY hg.hk_group_id
),
user_group_log AS (
SELECT hk_group_id, count(distinct hk_user_id) cnt_added_users
FROM STV230533__DWH.l_user_group_activity luga
JOIN STV230533__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity 
WHERE luga.hk_group_id IN (SELECT hk_group_id FROM STV230533__DWH.h_groups ORDER BY registration_dt ASC LIMIT 10) AND sah.event = 'add'
GROUP BY hk_group_id
)
SELECT ugl.hk_group_id, 
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users group_conversion
FROM user_group_log ugl
LEFT JOIN user_group_messages ugm on ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC
;