INSERT INTO analysis.dm_rfm_segments
SELECT trr.*, trf.frequency, trmv.monetary_value
FROM analysis.tmp_rfm_recency trr 
JOIN analysis.tmp_rfm_frequency trf on trr.user_id=trf.user_id 
JOIN analysis.tmp_rfm_monetary_value trmv on trr.user_id=trmv.user_id
ORDER BY user_id;

SELECT *
FROM analysis.dm_rfm_segments
LIMIT 10;

|user_id|recency|frequency|monetary_value|
|0		|3		|1		  |4 			 |
|1		|3		|4		  |3 			 |
|2		|3		|2		  |5 			 |
|3		|3		|2		  |3 			 |
|4		|3		|4		  |3 			 |
|5		|5		|5		  |5 			 |
|6		|3		|1		  |5 			 |
|7		|2		|4		  |2 			 |
|8		|1		|1		  |3 			 |
|9		|3		|1		  |2 			 |
