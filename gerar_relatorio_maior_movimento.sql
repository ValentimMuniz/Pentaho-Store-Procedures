DELIMITER $$


DROP PROCEDURE IF EXISTS `gerar_relatorio_maior_movimento`$$

CREATE PROCEDURE `gerar_relatorio_maior_movimento`(
	IN tgid INT(11),
	IN start_time VARCHAR(255),
	IN end_time VARCHAR(255),
    IN report_interval VARCHAR(20)
)
BEGIN
SET SESSION group_concat_max_len = 20000 ;
SET @start_date = CONCAT(start_time, ' 00:00:00');
SET @end_date = CONCAT(end_time, ' 23:00:59');
SET @terminal_group_id = tgid;
SET @intervals = report_interval;
SET @columns1 = '';
SET @columns2 = '';
SET @tmp_query_1 = '';


SELECT CONCAT(GROUP_CONCAT('SELECT total.Interval30min, SUM(total.entraram) as total, SUM(total.foram_limpos) as TotalLimpo FROM
(SELECT "00:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "00:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:00:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:30:00" AS Interval30min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT CONVERT(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.dat_historicaldate)/(60*60))*(60*60)), TIME) Interval30min, 
SUM(t1.int_action IN (1) AND t1.int_placecleaninglist_step_key IS NOT NULL) AS entraram,
SUM(t1.int_action IN (4) AND t1.vch_extra_meaning = "room_cleaned") AS foram_limpos
FROM tbl_hlt_placecleaninglist_historicaldata t1
where t1.int_placecleaninglist_group_key = ', @terminal_group_id, ' and t1.dat_historicaldate between "', @start_date ,'" AND "', @end_date, '"
GROUP BY Interval30min) as total
GROUP BY total.Interval30min')) INTO @columns1;



SELECT CONCAT(GROUP_CONCAT('
SELECT total.Interval60min, SUM(total.entraram) as total, SUM(total.foram_limpos) as TotalLimpo FROM 
(SELECT "00:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:00:00" AS Interval60min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT CONVERT(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.dat_historicaldate)/(60*60))*(60*60)), TIME) Interval60min, 
SUM(t1.int_action IN (1) AND t1.int_placecleaninglist_step_key IS NOT NULL) AS entraram,
SUM(t1.int_action IN (4) AND t1.vch_extra_meaning = "room_cleaned") AS foram_limpos
FROM tbl_hlt_placecleaninglist_historicaldata t1
where t1.int_placecleaninglist_group_key = ', @terminal_group_id, ' and t1.dat_historicaldate between "', @start_date ,'" AND "', @end_date, '"
GROUP BY Interval60min) as total
GROUP BY total.Interval60min')) INTO @columns2;



SELECT CONCAT(GROUP_CONCAT('SELECT total.Interval15min, SUM(total.entraram) as total, SUM(total.foram_limpos) as TotalLimpo FROM
(SELECT "00:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "00:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "00:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "00:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "01:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "02:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "03:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "04:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "05:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "06:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "07:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "08:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "09:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "10:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "11:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "12:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "13:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "14:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "15:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "16:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "17:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "18:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "19:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "20:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "21:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "22:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:00:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:15:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:30:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT "23:45:00" AS Interval15min, 0 AS entraram, 0 AS foram_limpos
UNION SELECT CONVERT(FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(t1.dat_historicaldate)/(15*60))*(15*60)), TIME) Interval15min, 
SUM(t1.int_action IN (1) AND t1.int_placecleaninglist_step_key IS NOT NULL) AS entraram,
SUM(t1.int_action IN (4) AND t1.vch_extra_meaning = "room_cleaned") AS foram_limpos
FROM tbl_hlt_placecleaninglist_historicaldata t1
where t1.int_placecleaninglist_group_key = ', @terminal_group_id, ' and t1.dat_historicaldate between "', @start_date ,'" AND "', @end_date, '"
GROUP BY Interval15min) as total
GROUP BY total.Interval15min')) INTO @columns3;


SET @tmp_query_1 = CONCAT(
CASE 
	WHEN @intervals = '30min' THEN @columns1
    WHEN @intervals = 'hour' THEN @columns2
    WHEN @intervals = '15min' THEN @columns3
END);

PREPARE stmt FROM @tmp_query_1;
EXECUTE stmt;

END$$

DELIMITER ;