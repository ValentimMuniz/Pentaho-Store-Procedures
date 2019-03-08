DELIMITER $$

DROP PROCEDURE IF EXISTS `gerar_relatorio_3`$$

CREATE PROCEDURE gerar_relatorio_3(IN tgid INT(11),IN report_type VARCHAR(255),IN view_type VARCHAR(255),IN start_time VARCHAR(255),IN end_time VARCHAR(255),IN periods VARCHAR(255) )
BEGIN
SET SESSION group_concat_max_len = 10000;
SET @terminal_group_id = tgid;
SET @report_type = report_type;
SET @start_date = CONCAT(start_time,' 00:00:00');
SET @end_date = CONCAT(end_time,' 23:59:59');
SET @toView = view_type;
SET @time_limits = periods;


SET @tmp_query_1 = '';
SET @tmp_query_2 = '';
SET @tmp_query_3 = '';
SET @tmp_query_4 = '';
SET @sql_columns1 = '';
SET @sql_columns2 = '';
SET @sql_columns3 = '';
SET @sql_columns4_1 = '';
SET @sql_columns4_2 = '';
SET @sql_columns5 = '';
SELECT int_location INTO @hospital_id FROM tbl_hlt_placecleaninglist_groups WHERE int_key = @terminal_group_id;
SELECT CONCAT(GROUP_CONCAT(
	
	
	CONCAT('MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS "',ts.vch_name,'"',IF(ts.int_IsStartStep = 1,CONCAT(',MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS Primeira_Etapa_',ts.int_key),''))
	ORDER BY ts.int_order)
, ",MIN(CASE WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (1,2,3)) THEN DATE_FORMAT(t1.dat_historicalDate, '%d/%m/%Y') END) AS DiaRequisicaoLimpeza"
, ",MIN(CASE WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS Requisitado"
, ",MIN(CASE WHEN (t1.int_placecleaninglist_step_key IS NULL) AND (t1.int_action IN (2) AND t1.vch_attribute = 'int_placecleaninglist_step_key') THEN t1.dat_historicalDate END) AS Finalizado"
, ",MIN(CASE WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (4)) AND t1.vch_extra_meaning = 'room_removed' THEN t1.dat_historicalDate END) AS Removido"
, ",MIN(CASE WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (4)) AND t1.vch_extra_meaning = 'room_rejected' THEN t1.dat_historicalDate END) AS Rejeitado"
)
INTO @sql_columns1
FROM tbl_hlt_placecleaninglist_steps ts
INNER JOIN tbl_hlt_placecleaninglist_step_flows tsfs ON ts.int_key = tsfs.int_terminal_step_key
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;

SET @tmp_query_1 = CONCAT(
	'SELECT t1.int_placecleaninglist_key AS PlaceKey, place.vch_name as PlaceName, ', @sql_columns1, '
	FROM tbl_hlt_placecleaninglist_historicaldata t1 
	LEFT JOIN tbl_hlt_placecleaninglist_steps ts on t1.int_placecleaninglist_step_key = ts.int_key
	LEFT JOIN tbl_hlt_cleantype ct ON (t1.int_cleantype_key = ct.int_key)
	LEFT JOIN tbl_hlt_place place ON (t1.int_place_key = place.int_key)
	WHERE (t1.int_action IN (1,2,3,4)) AND t1.int_place_hospital_key = ',@hospital_id,'
	AND (t1.dat_historicalDate between "',@start_date,'" AND "',@end_date,'")
	GROUP BY t1.int_placecleaninglist_key');
SELECT 
CONCAT('IF((`Finalizado` IS NULL) AND (`Removido` IS NULL) AND (`Rejeitado` IS NULL), 1, 0) as inCleaning, IF(`Finalizado` IS NOT NULL, 1, 0) as wasCleaned, IF(`Removido` IS NOT NULL, 1, 0) as wasRemoved, IF(`Rejeitado` IS NOT NULL, 1, 0) as wasRejected, TIMESTAMPDIFF(SECOND, `Requisitado`, `Finalizado`) AS diff_real_ini_fim, ',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS diff_',tss.int_key,'_ini_fim,'), ''),'TIMESTAMPDIFF(SECOND, `',tss2.vch_name,'`, `',tss.vch_name,'`) AS diff_',tss2.int_key,'_',tss.int_key,', TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `',tss1.vch_name,'`) AS diff_',tss.int_key,'_',tss1.int_key,', TIMESTAMPDIFF(SECOND, `',tss2.vch_name,'`, `',tss1.vch_name,'`) AS diff_',tss2.int_key,'_',tss1.int_key,'')
	WHEN tsfs.int_end_on_terminal_step_key > 0 AND tss1.vch_enum != 'inclean'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS diff_',tss.int_key,'_ini_fim,'), ''),'TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `',tss1.vch_name,'`) AS diff_',tss.int_key,'_',tss1.int_key,'') 	
	WHEN tsfs.int_end_on_terminal_step_key = 0 AND tss.vch_type = 'active'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS diff_',tss.int_key,'_ini_fim,'), ''),'TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS diff_',tss.int_key,'_final')
END ORDER BY tss.int_order
)) AS diff_field
INTO @sql_columns2
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;


SELECT GROUP_CONCAT(CONCAT('(`Stts`.`',ts.vch_name,'` IS NOT NULL)') ORDER BY ts.int_order SEPARATOR ' OR ')
INTO @sql_columns3
FROM tbl_hlt_placecleaninglist_steps ts
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;
SET @tmp_query_2 = CONCAT('SELECT Stts.*, ',@report_type,' AS Grouped,' , @sql_columns2 , ' FROM (',@tmp_query_1,') AS Stts WHERE ', @sql_columns3);
SELECT 
CONCAT('(MAX(diff_real_ini_fim)) as `Real_TempoTotalFila`,',GROUP_CONCAT(
CASE 	
	WHEN tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('(MAX(diff_',tss.int_key,'_ini_fim)) as `TempoTotalFila_Comecando_',tss.vch_name,'`,'), ''),'(MAX(diff_',tss2.int_key,'_',tss.int_key,')) as "Tempo_',tss2.vch_name,'_',tss.vch_name,'", (MAX(diff_',tss.int_key,'_',tss1.int_key,')) as "Tempo_',tss.vch_name,'_',tss1.vch_name,'", (MAX(diff_',tss2.int_key,'_',tss1.int_key,')) as "Tempo_Terminal_',tss2.vch_name,'_',tss1.vch_name,'"') 
	WHEN tsfs.int_end_on_terminal_step_key > 0 AND tss1.vch_enum != 'inclean'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('(MAX(diff_',tss.int_key,'_ini_fim)) as `TempoTotalFila_Comecando_',tss.vch_name,'`,'), ''),'(MAX(diff_',tss.int_key,'_',tss1.int_key,')) as "Tempo_',tss.vch_name,'_',tss1.vch_name,'"') 
	WHEN tsfs.int_end_on_terminal_step_key = 0 AND tss.vch_type = 'active'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('(MAX(diff_',tss.int_key,'_ini_fim)) as `TempoTotalFila_Comecando_',tss.vch_name,'`,'), ''),'(MAX(diff_',tss.int_key,'_final)) as "Tempo_',tss.vch_name,'_final"')
END ORDER BY tss.int_order 
)) AS diff_field
INTO @sql_columns4_1
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;



DROP TABLE IF EXISTS t;
CREATE TEMPORARY TABLE t( txt TEXT );
INSERT INTO t VALUES(@time_limits);
DROP TEMPORARY TABLE IF EXISTS temp;
CREATE TEMPORARY TABLE temp( val CHAR(255) );
SET @sql_periods = CONCAT("insert into temp (val) values ('", REPLACE(( SELECT GROUP_CONCAT(DISTINCT txt) AS DATA FROM t), ",", "'),('"),"');");
PREPARE stmt1 FROM @sql_periods;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
SELECT
CONCAT('COUNT(CASE WHEN diff_real_ini_fim<',teste.seconds,' THEN 1 END) AS "Real_TempoTotalFila_',(teste.seconds/60),'min",' ,GROUP_CONCAT(
CASE 	
	WHEN tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('COUNT(CASE WHEN diff_',tss.int_key,'_ini_fim<',teste.seconds,' THEN 1 END) AS "TempoTotalFila_Comecando_',tss.vch_name,'_',(teste.seconds/60),'min",'), ''),'COUNT(CASE WHEN diff_',tss2.int_key,'_',tss.int_key,'<',teste.seconds,' THEN 1 END) AS "Tempo_',tss2.vch_name,'_',tss.vch_name,'_',(teste.seconds/60),'min" ,COUNT(CASE WHEN diff_',tss.int_key,'_',tss1.int_key,'<',teste.seconds,' THEN 1 END) AS "Tempo_',tss.vch_name,'_',tss1.vch_name,'_',IF(tss.int_IsStartStep = 1,'comeco_',''),(teste.seconds/60),'min",COUNT(CASE WHEN diff_',tss2.int_key,'_',tss1.int_key,'<',teste.seconds,' THEN 1 END) AS "Tempo_Terminal_',tss2.vch_name,'_',tss1.vch_name,'_',(teste.seconds/60),'min"\n') 
	WHEN tsfs.int_end_on_terminal_step_key > 0 AND tss1.vch_enum != 'inclean'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('COUNT(CASE WHEN diff_',tss.int_key,'_ini_fim<',teste.seconds,' THEN 1 END) AS "TempoTotalFila_Comecando_',tss.vch_name,'_',(teste.seconds/60),'min",'), ''),'COUNT(CASE WHEN diff_',tss.int_key,'_',tss1.int_key,'<',teste.seconds,' THEN 1 END) AS "Tempo_',tss.vch_name,'_',tss1.vch_name,'_',IF(tss.int_IsStartStep = 1,'comeco_',''),(teste.seconds/60),'min"\n') 
	WHEN tsfs.int_end_on_terminal_step_key = 0 AND tss.vch_type = 'active'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('COUNT(CASE WHEN diff_',tss.int_key,'_ini_fim<',teste.seconds,' THEN 1 END) AS "TempoTotalFila_Comecando_',tss.vch_name,'_',(teste.seconds/60),'min",'), ''),'COUNT(CASE WHEN diff_',tss.int_key,'_final<',teste.seconds,' THEN 1 END) AS "Tempo_',tss.vch_name,'_final_',(teste.seconds/60),'min"\n')
END ORDER BY tss.int_order 
))
AS diff_field
INTO @sql_columns4_2
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
CROSS JOIN (
	SELECT DISTINCT(val) AS seconds FROM temp
) AS teste
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;

SET @tmp_query_3 = CONCAT('
	SELECT
		',@toView,' AS ToView, ',IF(@report_type = "PlaceKey", 'place.vch_name as PlaceName, ', ''),'
		Grouped AS Grouped,
		COUNT(*) AS TotalLimpezas,
		SUM(wasCleaned) AS "LimpezasEfetuadas",		
		SUM(wasRejected) AS "LimpezasRejeitadas",		
		SUM(WasRemoved) AS "LimpezasRemovidas",
		SUM(inCleaning) AS "LimpezasEmExecucao",
		',@sql_columns4_1,',',@sql_columns4_2,'		
	FROM (', @tmp_query_2,') AS Times ',
	IF(@report_type = "PlaceKey", ' INNER JOIN tbl_hlt_place place ON Times.PlaceKey = place.int_key ', ''),
	'GROUP BY Grouped');
	

SELECT
CONCAT('CAST(((`Real_TempoTotalFila_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas-LimpezasRejeitadas))*100 AS DECIMAL (10,2)) AS Pct_Real_TempoTotalFilal_',(teste.seconds/60),'min,\n' ,GROUP_CONCAT(
CASE 	
	WHEN tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('CAST(((`TempoTotalFila_Comecando_',tss.vch_name,'_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas-LimpezasRejeitadas))*100 AS DECIMAL (10,2)) AS `Pct_TempoTotalFila_',tss.vch_name,'_',(teste.seconds/60),'min`,\n'), ''),'CAST(((`Tempo_',tss2.vch_name,'_',tss.vch_name,'_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas))*100 AS DECIMAL (10,2)) AS "Pct_Tempo_',tss2.vch_name,'_',tss.vch_name,'_',(teste.seconds/60),'min"\n , CAST(((`Tempo_',tss.vch_name,'_',tss1.vch_name,'_',IF(tss.int_IsStartStep = 1,'comeco_',''),(teste.seconds/60),'min`*1.0)/(TotalLimpezas))*100 AS DECIMAL (10,2)) AS "Pct_Tempo_',tss.vch_name,'_',tss1.vch_name,'_',IF(tss.int_IsStartStep = 1,'comeco_',''),(teste.seconds/60),'min"\n, CAST(((`Tempo_Terminal_',tss2.vch_name,'_',tss1.vch_name,'_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas))*100 AS DECIMAL (10,2)) AS "Pct_Tempo_Terminal_',tss2.vch_name,'_',tss1.vch_name,'_',(teste.seconds/60),'min"\n') 
	WHEN tsfs.int_end_on_terminal_step_key > 0 AND tss1.vch_enum != 'inclean'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('CAST(((`TempoTotalFila_Comecando_',tss.vch_name,'_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas-LimpezasRejeitadas))*100 AS DECIMAL (10,2)) AS `Pct_TempoTotalFila_',tss.vch_name,'_',(teste.seconds/60),'min`,\n'), ''),'CAST(((`Tempo_',tss.vch_name,'_',tss1.vch_name,'_',IF(tss.int_IsStartStep = 1,'comeco_',''),(teste.seconds/60),'min`*1.0)/(TotalLimpezas))*100 AS DECIMAL (10,2)) AS "Pct_Tempo_',tss.vch_name,'_',tss1.vch_name,'_',IF(tss.int_IsStartStep = 1,'comeco_',''),(teste.seconds/60),'min"\n') 
	WHEN tsfs.int_end_on_terminal_step_key = 0 AND tss.vch_type = 'active'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('CAST(((`TempoTotalFila_Comecando_',tss.vch_name,'_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas-LimpezasRejeitadas))*100 AS DECIMAL (10,2)) AS `Pct_TempoTotalFila_',tss.vch_name,'_',(teste.seconds/60),'min`,\n'), ''),'CAST(((`Tempo_',tss.vch_name,'_final_',(teste.seconds/60),'min`*1.0)/(TotalLimpezas))*100 AS DECIMAL (10,2)) AS "Pct_Tempo_',tss.vch_name,'_final_',(teste.seconds/60),'min"\n')
END 
ORDER BY tss.int_order))
AS diff_field
INTO @sql_columns5
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
CROSS JOIN (
	SELECT DISTINCT(val) AS seconds FROM temp
) AS teste
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;

	
SET @tmp_query_4 = CONCAT('SELECT *, ',@sql_columns5,' FROM (',@tmp_query_3,') AS Totais ORDER BY Grouped');
		




PREPARE stmt FROM @tmp_query_4;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;