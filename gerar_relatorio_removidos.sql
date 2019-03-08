DELIMITER $$

DROP PROCEDURE IF EXISTS `gerar_relatorio_removidos`$$

CREATE PROCEDURE `gerar_relatorio_removidos`(
IN tgid INT(11),
IN start_time VARCHAR(255),
IN end_time VARCHAR(255), 
IN start_time_hour VARCHAR(255),
IN start_time_minute VARCHAR(255),
IN end_time_hour VARCHAR(255), 
IN end_time_minute VARCHAR(255), 
IN rp_filter_room VARCHAR(255), 
IN rp_filter_person VARCHAR(255))
BEGIN
SET SESSION group_concat_max_len = 10000 ;
SET @report_type = "PlaceKey";
SET @terminal_group_id = tgid;
SET @report_filter_room = rp_filter_room;
SET @report_filter_person = rp_filter_person;
SET @start_date = start_time;
SET @end_date = end_time;
SET @start_date_hour_ini = start_time_hour;
SET @end_date_hour_fi = end_time_hour;
SET @start_date_minute_ini = start_time_minute;
SET @end_date_minute_fi = end_time_minute;
SET @toView = "Grouped";
SET @tmp_query_1 = '';
SET @tmp_query_1_2 = '';
SET @tmp_query_2 = '';
SET @sql_columns1 = '';
SET @sql_columns2 = '';
SET @sql_columns3 = '';
SELECT CONCAT(GROUP_CONCAT(
	-- CONCAT('MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS Status',ts.int_key) 
	-- ORDER BY ts.int_order
	CONCAT('MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS "Etapa ',ts.vch_name,'"',IF(ts.int_IsStartStep = 1,CONCAT(',MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS Primeira_Etapa_',ts.int_key),''))
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
-- select @sql_columns1
SET @tmp_query_1 = CONCAT(
	'SELECT
    t1.int_placecleaninglist_key AS PlaceKey,
	plc.vch_name AS Quarto,
	fila.vch_name AS `Nome Fila Retirado`,
    
    MIN(CASE 
    WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) 
    AND (t1.int_action IN (4)) 
    AND t1.vch_extra_meaning = "room_removed" THEN IF(t1.vch_description = "", "Não informado", t1.vch_description) END) AS Motivo,
    
    MIN(CASE 
    WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) 
    AND (t1.int_action IN (4)) 
    AND t1.vch_extra_meaning = "room_removed" THEN pessoa.vch_name END) AS Responsavel,
    
	MIN(CASE 
    WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) 
    AND (t1.int_action IN (4)) 
    AND t1.vch_extra_meaning = "room_removed" THEN placsteps.vch_name END) AS EtapaRemovido,
    
    MIN(CASE 
    WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) 
    AND (t1.int_action IN (4)) 
    AND t1.vch_extra_meaning = "room_removed" THEN t1.dat_historicalDate END) AS DataRemovido,
    
    ',@sql_columns1,'
	
	FROM tbl_hlt_placecleaninglist_historicaldata t1
	LEFT JOIN tbl_hlt_placecleaninglist_groups fila ON t1.int_placecleaninglist_group_key = fila.int_key
	LEFT JOIN tbl_hlt_place plc ON t1.int_place_key = plc.int_key
	LEFT JOIN tbl_hlt_person pessoa ON t1.int_person_key_responsible = pessoa.int_key
	LEFT JOIN tbl_hlt_placecleaninglist_steps placsteps ON t1.int_placecleaninglist_step_key = placsteps.int_key
	
	WHERE t1.int_placecleaninglist_group_key = ',@terminal_group_id,'
	AND DATE(t1.dat_historicalDate) >= "',@start_date,'" AND DATE(t1.dat_historicalDate) <= "',@end_date,'"
	AND TIME(t1.dat_historicalDate) BETWEEN TIME("',@start_date_hour_ini,':',@start_date_minute_ini,':00") and TIME("',@end_date_hour_fi,':',@end_date_minute_fi,':59")
    ', IF(LENGTH(@report_filter_room) = 0, '' , CONCAT(' AND plc.vch_name like "%',@report_filter_room,'%"')),'
	GROUP BY t1.int_placecleaninglist_key');
    
    SET @tmp_query_1_2 = CONCAT('select * FROM (',@tmp_query_1,') as Sem_Excluidos where `Removido` is not null and `Rejeitado` is null and `Finalizado` is null');

SELECT 
CONCAT('IF((`Finalizado` IS NULL) AND (`Removido` IS NULL) AND (`Rejeitado` IS NULL), 1, 0) as inCleaning, IF(`Finalizado` IS NOT NULL, 1, 0) as wasCleaned, IF(`Removido` IS NOT NULL, 1, 0) as wasRemoved, IF(`Rejeitado` IS NOT NULL, 1, 0) as wasRejected, CAST(TIMEDIFF(`Finalizado` , `Requisitado`) AS CHAR) AS `Tempo Total`, TIMESTAMPDIFF(SECOND, `Requisitado` , `Finalizado`) AS `Tempo Total SEC`,',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('CAST(TIMEDIFF(`Finalizado`, `Etapa ',tss.vch_name,'`)AS CHAR) AS `Dif. ',tss.vch_name,'-Fim`, TIMESTAMPDIFF(SECOND,`Etapa ',tss.vch_name,'`, `Finalizado`) AS `DifSec. ',tss.vch_name,'-Fim`,'), ''),'CAST(TIMEDIFF(`Etapa ',tss.vch_name,'`, `Etapa ',tss2.vch_name,'`) AS CHAR) AS `Dif. ',tss2.vch_name,'-',tss.vch_name,'`, CAST(TIMEDIFF(`Etapa ',tss1.vch_name,'`, `Etapa ',tss.vch_name,'`) AS CHAR) AS `Dif. ',tss.vch_name,'-',tss1.vch_name,'`, CAST(TIMEDIFF(`Etapa ',tss1.vch_name,'`, `Etapa ',tss2.vch_name,'`) AS CHAR) AS `Dif. ',tss2.vch_name,'-',tss1.vch_name,'`,
    TIMESTAMPDIFF(SECOND,`Etapa ',tss2.vch_name,'`, `Etapa ',tss.vch_name,'`)  AS `DifSec. ',tss2.vch_name,'-',tss.vch_name,'`, 
    TIMESTAMPDIFF(SECOND,`Etapa ',tss.vch_name,'`, `Etapa ',tss1.vch_name,'`) AS `DifSec. ',tss.vch_name,'-',tss1.vch_name,'`, 
    TIMESTAMPDIFF(SECOND,`Etapa ',tss2.vch_name,'`, `Etapa ',tss1.vch_name,'`) AS `DifSec. ',tss2.vch_name,'-',tss1.vch_name,'`
    ')
	WHEN tsfs.int_end_on_terminal_step_key > 0 AND tss1.vch_enum != 'inclean'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('CAST(TIMEDIFF(`Finalizado`, `Etapa ',tss.vch_name,'`)AS CHAR) AS `Dif. ',tss.vch_name,'-Fim`,TIMESTAMPDIFF(SECOND,`Etapa ',tss.vch_name,'`, `Finalizado`) AS `DifSec. ',tss.vch_name,'-Fim`,'), ''),'CAST(TIMEDIFF(`Etapa ',tss1.vch_name,'`, `Etapa ',tss.vch_name,'`) AS CHAR) AS `Dif. ',tss.vch_name,'-',tss1.vch_name,'`,
    TIMESTAMPDIFF(SECOND,`Etapa ',tss.vch_name,'`, `Etapa ',tss1.vch_name,'`) AS `DifSec. ',tss.vch_name,'-',tss1.vch_name,'`') 	
	WHEN tsfs.int_end_on_terminal_step_key = 0 AND tss.vch_type = 'active'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('CAST(TIMEDIFF(`Finalizado`, `Etapa ',tss.vch_name,'`)AS CHAR) AS `Dif. ',tss.vch_name,'-Fim`,TIMESTAMPDIFF(SECOND,`Etapa ',tss.vch_name,'`, `Finalizado`) AS `Dif. ',tss.vch_name,'-Fim`,'), ''),'CAST(TIMEDIFF(`Finalizado`, `Etapa ',tss.vch_name,'`) AS CHAR) AS `Dif. ',tss.vch_name,'-Final`,
    TIMESTAMPDIFF(SECOND,`Etapa ',tss.vch_name,'`, `Finalizado`) AS `DifSec. ',tss.vch_name,'-Final`')
END ORDER BY tss.int_order
)) AS diff_field
INTO @sql_columns2
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;
-- ORDER BY tss.int_order
-- select @sql_columns2;
SELECT GROUP_CONCAT(CONCAT('(`Stts`.`Etapa ',ts.vch_name,'` IS NOT NULL)') ORDER BY ts.int_order SEPARATOR ' OR ')
INTO @sql_columns3
FROM tbl_hlt_placecleaninglist_steps ts
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;
SET @tmp_query_2 = CONCAT('SELECT Stts.*, ',@report_type,' AS Grouped,' , @sql_columns2 , ' FROM (',@tmp_query_1_2,') AS Stts WHERE (', @sql_columns3, ')',IF(LENGTH(@report_filter_person) = 0, '', CONCAT(' AND Stts.Responsavel like "%',@report_filter_person,'%" ')));

PREPARE stmt FROM @tmp_query_2;
EXECUTE stmt;

END$$

DELIMITER ;