DELIMITER $$


DROP PROCEDURE IF EXISTS `gerar_relatorio_andamentos_filas_checklist`$$

CREATE PROCEDURE `gerar_relatorio_andamentos_filas_checklist`(
	IN tgid INT(11),
	IN start_time VARCHAR(255),
	IN end_time VARCHAR(255), 
	IN start_time_hour VARCHAR(255),
	IN start_time_minute VARCHAR(255),
	IN end_time_hour VARCHAR(255), 
	IN end_time_minute VARCHAR(255), 
	IN rp_filter_room VARCHAR(255), 
	IN rp_filter_cleantype VARCHAR(255),
	IN rp_filter_person VARCHAR(255),
	IN rp_filter_person_responsible VARCHAR(255)
	
)
BEGIN
SET SESSION group_concat_max_len = 20000 ;
SET @report_filter_cleantype = rp_filter_cleantype;
SET @report_filter_room = rp_filter_room;
SET @report_filter_person = rp_filter_person;
SET @report_filter_person_responsible = rp_filter_person_responsible;
SET @terminal_group_id = tgid;
SET @start_date = start_time;
SET @end_date = end_time;
SET @start_date_hour_ini = start_time_hour;
SET @end_date_hour_fi = end_time_hour;
SET @start_date_minute_ini = start_time_minute;
SET @end_date_minute_fi = end_time_minute;
SET @toView = "Grouped";
SET @tmp_query_1 = '';
SET @tmp_query_0 = '';
SET @tmp_query_1_2 = '';
SET @tmp_query_2 = '';
SET @sql_columns1 = '';
SET @sql_columns1_2 = '';
SET @sql_columns1_3 = '';
SET @sql_columns2 = '';
SET @sql_columns3 = '';

SELECT CONCAT(GROUP_CONCAT(
	CONCAT('MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS "Etapa ',ts.vch_name,'", MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS "EtapaID ',ts.int_key,'"',IF(ts.int_IsStartStep = 1,CONCAT(',MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS Primeira_Etapa_',ts.int_key),''))
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
SELECT CONCAT(GROUP_CONCAT(
	CONCAT('GROUP_CONCAT(DISTINCT
			case
				WHEN t1.int_placecleaninglist_step_key =',ts.int_key,' AND person.int_key is not null
                THEN person.vch_name   
            end separator ", "
    ) AS "Participantes - ',ts.vch_name,'"')
	ORDER BY ts.int_order))
INTO @sql_columns1_2 
FROM tbl_hlt_placecleaninglist_steps ts
INNER JOIN tbl_hlt_placecleaninglist_step_flows tsfs ON ts.int_key = tsfs.int_terminal_step_key
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;

SELECT CONCAT(GROUP_CONCAT(
	CONCAT('
			MIN(
				CASE 
					WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (1,2,3)) AND ts.vch_name = "',ts.vch_name,'"
				THEN t1.dat_historicalDate END) AS "Requisitado - ',ts.vch_name,'",
			MIN(
				CASE 
					WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (1,2,3)) AND ts.vch_name = "',ts.vch_name,'"
				THEN TIME_TO_SEC(TIME(t1.dat_historicalDate)) END) AS "RequisitadoEtapaSec - ',ts.vch_name,'"'
			) 
            
	ORDER BY ts.int_order))
INTO @sql_columns1_3
FROM tbl_hlt_placecleaninglist_steps ts
INNER JOIN tbl_hlt_placecleaninglist_step_flows tsfs ON ts.int_key = tsfs.int_terminal_step_key
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;

SET @tmp_query_0 = CONCAT('
SELECT
DISTINCT int_placecleaninglist_key as KEYCLEAN, 
MIN(IF(int_action = 1, dat_historicalDate, NULL)) as entrada, 
MAX(IF(int_action = 4, dat_historicalDate, NULL)) as saida
FROM tbl_hlt_placecleaninglist_historicaldata t1
left JOIN tbl_hlt_person person_lancou ON t1.int_person_key_responsible = person_lancou.int_key 
left join tbl_hlt_employee employee_lancou on employee_lancou.int_person_key = person_lancou.int_key
left join tbl_hlt_administrator adm on adm.int_employee_key = employee_lancou.int_key
left JOIN tbl_hlt_person person ON t1.int_personlisten_key = person.int_key 
left join tbl_hlt_employee employee on employee.int_person_key = person.int_key
left join tbl_hlt_user uc on uc.int_employee_key = employee.int_key
LEFT JOIN tbl_hlt_placecleaninglist_steps ts on t1.int_placecleaninglist_step_key = ts.int_key
LEFT JOIN tbl_hlt_cleantype ct ON (t1.int_cleantype_key = ct.int_key)
LEFT JOIN tbl_hlt_place place ON (t1.int_place_key = place.int_key)
left join tbl_hlt_placecleaninglist_groups tg on t1.int_placecleaninglist_group_key = tg.int_key
 
WHERE (t1.int_action IN (1,2,3,4)) AND t1.int_placecleaninglist_group_key = ',@terminal_group_id,'
	  AND DATE(t1.dat_historicalDate) >= "',@start_date,'" AND DATE(t1.dat_historicalDate) <= "',@end_date,'"
      AND TIME(t1.dat_historicalDate) BETWEEN TIME("',@start_date_hour_ini,':',@start_date_minute_ini,':00") and TIME("',@end_date_hour_fi,':',@end_date_minute_fi,':59")
    
    ', IF(LENGTH(@report_filter_room) = 0, '' , CONCAT(' AND place.vch_name like "%',@report_filter_room,'%"')), 
	   IF(@report_filter_cleantype = "Todos os tipos", '' , CONCAT(' AND ct.vch_name = "',@report_filter_cleantype,'" ')), 
       '
	GROUP BY int_placecleaninglist_key
HAVING saida IS NOT NULL
');

SET @tmp_query_1 = CONCAT(
	'SELECT t1.int_placecleaninglist_key AS PlaceKey, place.vch_name as PlaceName,
	ct.vch_name as `Tipo de Limpeza`,
	tg.vch_name AS Nome_Fila,
	
    GROUP_CONCAT(
		CASE        
			WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (ts.int_isStartStep = 1) AND (t1.dat_historicalDate IS NOT NULL) AND (t1.int_action IN (1)) THEN IFNULL(person_lancou.vch_name, "")                    
        END
    ) 
    as `Lancado Por`,
    
    GROUP_CONCAT(DISTINCT
			case
                WHEN (person_lancou.int_key is not null) AND (t1.int_placecleaninglist_step_key IS NULL) AND (t1.dat_historicalDate IS NOT NULL) AND (t1.int_action IN (4)) AND t1.vch_extra_meaning = "room_cleaned" 
                THEN person_lancou.vch_name
            end
    ) 
    as Supervisao,
                    
    GROUP_CONCAT(DISTINCT
			case
				WHEN person.int_key is not null 
                THEN
					CONCAT(person.vch_name, " | ", IF(ts.vch_enum <> "allocated", ts.vch_name, null))
				WHEN person.int_key is null and person_lancou.int_key is not null and ts.vch_enum = "inclean"
                THEN
					CONCAT(person_lancou.vch_name, " | ", IF(ts.vch_enum <> "allocated", ts.vch_name, null))                
				WHEN (person_lancou.int_key is not null) AND (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.dat_historicalDate IS NOT NULL) AND (t1.int_action IN (2,3)) AND t1.vch_attribute = "int_order" 
                THEN
					CONCAT(person_lancou.vch_name, " (requisitou prioridade)")
                WHEN (person_lancou.int_key is not null) AND (t1.int_placecleaninglist_step_key IS NULL) AND (t1.dat_historicalDate IS NOT NULL) AND (t1.int_action IN (4)) AND t1.vch_extra_meaning = "room_cleaned" 
                THEN
					CONCAT(person_lancou.vch_name, " (supervisionou)")
            end order by t1.int_key asc, ts.int_order asc SEPARATOR "\n\n"
    ) 
    as Participantes,
    
                              
    ', @sql_columns1, ' , ', @sql_columns1_2, ', ', @sql_columns1_3, '
	FROM tbl_hlt_placecleaninglist_historicaldata t1
    left JOIN tbl_hlt_person person_lancou ON t1.int_person_key_responsible = person_lancou.int_key 
	left join tbl_hlt_employee employee_lancou on employee_lancou.int_person_key = person_lancou.int_key
	left join tbl_hlt_administrator adm on adm.int_employee_key = employee_lancou.int_key
    left JOIN tbl_hlt_person person ON t1.int_personlisten_key = person.int_key 
	left join tbl_hlt_employee employee on employee.int_person_key = person.int_key
	left join tbl_hlt_user uc on uc.int_employee_key = employee.int_key
	LEFT JOIN tbl_hlt_placecleaninglist_steps ts on t1.int_placecleaninglist_step_key = ts.int_key
	LEFT JOIN tbl_hlt_cleantype ct ON (t1.int_cleantype_key = ct.int_key)
	LEFT JOIN tbl_hlt_place place ON (t1.int_place_key = place.int_key)
	left join tbl_hlt_placecleaninglist_groups tg on t1.int_placecleaninglist_group_key = tg.int_key
    
	WHERE t1.int_placecleaninglist_key IN(select KEYCLEAN from (', @tmp_query_0,') as query0)
    Group by PlaceKey');
    
    SET @tmp_query_1_2 = CONCAT('select * FROM (',@tmp_query_1,') as Sem_Excluidos where `Etapa em CheckList` is not null and `Rejeitado` is null');
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


SELECT GROUP_CONCAT(CONCAT('(`Stts`.`Etapa ',ts.vch_name,'` IS NOT NULL)') ORDER BY ts.int_order SEPARATOR ' OR ')
INTO @sql_columns3
FROM tbl_hlt_placecleaninglist_steps ts
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;
SET @tmp_query_2 = CONCAT('SELECT Stts.*, ', @sql_columns2 , ' FROM (',@tmp_query_1_2,') AS Stts WHERE (', @sql_columns3, ')',IF(LENGTH(@report_filter_person) = 0, '', CONCAT(' AND Stts.Participantes like "%',@report_filter_person,'%" ')), IF(LENGTH(@report_filter_person_responsible) = 0, '', CONCAT(' AND `Stts`.`Lancado Por` like "%',@report_filter_person_responsible,'%" ')));

PREPARE stmt FROM @tmp_query_2;
EXECUTE stmt;


END$$

DELIMITER ;