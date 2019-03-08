DELIMITER $$

DROP PROCEDURE IF EXISTS `gerar_relatorio_totais`$$

CREATE PROCEDURE gerar_relatorio_totais(
	IN tgid INT(11),
	IN start_time VARCHAR(255),
	IN end_time VARCHAR(255)
)
BEGIN
SET SESSION group_concat_max_len = 10000 ;
SET @report_type = "PlaceKey";
SET @terminal_group_id = tgid;
SET @start_date = CONCAT(start_time,' 00:00:00');
SET @end_date = CONCAT(end_time,' 23:59:59');
SET @toView = "Grouped";
SET @tmp_query_1 = '';
SET @tmp_query_1_2 = '';
SET @tmp_query_2 = '';
SET @tmp_query_3 = '';
SET @sql_columns1 = '';
SET @sql_columns2 = '';
SET @sql_columns3 = '';
SET @sql_columns4 = '';
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
	'SELECT t1.int_placecleaninglist_key AS PlaceKey, place.vch_name as PlaceName,ct.vch_name as `Tipo de Limpeza`,
	
    GROUP_CONCAT(
		CASE        
			WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (ts.int_isStartStep = 1) AND (t1.dat_historicalDate IS NOT NULL) AND (t1.int_action IN (1)) THEN IFNULL(person_lancou.vch_name, "")                    
        END
    ) 
    as `Lancado Por`,
    
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
    COUNT(*) as TotalLimpezas,
                                              
    ', @sql_columns1, '
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
    
    
	WHERE (t1.int_action IN (1,2,3,4)) AND t1.int_placecleaninglist_group_key = ',@terminal_group_id,'
	AND (t1.dat_historicalDate between "',@start_date,'" AND "',@end_date,'")
	GROUP BY t1.int_placecleaninglist_key');
    
    SET @tmp_query_1_2 = CONCAT('select * FROM (',@tmp_query_1,') as Sem_Excluidos where `Removido` is null and `Rejeitado` is null and `Finalizado` is not null');
SET @tmp_query_3 = CONCAT('SELECT teste.Finalizado AS DataFinalizado, COUNT(teste.DiaRequisicaoLimpeza) as QtdRegistros FROM (',@tmp_query_1_2,') AS teste Group By DATE_FORMAT(teste.Finalizado, "%Y-%m-%d")');
-- select @tmp_query_3;
PREPARE stmt FROM @tmp_query_3;
EXECUTE stmt;
END$$

DELIMITER ;