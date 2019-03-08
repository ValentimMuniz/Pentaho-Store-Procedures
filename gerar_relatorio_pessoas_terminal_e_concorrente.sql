DELIMITER $$


CREATE PROCEDURE gerar_relatorio_pessoas_2(IN tgid INT(11),IN start_time VARCHAR(255),IN end_time VARCHAR(255), IN order_field VARCHAR(255), IN estimated_concurrent_duration_sec INT(11))
BEGIN

SET SESSION group_concat_max_len = 10000;
SET @terminal_group_id = tgid;
SET @start_date = CONCAT(start_time,' 00:00:00');
SET @end_date = CONCAT(end_time,' 23:59:59');
SET @order_by = IFNULL(order_field, 'Colaborador');
SET @concurrent_duration_secs = IFNULL(estimated_concurrent_duration_sec, 600);
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

SELECT CONCAT(
"MAX(CASE WHEN (t1.int_action in (1,2)) THEN t1.int_personlisten_key END) AS ColaboradorKey,"
, GROUP_CONCAT(	
	CONCAT('MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS "',ts.vch_name,'"',IF(ts.int_IsStartStep = 1,CONCAT(',MIN(CASE WHEN (t1.int_placecleaninglist_step_key=',ts.int_key,') AND (t1.int_action IN (1,2,3)) THEN t1.dat_historicalDate END) AS Primeira_Etapa_',ts.int_key),''))
	ORDER BY ts.int_order)
, ",MIN(CASE WHEN (t1.int_placecleaninglist_step_key IS NOT NULL) AND (t1.int_action IN (1,2,3)) THEN DATE_FORMAT(t1.dat_historicalDate, '%d/%m/%Y') END) AS DiaRequisicaoLimpeza"
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
	'SELECT t1.int_placecleaninglist_key AS CleaningKey, place.vch_name as PlaceName, ', @sql_columns1, '
	FROM tbl_hlt_placecleaninglist_historicaldata t1 
	LEFT JOIN tbl_hlt_placecleaninglist_steps ts on t1.int_placecleaninglist_step_key = ts.int_key
	LEFT JOIN tbl_hlt_cleantype ct ON (t1.int_cleantype_key = ct.int_key)
	LEFT JOIN tbl_hlt_place place ON (t1.int_place_key = place.int_key)
	WHERE (t1.int_action IN (1,2,3,4)) AND t1.int_place_hospital_key = ',@hospital_id,'
	AND (t1.dat_historicalDate between "',@start_date,'" AND "',@end_date,'")
	GROUP BY t1.int_placecleaninglist_key');
	
-- select @tmp_query_1;

SELECT 
CONCAT('IF((`Finalizado` IS NULL) AND (`Removido` IS NULL) AND (`Rejeitado` IS NULL), 1, 0) as inCleaning, IF(`Finalizado` IS NOT NULL, 1, 0) as wasCleaned, IF(`Removido` IS NOT NULL, 1, 0) as wasRemoved, IF(`Rejeitado` IS NOT NULL, 1, 0) as wasRejected, ',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS `TempoTotalFila_Comecando_',tss.vch_name,'`,'), ''),'TIMESTAMPDIFF(SECOND, `',tss2.vch_name,'`, `',tss.vch_name,'`) AS "Tempo_',tss2.vch_name,'_',tss.vch_name,'", TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `',tss1.vch_name,'`) AS "Tempo_',tss.vch_name,'_',tss1.vch_name,'", TIMESTAMPDIFF(SECOND, `',tss2.vch_name,'`, `',tss1.vch_name,'`) AS "Tempo_Terminal_',tss2.vch_name,'_',tss1.vch_name,'"')
	WHEN tsfs.int_end_on_terminal_step_key > 0 AND tss1.vch_enum != 'inclean'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS `TempoTotalFila_Comecando_',tss.vch_name,'`,'), ''),'TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `',tss1.vch_name,'`) AS "Tempo_',tss.vch_name,'_',tss1.vch_name,'"') 	
	WHEN tsfs.int_end_on_terminal_step_key = 0 AND tss.vch_type = 'active'
	THEN CONCAT(IF(tss.int_IsStartStep = 1, CONCAT('TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS `TempoTotalFila_Comecando_',tss.vch_name,'`,'), ''),'TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `Finalizado`) AS "Tempo_',tss.vch_name,'_final"')
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

SELECT CONCAT("(Stts.ColaboradorKey is not null AND Stts.ColaboradorKey != 0) AND ",GROUP_CONCAT(CONCAT('(`Stts`.`',ts.vch_name,'` IS NOT NULL)') ORDER BY ts.int_order SEPARATOR ' OR '))
INTO @sql_columns3
FROM tbl_hlt_placecleaninglist_steps ts
WHERE ts.int_placecleaninglist_group_key = @terminal_group_id AND ts.vch_type IN ('active','passive')
GROUP BY ts.int_placecleaninglist_group_key;

-- select @sql_columns3;

SET @tmp_query_2 = CONCAT('SELECT Stts.*, ' , @sql_columns2 , ' FROM (',@tmp_query_1,') AS Stts WHERE ', @sql_columns3);

-- select @tmp_query_2;

SELECT 
CONCAT('',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT('SUM(TIMESTAMPDIFF(SECOND, `',tss.vch_name,'`, `',tss1.vch_name,'`)) AS "Total_Tempo_Terminal_',tss.vch_name,'_',tss1.vch_name,'", COUNT("Tempo_Terminal_',tss.vch_name,'_',tss1.vch_name,'") AS "Quantidade_Terminal_',tss.vch_name,'_',tss1.vch_name,'"')
END ORDER BY tss.int_order
)) AS diff_field
INTO @sql_columns4_1
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;

-- select @sql_columns4_1;

SELECT 
CONCAT('SUM(',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT('`Tempo_',tss.vch_name,'_',tss1.vch_name,'`')
END ORDER BY tss.int_order SEPARATOR '+'
),') as "TempoTotalTrabalhadoTerminal", COUNT(',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT('`Tempo_',tss.vch_name,'_',tss1.vch_name,'`')
END ORDER BY tss.int_order SEPARATOR '*'
),') as "QuantidadeTerminal"') AS diff_field
INTO @sql_columns4_2
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;

-- select @sql_columns4_2;

SET @tmp_query_3 = CONCAT('
		SELECT 
		DiaRequisicaoLimpeza, 
		Times.ColaboradorKey, 
		(SELECT COALESCE(NULLIF(pr.vch_fullname, ""),pr.vch_name) FROM tbl_hlt_person pr WHERE pr.int_key = Times.ColaboradorKey) as Colaborador, 
		',@sql_columns4_1,',',@sql_columns4_2,'
		,(
			SELECT 
				COUNT(distinct NULLIF(t2.int_placeconcurrent_key, 0)) 
			FROM 
				tbl_hlt_placeconcurrent_historicaldata t2 
			WHERE 
				DATE_FORMAT(t2.dat_historicalDate, "%d/%m/%Y")=DiaRequisicaoLimpeza 
				and t2.int_person_key_responsible = Times.ColaboradorKey
		) as QuantidadeConcorrente 
	FROM (',@tmp_query_2,') as Times 
	GROUP BY 
		DiaRequisicaoLimpeza, ColaboradorKey
');

-- select @tmp_query_3;

SELECT 
CONCAT('',GROUP_CONCAT(
CASE 	
	WHEN (tsfs.int_start_from_terminal_step_key > 0 AND tsfs.int_end_on_terminal_step_key > 0)
	THEN CONCAT('AVG(`Total_Tempo_Terminal_',tss.vch_name,'_',tss1.vch_name,'`) as "Tempo Medio Trabalhado Terminal(',tss.vch_name,'-',tss1.vch_name,') por dia"',
	',AVG(`Quantidade_Terminal_',tss.vch_name,'_',tss1.vch_name,'`) as "Quantidade Media Terminal(',tss.vch_name,'-',tss1.vch_name,')"',
	',AVG(`Total_Tempo_Terminal_',tss.vch_name,'_',tss1.vch_name,'`/`Quantidade_Terminal_',tss.vch_name,'_',tss1.vch_name,'`) as "Tempo Medio Cada Terminal(',tss.vch_name,'-',tss1.vch_name,')"')
END ORDER BY tss.int_order
)) AS diff_field
INTO @sql_columns5
FROM tbl_hlt_placecleaninglist_step_flows tsfs
INNER JOIN tbl_hlt_placecleaninglist_steps tss ON tss.int_key = tsfs.int_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss1 ON tss1.int_key = tsfs.int_end_on_terminal_step_key
LEFT JOIN tbl_hlt_placecleaninglist_steps tss2 ON tss2.int_key = tsfs.int_start_from_terminal_step_key
WHERE tss.int_placecleaninglist_group_key = @terminal_group_id AND tss.vch_type IN ('active','passive') 
GROUP BY tss.int_placecleaninglist_group_key;

-- select @sql_columns5;

SET @tmp_query_4 = CONCAT('
	SELECT 
	Totais.Colaborador, 
	COUNT(Totais.DiaRequisicaoLimpeza) as "Dias Trabalhados",  
	SUM(Totais.QuantidadeConcorrente) as "Quantidade Total Concorrentes",
	AVG(Totais.QuantidadeConcorrente*',@concurrent_duration_secs,') as "Tempo Médio Estimado Trabalhado Concorrente por Dia", 
	AVG(Totais.QuantidadeConcorrente) as "Quantidade Média Concorrentes", ',@sql_columns5,',
	AVG(Totais.TempoTotalTrabalhadoTerminal) as "Tempo Médio Trabalhado Terminal por Dia", 
	AVG(Totais.QuantidadeTerminal) as "Quantidade Média de Terminais", 	
	AVG(Totais.TempoTotalTrabalhadoTerminal/Totais.QuantidadeTerminal) as "Tempo Médio de cada Terminal"
	FROM 
	(',@tmp_query_3,') as Totais 
	WHERE 
		Colaborador is not null 
	GROUP BY 
		Colaborador 
	ORDER BY
		"',@order_by,'" DESC
');

PREPARE stmt FROM @tmp_query_4;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;