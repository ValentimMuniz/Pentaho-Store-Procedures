DELIMITER $$

DROP PROCEDURE `gerar_relatorio_pessoas_concorrente`$$

CREATE PROCEDURE `gerar_relatorio_pessoas_concorrente`(IN start_time VARCHAR(255),IN end_time VARCHAR(255), IN order_field VARCHAR(255), IN estimated_concurrent_duration_sec INT(11))
BEGIN

SET SESSION group_concat_max_len = 10000;
SET @start_date = CONCAT(start_time,' 00:00:00');
SET @end_date = CONCAT(end_time,' 23:59:59');
SET @order_by = IFNULL(order_field, 'Colaborador');
SET @concurrent_duration_secs = IFNULL(estimated_concurrent_duration_sec, 600);
SET @tmp_query_1 = '';
SET @tmp_query_1_2 = '';
SET @tmp_query_2 = '';
SET @tmp_query_3 = '';
SET @tmp_query_4 = '';
SET @sql_columns1 = '';

SELECT CONCAT(
"MAX(CASE WHEN (t2.int_action in (1,2,4)) THEN t2.int_person_key_responsible END) AS ColaboradorKey"
, ",MIN(CASE WHEN (t2.int_action IN (1,2,4)) THEN DATE_FORMAT(t2.dat_historicalDate, '%d/%m/%Y') END) AS DiaRequisicaoLimpeza"
)
INTO @sql_columns1;

SET @tmp_query_1 = CONCAT(
	'SELECT t2.int_placeconcurrent_key AS CleaningKey, ',@sql_columns1,'
	FROM tbl_hlt_placeconcurrent_historicaldata t2 
	LEFT JOIN tbl_hlt_place place ON (t2.int_place_key = place.int_key)
    left join tbl_hlt_concurrent_cleaning_types ct on ct.int_key = t2.int_concurrent_cleaning_type_key
	WHERE  (t2.dat_historicalDate between "',@start_date,'" AND "',@end_date,'") AND 
    ct.int_is_not_cleaned_reason IN(0) and t2.int_action in(4)
   GROUP BY CleaningKey');


SET @tmp_query_3 = CONCAT('
		SELECT 
		DiaRequisicaoLimpeza, 
		Times.ColaboradorKey, 
		(SELECT COALESCE(NULLIF(pr.vch_fullname, ""),pr.vch_name) FROM tbl_hlt_person pr WHERE pr.int_key = Times.ColaboradorKey) as Colaborador
		,(
			SELECT 
				COUNT(distinct NULLIF(t2.int_placeconcurrent_key, 0)) 
			FROM 
				tbl_hlt_placeconcurrent_historicaldata t2 
				left join tbl_hlt_concurrent_cleaning_types ct on ct.int_key = t2.int_concurrent_cleaning_type_key
			WHERE 
				DATE_FORMAT(t2.dat_historicalDate, "%d/%m/%Y")=DiaRequisicaoLimpeza 
				and t2.int_person_key_responsible = Times.ColaboradorKey
                and ct.int_is_not_cleaned_reason IN(0) and t2.int_action in(4)
		) as QuantidadeConcorrente 
	FROM (',@tmp_query_1,') as Times 
	GROUP BY 
		DiaRequisicaoLimpeza, ColaboradorKey
');

-- select @tmp_query_3;

SET @tmp_query_4 = CONCAT('
	SELECT 
	Totais.Colaborador, 
	COUNT(Totais.DiaRequisicaoLimpeza) as "Dias Trabalhados",  
	SUM(Totais.QuantidadeConcorrente) as "Quantidade Total Concorrentes",
	AVG(Totais.QuantidadeConcorrente*',@concurrent_duration_secs,') as "Tempo Médio Estimado Trabalhado Concorrente por Dia", 
	AVG(Totais.QuantidadeConcorrente) as "Quantidade Média Concorrentes"
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


END$$

DELIMITER ;