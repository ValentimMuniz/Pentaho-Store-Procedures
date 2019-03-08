DELIMITER $$

DROP PROCEDURE IF EXISTS `gerar_relatorio_tempo_limpeza_concorrente`$$

CREATE PROCEDURE `gerar_relatorio_tempo_limpeza_concorrente`(
	IN start_time VARCHAR(255), 
	IN end_time VARCHAR(255),
    IN filter_person VARCHAR(255),
    IN filter_cleantype VARCHAR(255)
)
BEGIN

SET @start_date = CONCAT(start_time, ' 00:00:00');
SET @end_date = CONCAT(end_time, ' 23:00:59');
SET @tmp_query_1 = '';
SET @report_filter_person = IFNULL(filter_person, 'Todos');
SET @report_filter_cleantype = IFNULL(filter_cleantype, 'Todos');
SELECT CONCAT('select place.int_key as PlaceKey, 
place.vch_name as Lugar, 
pc.dat_historicalDate as Data, 
ct.vch_name as TipoConcorrente,
person.vch_name as Pessoa 
from tbl_hlt_placeconcurrent_historicaldata pc 
LEFT JOIN tbl_hlt_person person 
on person.int_key = pc.int_person_key_responsible 
LEFT JOIN tbl_hlt_place place 
on place.int_key = pc.int_place_key  
left join tbl_hlt_concurrent_cleaning_types ct 
on ct.int_key = pc.int_concurrent_cleaning_type_key
where dat_historicalDate between "',@start_date,'" and "',@end_date,'"
AND int_action IN(4) AND ct.int_is_not_cleaned_reason IN(0)
', IF(@report_filter_person = 'Todos', '', CONCAT(' AND person.vch_name = "', @report_filter_person ,'" ')),
   IF(@report_filter_cleantype = 'Todos', '', CONCAT(' AND ct.vch_name = "', @report_filter_cleantype ,'" '))
) INTO @tmp_query_1;



PREPARE stmt FROM @tmp_query_1;
EXECUTE stmt;

END$$

DELIMITER ;