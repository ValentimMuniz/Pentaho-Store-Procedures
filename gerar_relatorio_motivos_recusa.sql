DELIMITER $$

DROP PROCEDURE IF EXISTS `gerar_relatorio_motivos_recusa`$$

CREATE PROCEDURE `gerar_relatorio_motivos_recusa`(
	IN start_time VARCHAR(255),
	IN end_time VARCHAR(255),
    IN filter_person VARCHAR(255),
    IN filter_motive VARCHAR(255)
)
BEGIN

SET @start_date = CONCAT(start_time, ' 00:00:00');
SET @end_date = CONCAT(end_time, ' 23:00:59');
SET @tmp_query_1 = '';
SET @report_filter_person = IFNULL(filter_person, 'Todos');
SET @report_filter_motive = IFNULL(filter_motive, 'Todos');

SELECT CONCAT('select ct.vch_name as Recusa, 
place.vch_name as Lugar, 
pchd.dat_historicalDate as Data, 
person.vch_name as Pessoa 
from tbl_hlt_placeconcurrent_historicaldata pchd
left join tbl_hlt_concurrent_cleaning_types ct on ct.int_key = pchd.int_concurrent_cleaning_type_key
LEFT JOIN tbl_hlt_person person on person.int_key = pchd.int_person_key_responsible 
LEFT JOIN tbl_hlt_place place on place.int_key = pchd.int_place_key  
where dat_historicalDate between "',@start_date,'" and "',@end_date,'"
AND ct.int_is_not_cleaned_reason IN(1) AND int_action IN(4)
', IF(@report_filter_person = 'Todos', '', CONCAT(' AND person.vch_name = "', @report_filter_person ,'" ')),
   IF(@report_filter_motive = 'Todos', '', CONCAT(' AND ct.vch_name = "', @report_filter_motive ,'" '))
) INTO @tmp_query_1;


PREPARE stmt FROM @tmp_query_1;
EXECUTE stmt;

END$$

DELIMITER ;