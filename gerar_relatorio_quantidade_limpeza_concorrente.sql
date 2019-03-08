DELIMITER $$


DROP PROCEDURE IF EXISTS `gerar_relatorio_quantidade_limpeza_concorrente`$$

CREATE PROCEDURE `gerar_relatorio_quantidade_limpeza_concorrente`(
	IN start_time VARCHAR(255),
	IN end_time VARCHAR(255)
)
BEGIN

SET @start_date = CONCAT(start_time, ' 00:00:00');
SET @end_date = CONCAT(end_time, ' 23:00:59');
SET @tmp_query_1 = '';

SELECT CONCAT('select COUNT(*) as TotalQuartos, DATE(t1.dat_historicalDate) as Data 
from tbl_hlt_placeconcurrent_historicaldata t1
left join tbl_hlt_concurrent_cleaning_types ct on ct.int_key = t1.int_concurrent_cleaning_type_key
where (t1.int_action IN(4) and t1.dat_historicaldate between "',@start_date,'" and "',@end_date,'"
and t1.int_placeconcurrent_key > 0) and ct.int_is_not_cleaned_reason IN(0)
group by DATE(t1.dat_historicalDate) order by Data ASC
') INTO @tmp_query_1;
 

PREPARE stmt FROM @tmp_query_1;
EXECUTE stmt;

END$$

DELIMITER ;