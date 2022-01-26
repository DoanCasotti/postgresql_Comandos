with tabela1 as(
select 
round(sum((COALESCE(di.quantidade,0)) * (COALESCE(di.valor_unitario,0))),2) as valor_un,
round(sum(COALESCE(di.quantidade,0))) as quantidade,
round(sum(COALESCE(di.valor_desconto,0)),2) as desconto,
round(sum((COALESCE(di.quantidade,0)) * (COALESCE(di.valor_unitario,0))),2) - round(sum(COALESCE(di.valor_desconto,0)),2) as valor_total,
date_trunc('day', d.data_registro) as data_registro,
EXTRACT(dow from d.data_registro) as dia_registro,
namedow(EXTRACT(dow from d.data_registro)::integer) as dia_semana
from documentos d 
left outer join documento_itens di on di.documento_id		= d.id
left outer join documento_historicos dh on dh.documento_id	= d.id
left outer join documento_status ds on ds.id				= dh.status_id
where ds.id = 6 
AND date_trunc('day', d.data_registro) >= '2021-12-01'::date AND date_trunc('day', d.data_registro) <= '2021-12-31'::date 
group BY date_trunc('day', d.data_registro), EXTRACT(dow from d.data_registro)
order by date_trunc('day', d.data_registro)
)

select
t1.*,
case t1.dia_registro when 1 THEN
	t1.valor_total
END as segunda,
case t1.dia_registro when 2 THEN
 	t1.valor_total
END as terca,
case t1.dia_registro when 3 THEN
	t1.valor_total
END as quarta,
case t1.dia_registro when 4 THEN
	t1.valor_total
END as quinta,
case t1.dia_registro when 5 THEN
	t1.valor_total
END as sexta,
case t1.dia_registro when 6 THEN
	t1.valor_total
END as sabado,
case t1.dia_registro when 7 THEN
	t1.valor_total
END as domingo
from tabela1 t1
group by t1.valor_un, t1.quantidade, t1.desconto, t1.valor_total, t1.data_registro, t1.dia_registro, t1.dia_semana