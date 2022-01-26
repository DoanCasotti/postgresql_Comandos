with ranks as(
select me.id as movimento_id,
       me.item_id as item_id,
       i.nome as item_nome,
       idd.preco_custo as preco_custo,
       idd.preco_venda as preco_venda,
       round((me.saldo_atual_data), 3) as saldo,
       round((round((me.saldo_atual_data), 3) * idd.preco_custo), 3) as
        preco_custo_total,
       round((round((me.saldo_atual_data), 3) * idd.preco_venda), 3) as
        preco_venda_total,
       i.item_tributacao_id as id_tributacao,
       it.nome as nome_tributacao,
       COALESCE(i.origem_mercadoria, 0) || lpad(concat(it.cst_icms_id, 0), 2,
        '0') as cst,
       u.sigla as unidade,
       e.nome as empresa,
       e.cpf_cnpj,
       e.rg_ie,
       ita.aliquota,
       rank() over(partition by me.item_id, me.estoque_id, me.lote_codigo,
        me.empresa_id
order by me.id desc) as i
from movimentacao_estoque me
     inner join itens i on i.id = me.item_id
     inner join item_dados idd on idd.item_id = me.item_id and idd.empresa_id =
      1
     inner join item_tributacoes it on it.id = i.item_tributacao_id
     inner join unidades u on u.id = i.unidade_id
     inner join pessoas e on e.id = me.empresa_id
     inner join item_tributacao_aliquotas ita on ita.tributacao_id =
      i.item_tributacao_id and ita.uf_origem = 'ES' and ita.uf_destino = 'ES'
where me.empresa_id = 1 and
      (me.item_id = 0 or
      0 = 0 or
      me.item_id is null) and
      round((me.saldo_atual_data), 2) > 0 and
      i.inventario = TRUE AND
      date_trunc('day', me.data_movimento) <= '2022-01-06' ::date)
select r.item_id as item_id,
       r.item_nome as item_nome,
       r.preco_custo as preco_custo,
       sum(r.saldo) as saldo,
       r.preco_custo_total,
       round((r.preco_custo_total * r.aliquota / 100), 4) as valor_icms_custo,
       r.preco_venda as preco_venda,
       r.preco_venda_total,
       round((r.preco_venda_total * r.aliquota / 100), 4) as valor_icms_venda,
       r.id_tributacao,
       r.nome_tributacao,
       r.cst as cst,
       r.unidade,
       r.empresa,
       r.cpf_cnpj,
       r.rg_ie,
       r.aliquota,
       round(COALESCE((
                        select di.valor_unitario
                        from documentos d
                             inner join documento_itens di on di.documento_id =
                              d.id and di.item_id = r.item_id
                             inner join documento_historicos dh on
                              dh.documento_id = d.id and dh.sequencia =
                             (
                               select max(x.sequencia)
                               from documento_historicos x
                               where x.documento_id = d.id
                             )
                             inner join documento_status ds on ds.id =
                              dh.status_id and ds.movimento_estoque
                        where d.tipo = 5 and
                              d.data_movimento <= '2022-01-06' ::date
                        order by d.data_movimento desc,
                                 di.item_sequencia desc
                        limit 1
       ), 0), 4) as preco_nfe,
       round(COALESCE((
                        select di.valor_unitario
                        from documentos d
                             inner join documento_itens di on di.documento_id =
                              d.id and di.item_id = r.item_id
                             inner join documento_historicos dh on
                              dh.documento_id = d.id and dh.sequencia =
                             (
                               select max(x.sequencia)
                               from documento_historicos x
                               where x.documento_id = d.id
                             )
                             inner join documento_status ds on ds.id =
                              dh.status_id and ds.movimento_estoque
                        where d.tipo = 5 and
                              d.data_movimento <= '2022-01-06' ::date
                        order by d.data_movimento desc,
                                 di.item_sequencia desc
                        limit 1
       ), 0) * sum(r.saldo), 4) as preco_nfe_total,
       (COALESCE((
                   select di.valor_unitario
                   from documentos d
                        inner join documento_itens di on di.documento_id = d.id
                         and di.item_id = r.item_id
                        inner join documento_historicos dh on dh.documento_id =
                         d.id and dh.sequencia =
                        (
                          select max(x.sequencia)
                          from documento_historicos x
                          where x.documento_id = d.id
                        )
                        inner join documento_status ds on ds.id = dh.status_id
                         and ds.movimento_estoque
                   where d.tipo = 5 and
                         d.data_movimento <= '2022-01-06' ::date
                   order by d.data_movimento desc,
                            di.item_sequencia desc,
                            di.item_id
                   limit 1
       ), 0) * sum(r.saldo)) *(r.aliquota / 100) as valor_icms_nf
from ranks r
where r.i = 1
group by r.item_id,
         r.item_nome,
         r.preco_custo,
         r.preco_venda,
         r.id_tributacao,
         r.nome_tributacao,
         r.empresa,
         r.cpf_cnpj,
         r.rg_ie,
         r.unidade,
         r.preco_custo_total,
         r.preco_venda_total,
         r.cst,
         r.aliquota
order by r.id_tributacao,
         r.item_nome