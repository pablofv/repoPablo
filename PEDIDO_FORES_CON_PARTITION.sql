select count(DISTINCT ID_CAMBIO_ASIGNACION_EXP),CANTIDAD,ID_CAMARA
from (select count(*) over(partition by i.ID_CAMBIO_ASIGNACION_EXP) CANTIDAD,i.* from EST_PEDIDO_FORES i join DELITO_EXPEDIENTE de on de.ID_EXPEDIENTE = i.ID_EXPEDIENTE
JOIN delito d on d.ID_DELITO = de.ID_DELITO
where d.articulo in (256, 257, 258, 259, 260, 261, 265, 266, 267, 268)
        or (articulo in (173) and inciso in (7))
        or (articulo in (174) and inciso in (5)))
group by CANTIDAD,ID_CAMARA
order by ID_CAMARA, CANTIDAD
;