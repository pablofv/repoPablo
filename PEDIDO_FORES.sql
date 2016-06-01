/*
Programa....: PEDIDO_FORES.SQL
Entorno.....: MCENTRA
Localización: Proyecto Litigiosidad\LEX100\GitLex100\Algoritmos Estadisticas\SQL Definitivo


Descripción.: Calcula los expedientes ingresados en el fuero federal y ordinario (cámara 8 y 9) en base a los delitos que pide FORES.

Aclaración: como una causa puede tener mas de un delito, se informa cantidad de ingresos y cantidad de delitos por ingresos
*/

/* De acuerdo al select que me proveyeron (ingresados.sql), genero los ingresos de primer instancia de todo el fuero 8 y 9 para el año 2015 
   y los guardo en la tabla PEDIDO_FORES */

create table pedido_FORES as
SELECT *
from (select ROW_NUMBER() over(partition by c.ID_EXPEDIENTE,o2.ID_OFICINA order by FECHA_ASIGNACION ) rn,
          --   OFICINA_RADICACION_MOD(c.ID_CAMBIO_ASIGNACION_EXP,c.FECHA_ASIGNACION,c.ID_EXPEDIENTE,c.ID_OFICINA,c.ID_SECRETARIA) OFICINA_RADICACION,
             c.*,
             o2.DESCRIPCION JUZGADO,
             o2.SIGLA_CEDULAS,
             o.ID_TIPO_INSTANCIA,
             e.NUMERO_EXPEDIENTE,
             e.ANIO_EXPEDIENTE,
             o.ID_CAMARA,
             e.CLAVE_EXPEDIENTE,
             e.EN_TRAMITE,
            o2.ID_OFICINA ID_JUZGADO
      from CAMBIO_ASIGNACION_EXP c
           JOIN EXPEDIENTE e on e.status = 0 and e.ID_EXPEDIENTE = c.ID_EXPEDIENTE and e.NATURALEZA_EXPEDIENTE in ('P')
           JOIN OFICINA o on c.ID_OFICINA = o.ID_OFICINA
           JOIN OFICINA o2 on o2.ID_OFICINA = (CASE WHEN o.ID_OFICINA_SUPERIOR is NULL then o.ID_OFICINA else o.ID_OFICINA_SUPERIOR END)
      where c.status = 0
      and (c.CODIGO_TIPO_CAMBIO_ASIGNACION != 'IJM'       -- Se Excluyen los ingresos a Mediación
           or c.CODIGO_TIPO_CAMBIO_ASIGNACION is null     -- Se incluyen los que no poseen código alguno
          )
      and o.ID_TIPO_INSTANCIA = 1
      and (o.ID_CAMARA  not in (9, 99) OR (o.ID_CAMARA = 9 and o2.SIGLA_CEDULAS in ('CI', 'CR','JNM', 'RO'))
          )
     )
where rn = 1
  and trunc(FECHA_ASIGNACION) between to_date('20150101', 'YYYYMMDD') and to_date('20151231', 'YYYYMMDD')
  and id_camara in (8,9)
--GROUP BY ID_CAMARA, JUZGADO
order by ID_CAMARA, JUZGADO
;

/* Una vez obtenidos estos, los relaciono con la tabla delito expediente para saber que delitos tienen cada uno y a su vez lo relaciono
   con la tabla de delitos para saber cuales son aquellas causas del pedido */
/* los delitos id_delito = 566 y 567 los excluí de acuerdo a una revisión que hicimos junto a Inés, el resto de los delitos son los pedidos
   de acuerdo a los artículos e incisos que figuran en la tabla DELITOS */
/* resultado en MCENTRA 1104 registros */

select d.DESCRIPCION_DELITO, /*d.articulo, d.inciso, */p.ID_CAMARA, count(*) -- id_cambio_asignacion_exp, count(*)
from EST_PEDIDO_FORES p JOIN DELITO_EXPEDIENTE de on p.ID_EXPEDIENTE = de.ID_EXPEDIENTE
                        join delito d on de.ID_DELITO = d.ID_DELITO
where (d.articulo in (256, 257, 258, 259, 260, 261, 265, 266, 267, 268) or (articulo in (173) and inciso in (7) or (articulo in (174) and inciso in (5))))
and   d.id_delito not in (566, 567)
-- group by p.ID_CAMBIO_ASIGNACION_EXP
group by p.id_camara, d.descripcion_delito--, d.articulo, d.inciso
order by /*4,2,3 */ p.id_camara, d.descripcion_delito
;

/* Para saber cuantos ingresos diferentes son */
/* resultado en MCENTRA 935 ingresos */

select p.ID_CAMBIO_ASIGNACION_EXP
from est_pedido_fores p
where exists (select 1
              from DELITO_EXPEDIENTE de join delito d on d.id_delito = de.id_delito
              where de.id_expediente = p.id_expediente
              and   (d.articulo in (256, 257, 258, 259, 260, 261, 265, 266, 267, 268) or (articulo in (173) and inciso in (7) or (articulo in (174) and inciso in (5))))
              and   d.id_delito not in (566, 567)) -- luego de un análisis decidimos excluir estos dos delitos
;
/* También puedo calcular la cantidad de ingresos diferentes si al select resultado lo agrupo por ID_CAMBIO_ASIGNACION_EXP */