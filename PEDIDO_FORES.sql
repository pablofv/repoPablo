/*
Programa.....: PEDIDO_FORES.SQL
Entorno......: MCENTRA
Localizaci�n.: Proyecto Litigiosidad\LEX100\GitLex100\Algoritmos Estadisticas\SQL Definitivo
Programa Base: Proyecto Litigiosidad\LEX100\GitLex100\Algoritmos Estadisticas\SQL Base\INGRESOS.SQL



Descripci�n.: Calcula los expedientes ingresados en el fuero Criminal y Correccional Federal y 
              Criminal y Correccional de la Capital Federal en base a los delitos solicitados por el FORES.

Aclaraci�n: como una causa puede tener mas de un delito, se informa cantidad de ingresos por delito y cantidad de ingresos por causa
*/

/*
  PASO 1�
  Se ejecuta el programa base filtrando por las C�maras que tramitan los delitos solicitados (de la tabla OFICINA el campo ID_CAMARA con valor 8 o 9)


Descripci�n.: Cuenta los Expedientes ingresados desde una vista (Sub Consulta), tomando la tabla CAMBIO_ASIGNACION_EXP, 
              buscando entre dos fechas dadas la primer asignaci�n de causas de cada juzgado de cada fuero o Jurisdicci�n.
              Crea la tabla EST_PEDIDO_FORES.

Joins.......: Une la tabla CAMBIO_ASIGNACION_EXP con la tabla Expediente por igual ID_EXPEDIENTE filtrando por aquellos donde
              en el campo NATURALEZA_EXPEDIENTE sean igual a 'P' (solo los expedientes principales)
              Une la tabla CAMBIO_ASIGNACION_EXP con la tabla OFICINA por igual ID_OFICINA
              Une la tabla OFICINA con sigo misma en caso que el campo ID_OFICINA_SUPERIOR sea NULO toma ID_OFICINA sino toma ID_OFICINA_SUPERIOR
              Esto �ltimo lo hacemos en caso de que en el campo ID_OFICINA tengamos una Secretar�a.


Filtros.....: Dentro de la subconsulta, seg�n se requiere para este pedido: 
              status = 0 para la tabla CAMBIO_ASIGNACION_EXP
              ID_TIPO_INSTANCIA = 1 para la tabla OFICINA -- Atenci�n, esta condici�n no es suficiente para tomar solo los Juzgados y sus secretar�as
              ID_CAMARA  = 8 O (ID_CAMARA = 9 Y SIGLA_CEDULAS contenga ('CI', 'CR','JNM', 'RO'))) -- Que incluya si es C�mara 9 (Criminal y Correccional) y 
              que el  campo sigla_cedula contenga los c�digos de Instrucci�n (CI), Correccional (CR), Menores (JNM) y Rogatorias (RO), esto �ltimo
              es equivalente y m�s r�pido que filtrarlo por descripci�n para considerar solo los Juzgados.
              ID_TIPO_OFICINA in (1,2)
*/


create table est_pedido_fores as
SELECT *
from (select ROW_NUMBER() over(partition by c.ID_EXPEDIENTE,o2.ID_OFICINA order by FECHA_ASIGNACION ) rn,
             C.ID_CAMBIO_ASIGNACION_EXP,
             C.ID_EXPEDIENTE,
             C.FECHA_ASIGNACION,
             o.ID_CAMARA
      from CAMBIO_ASIGNACION_EXP c
           JOIN EXPEDIENTE e on e.status = 0 and e.ID_EXPEDIENTE = c.ID_EXPEDIENTE and e.NATURALEZA_EXPEDIENTE in ('P')
           JOIN OFICINA o on c.ID_OFICINA = o.ID_OFICINA
           JOIN OFICINA o2 on o2.ID_OFICINA = (CASE WHEN o.ID_OFICINA_SUPERIOR is NULL then o.ID_OFICINA else o.ID_OFICINA_SUPERIOR END)
      where c.status = 0
      and o.ID_TIPO_INSTANCIA = 1
      and (o.ID_CAMARA  = 8 OR (o.ID_CAMARA = 9 and o2.SIGLA_CEDULAS in ('CI', 'CR','JNM', 'RO')))  
           -- Se utiliza el campo sigla_cedulas por ser equivalente a filtrar por descripcion like'%JUZGADO%' and descripci�n like'%CORRECCIONAL%' ETC.
      and o.ID_TIPO_OFICINA in (1,2) -- Se utiliza para considerar que en la C�mara Criminal y Correcional Federal solo tome los Juzgados y Secretarias
     )
where rn = 1
  and trunc(FECHA_ASIGNACION) between to_date('20150101', 'YYYYMMDD') and to_date('20151231', 'YYYYMMDD')
;

/* PASO 2 -- CANTIDAD DE INGRESOS POR DELITO POR C�MARA
   De la tabla EST_PEDIDOS_FORES se calculan las cantidades de ingresos discriminando por los delitos descriptos en lo solicitado por FORES

   Joins..: Une la tabla EST_PEDIDO_FORES con la tabla  DELITO_EXPEDIENTE por el campo ID_EXPEDIENTE
            Une la tabla DELITO con la tabla DELITO_EXPEDIENTE por el campo ID_DELITO
            une la tabla CAMARA con la tabla EST_PEDIDO_FORES por el campo ID_CAMARA
            
   Filtros: Que el campo ARTICULO contenga ((256, 257, 258, 259, 260, 261, 265, 266, 267, 268) 
                                            o ((campo articulo contenga 173 y campo inciso contenga 7) 
                                                 o (campo articulo contenga 174 and campo inciso contenga 5)))
                                                 
   El resultado en MCENTRA es 1109 registros 
   Agrupado por: Art�culo del C�digo Penal, Apertura del art�culo, Inciso, Descripci�n del Delito, C�mara
   Ordenado por: C�mara, Art�culo del C�digo Penal, Apertura del art�culo, Inciso, Descripci�n del Delito
   
*/


select CA.DESCRIPCION, d.articulo, d.apertura_articulo, d.inciso, d.DESCRIPCION_DELITO, count(*) 
from EST_PEDIDO_FORES p JOIN DELITO_EXPEDIENTE de on p.ID_EXPEDIENTE = de.ID_EXPEDIENTE
                        join delito d on de.ID_DELITO = d.ID_DELITO
                        join camara ca on P.ID_CAMARA = CA.ID_CAMARA
where d.articulo in (256, 257, 258, 259, 260, 261, 265, 266, 267, 268)
  or (articulo in (173) and inciso in (7))
  or (articulo in (174) and inciso in (5))
group by d.articulo, d.apertura_articulo, d.inciso, d.DESCRIPCION_DELITO, CA.DESCRIPCION
order by CA.DESCRIPCION, d.articulo, d.apertura_articulo nulls first, d.inciso, d.DESCRIPCION_DELITO
;

/* PASO 3 -- CANTIDAD DE INGRESOS POR CAUSA POR C�MARA
   De la tabla EST_PEDIDOS_FORES se calculan las cantidades de ingresos agrupando por causa y por c�mara  
   esto se logra con una consulta anidada (de adentro hasia afuera).

   Primer consulta devuelve: Descripci�n de C�mara, Identificaci�n de Asignaci�n y la cantidad
   
   Joins..: Une la tabla EST_PEDIDO_FORES con la tabla  DELITO_EXPEDIENTE por el campo ID_EXPEDIENTE
            Une la tabla DELITO con la tabla DELITO_EXPEDIENTE por el campo ID_DELITO
            une la tabla CAMARA con la tabla EST_PEDIDO_FORES por el campo ID_CAMARA
   
   Filtros: Que el campo ARTICULO contenga ((256, 257, 258, 259, 260, 261, 265, 266, 267, 268) 
                                            o ((campo articulo contenga 173 y campo inciso contenga 7) 
                                                 o (campo articulo contenga 174 and campo inciso contenga 5)))
   Se agrupa y se ordena por la Descripci�n de C�mara e Identificaci�n de Asignaci�n
  
  Segunda consulta devuelve: Descripci�n de C�mara, cantidad de delitos por ingreso (primer consulta) y cantidad de ingresos por cantidad de delitos
  Se agrupa y se ordena por la Descripci�n y cantidad de delitos por ingreso
  
   El resultado en MCENTRA 938 ingresos; 335 de federal y 603 penal ordinario
*/


select DESCRIPCION, cantidad, count(*)
from (select CA.DESCRIPCION, p.ID_CAMBIO_ASIGNACION_EXP, count(*) cantidad 
      from EST_PEDIDO_FORES p JOIN DELITO_EXPEDIENTE de on p.ID_EXPEDIENTE = de.ID_EXPEDIENTE
                              join delito d on de.ID_DELITO = d.ID_DELITO
                              join camara ca on P.ID_CAMARA = ca.ID_CAMARA
      where d.articulo in (256, 257, 258, 259, 260, 261, 265, 266, 267, 268)
        or (articulo in (173) and inciso in (7))
        or (articulo in (174) and inciso in (5))
      group by CA.DESCRIPCION, p.ID_CAMBIO_ASIGNACION_EXP
      order by CA.DESCRIPCION, p.ID_CAMBIO_ASIGNACION_EXP) t_ingresosDiferentes
group by DESCRIPCION, cantidad
order by DESCRIPCION, cantidad
;