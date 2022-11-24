
-- CSR detal all:
select stat_day, stat.reg, district, town, band,
       bsc_rnc, altername, bcf_nodeb, cell,
      -- далее показатель, заполняемый в случае, если сота проблемная и ей необходимо заниматься в первую очередь
       case when problem_cell is not null then 'excluded' -- проблема на соте в процессе решения
            when stretching - remains_CSR_previous_cell <= 0 and stretching is not null 
            then null -- сота не требует вмешательства
            when threshould - remains_CSR_previous_cell > 0 and threshould is not null  
                 and rating < count_reg_cell*9/10 -- сота не в конце рейтинга хужести
            then 'below_threshould' -- сота вместе с вышерасположенными сотами просаживает показатели ниже пороговых
            when Target - remains_CSR_previous_cell > 0 and Target is not null  
                 and rating < count_reg_cell*9/10 -- сота не в конце рейтинга хужести
            then 'below_target' -- сота вместе с вышерасположенными сотами просаживает показатели ниже целевых
            when stretching - remains_CSR_previous_cell > 0 and stretching is not null  
                 and rating < count_reg_cell*9/10 -- сота не в конце рейтинга хужести
            then 'below_stretching' -- сота вместе с вышерасположенными сотами просаживает показатели ниже выдающихся
            else null -- сота не требует вмешательства
       end as dissatisfaction,  
       rating,
       CSR_CELL,
--------------
       CSR_REG,
       threshould,Target, stretching, -- порог, цель, выдающийся результат
       threshould - CSR_REG as delta_threshould, -- дельта между пороговым и текущим результатом ключевого показателя
       Target - CSR_REG as delta_Target, -- дельта между целевым и текущим результатом ключевого показателя
       stretching - CSR_REG as delta_stretching, -- дельта между выдающимся и текущим результатом ключевого показателя
       count_reg_cell, -- кол-во сот в регионе
       CSR_without_this_cell, -- CSR построенный без текущей соты
       remains_CSR_previous_cell, -- CSR построенный без предшествующих по рейтингу сот (текущая сота включена в расчет CSR)
       remains_CSR_previous_cell - CSR_REG as DELTA_REMAINS_CSR
from
(/*!!*/
select problem_cell,
       stat_day, reg, district, town, band
      ,bsc_rnc,altername, bcf_nodeb, cell
      ,count_reg_cell
      ,rating
      ,1- case when nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0) != 0
              then (nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0))
                /(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0))
              else null
       end as CSR_CELL 
      ,CSR_REG
      ,CSR_without_this_cell
--------------------------------------------------------------
-- CSR построенный без предшествующих по рейтингу сот (текущая сота включена в расчет CSR)
-- расчитываем KPI на всех сотах у которых ранг влияния на ухудшение показателя выше.
-- если у данной соты ранг = 5 то из расчета выкидываются данные сот с рангом 1-4    
-- если проблема на соте в процессе решения (problem_cell is not null), то ее не учитываем -
-- это сделано для того, чтобы в финальном отчете не учитывать соты 
-- на которых проблема не решаема в ближайшее время
      (1-(CASE WHEN
            (nvl(denum_dcr_2g_reg,0) + nvl(denum_dcr_3g_reg,0) + nvl(denum_dcr_LTE_reg,0)
            -- если не проблемная сота, то в строке ниже используем значение, иначе 0 
            + decode(problem_cell,null,nvl(denum_dcr_2g_reg,0) + nvl(denum_dcr_3g_reg,0) + nvl(denum_dcr_LTE_reg,0),0) 
            -- вычитаем сумму значений, если не проблемная сота
            - SUM(decode(problem_cell,null,nvl(denum_dcr_2g_reg,0) + nvl(denum_dcr_3g_reg,0) + nvl(denum_dcr_LTE_reg,0),0)) 
            OVER (PARTITION BY stat_day, reg ORDER BY rating 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) !=0
                THEN 
            (nvl(num_dcr_2g_reg,0) + nvl(num_dcr_3g_reg,0) + nvl(num_dcr_LTE_reg,0) 
            + decode(problem_cell,null,nvl(num_dcr_2g_reg,0) + nvl(num_dcr_3g_reg,0) + nvl(num_dcr_LTE_reg,0),0) 
            - SUM(decode(problem_cell,null,nvl(num_dcr_2g_reg,0) + nvl(num_dcr_3g_reg,0) + nvl(num_dcr_LTE_reg,0),0)) 
            OVER (PARTITION BY stat_day, reg ORDER BY rating 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
            /
            (nvl(denum_dcr_2g_reg,0) + nvl(denum_dcr_3g_reg,0) + nvl(denum_dcr_LTE_reg,0)
            -- если не проблемная сота, то в строке ниже используем значение, иначе 0
            + decode(problem_cell,null,nvl(denum_dcr_2g_reg,0) + nvl(denum_dcr_3g_reg,0) + nvl(denum_dcr_LTE_reg,0),0) 
            -- вычитаем сумму значений, если не проблемная сота
            - SUM(decode(problem_cell,null,nvl(denum_dcr_2g_reg,0) + nvl(denum_dcr_3g_reg,0) + nvl(denum_dcr_LTE_reg,0),0)) 
            OVER (PARTITION BY stat_day, reg ORDER BY rating 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) 
                ELSE 0
          end))/*(1-DCR)*/
 as remains_CSI_previous_cell
--------------------------------------------------------------
from
---------------------------------------------------------------------------
(select s.* , -- упорядочено по убыванию отрицателного влияния на CSR региона
     p.cell as problem_cell, -- проблемная сота, у которой еще не вышел срок решения проблемы
     count(reg) over (partition by stat_day, reg) as count_reg_cell,
     -- Далее каждой соте присваивается ранг.
     -- Используется расчет регионального показателя без этой соты.
     -- Чем выше CSR без этой соты, тем больше эта сота ухудшает показатель в регионе
     -- Первое место у той соты, где KPI без нее будет наивысшим.
     row_number() over
/*-*/               (partition by stat_day, reg 
                         order by stat_day, reg, CSR_WITHOUT_THIS_CELL 
/*-*/               desc)
      as rating 

from
(/*!*/ -- данные по разным технологиям, собранные линейно 
    --  + дополнительные поля уровня региона, будут использоваться для вычислений выше
    select s.* ,-- все исходные поля 
    -- + дополнительно посчитанные поля с агрегацией до региона. Понадобятся для дальнейших вычислений
    -- суммарные данные по дате и региону к которому относится сота и так для каждой соты
    sum(nvl(num_dcr_2g,0)) over (partition by stat_day, reg) as num_dcr_2g_reg, 
    sum(nvl(denum_dcr_2g,0)) over (partition by stat_day, reg) as denum_dcr_2g_reg,
    sum(nvl(num_dcr_2g,0)) over (partition by stat_day, reg) as num_dcr_2g_reg,
    sum(nvl(denum_dcr_2g,0)) over (partition by stat_day, reg) as denum_dcr_2g_reg,
    sum(nvl(num_dcr_LTE,0)) over (partition by stat_day, reg) as num_dcr_LTE_reg,
    sum(nvl(denum_dcr_LTE,0)) over (partition by stat_day, reg) as denum_dcr_LTE_reg,
    /* некоторый показатель, как пример построения комбинированного KPI на основании данных из разных источников 
       в данном запросе происходит расчет за выбранную дату по всему региону к которому относится сота по формуле: 
       сумма числителей всех сот региона деленная на сумму всех знаменателей */
    1-case when (sum(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0)) over (partition by stat_day, reg))  != 0 
            then (sum(nvl(num_dcr_2g,0)+nvl(num_dcr_3g,0)+nvl(num_dcr_LTE,0)) over (partition by stat_day, reg))
                /(sum(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0)) over (partition by stat_day, reg))
            else 0   
    end as CSR_REG,
    /* Следующий показатель аналогичен предыдущему только из региональных числителя и знаменателя вычитаются данные по соте
       в результате получаем показатель рассчитанный на всех сотах региона без этой конкретной соты */
    1-case when (sum(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0)) over (partition by stat_day, reg)
                -(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0)))  != 0 
            then (sum(nvl(num_dcr_2g,0)+nvl(num_dcr_3g,0)+nvl(num_dcr_LTE,0)) over (partition by stat_day, reg)
                -(nvl(num_dcr_2g,0)+nvl(num_dcr_3g,0)+nvl(num_dcr_LTE,0)))
                /(sum(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0)) over (partition by stat_day, reg)
                -(nvl(denum_dcr_2g,0)+nvl(denum_dcr_3g,0)+nvl(denum_dcr_LTE,0)))
            else 0   
    end as CSR_WITHOUT_THIS_CELL
from
    ( -- технологии 2G,3G,4G собранны линейно для возможности вычисления комбинированного показателя!!!
    select stat_day, reg, district, town, 'g' as band
        ,bsc as bsc_rnc, bcf as bcf_nodeb, cell,altername
        ,nvl(num_dcr_2g,0) num_dcr_2g, nvl(denum_dcr_2g,0) denum_dcr_2g
        ,null as num_dcr_3g, null as denum_dcr_3g
        ,null as num_dcr_LTE,null as denum_dcr_LTE
    from GSM_data where stat_day = to_date(&day_from, 'DD.MM.YYYY')
    ---------   
    union all
    ---------   
    select stat_day, reg, district, town, 'u' as band
        ,rnc, nodeb, cell,altername
        ,null as num_dcr, null as denum_dcr
        ,nvl(num_cdr_3g,0) num_cdr_3g, nvl(denum_cdr_3g,0) denum_cdr_3g
        ,null as num_cdr_LTE,null as denum_cdr_LTE
    from UMTS_data where stat_day = to_date(&day_from, 'DD.MM.YYYY')
    ---------   
    union all
    ---------   
    select stat_day, reg, district, town, 'l' as band
        ,null as bsc_rnc, enodeb, cell,altername
        ,null as num_dcr, null as denum_dcr
        ,null as num_cdr_3g, null as denum_cdr_3g
        ,nvl(num_cdr_LTE,0) num_cdr_ps_LTE, nvl(denum_cdr_LTE,0) denum_cdr_LTE
    from LTE_data where stat_day = to_date(&day_from, 'DD.MM.YYYY')
    ) s 
/*!*/)s
---------------------------------------------------------------------------
left join
---------------------------------------------------------------------------
(-- проблемные соты, у которых еще не вышел срок решения проблемы
    select CELL from CSR_PROBLEM_SOLUTION
    where actual in (1,3) and DATE_EXPECTED_SOLUTION > sysdate - 1
) p
---------------------------------------------------------------------------
on s.cell = p.CELL
---------------------------------------------------------------------------
) 
/*!!*/) stat
left join
(-- пороговые и целевые значения
    select REGION as REG, threshould, Target, stretching 
    from CSR_TARGETS 
) csr_t
on stat.REG = csr_t.REG
;