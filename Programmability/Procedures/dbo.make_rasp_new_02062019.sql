SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================  
-- Author:  <Author,,Name>  
-- Create date: <Create Date,,>  
-- Description: <Description,,>  
--2018-04-17 OD Отправка смс на склад перенесена в конец процедуры  
--2019-03-21 OD Обращения к DT переключены на 06 сервер пока. В будущем вернем на 01  
--select * from jobs..Jobs where job_name like '%make_rasp_new%' order by date_add desc  
-- =============================================  
  
/*  
  
EXEC M2.[dbo].[make_rasp_new] 7777, 53980  
  
*/  
  
create PROCEDURE [dbo].[make_rasp_new_02062019]  
  @id_job int,  
  @N      int   
AS  
BEGIN   
 SET NOCOUNT ON  
  
  
-- при ручном распределении нужно обратно вернуть  
  
  
  
--declare @N as int =9545  , @id_job as int =683332121  
  
/**  
  delete from r  
  from m2..archive_rasp  r  
  where r.number_r =  @N   
    
  delete from r  
  from m2..archive_tt_tov_kontr  r  
  where r.number_r in (  @N , - @N )    
**/    
    
--и не забыть после распределения почистить  
  
  /**  
declare @N as int = 9545  
    
delete r  
--output deleted.*  into M2..archive_rasp  
--select *  
from M2..rasp r  
where r.number_r = @N  
  
delete r  
--output deleted.*  into M2..archive_tt_tov_kontr  
--select *  
from M2..tt_tov_kontr r  
where r.number_r in  (@N,-@N)  
  
  **/  
    
    
  
  
  
  DECLARE @nvaSQL   nvarchar(4000),   
          @email    nvarchar(100),  
          @email_t  char(38),   
          @desc     char(200),  
          @getdate  datetime = GETDATE(),  
        @err_mes  varchar(max),  
        @Job_name varchar(100) ='m2..make_rasp_new',  
        @err_str  nvarchar(max),  
        @err      int   
  
---------------ИНИЦИАЛИЗАЦИЯ ПАРАМЕТРОВ РАСПРЕДЕЛЕНИЯ--------  
 declare @date_rasp as date    
  , @id_sklad as int   
  , @test_raspr as tinyint  
  , @result_table as varchar(100)  
  , @q int = 0  
  , @id_zone int  
  , @date_add datetime  
  , @dont_use_wait_sklad int  
  , @sql_raspr int  
  , @only_zakaz int  
  , @hour_raspr int  
 -- взять параметры распределения  
 select @date_rasp=Date_r   
  , @id_sklad=id_sklad  
  , @test_raspr =ISNULL(test_raspr ,0)  
  , @result_table =result_table  
  , @id_zone = id_zone   
  , @date_add = time_add  
  , @sql_raspr =isnull(sql_raspr,0)  
  , @only_zakaz = isnull([only_zakaz],0)  
  , @hour_raspr = DATEPART(HOUR,time_add)  
 from M2..Raspr_zadanie   where Number_r=@N  
  
 set @result_table =case when ISNULL(@result_table ,'')=''   
  then '[IzbenkaFin].dbo.[_InfoRg4020]'  
  else @result_table end  
  
  
---------------------------  
  
begin try  
  
update r  
set id_job = @id_job  
from M2..Raspr_zadanie r  
where Number_r =@N  
  
  
-- АК - новый кусок - запуск подготовки распределения вручную  определяем по наличию записей в подготовительных таблицах  
if not exists (  
select *  
from M2..tov_init  
where Number_r =@N)  
begin  
-- значит обычное распределение  - нужно создать таблицы (tov_init , tov_kontr_init и тп)  
  
 if @test_raspr = 1 -- для распределения из тестовой базы данные готовятся другой хранимкой(данные из srv-sql05.fin_test_full)  
  begin  
   exec a_kor.dbo.[prepare_test_raspred_1C] @N , @id_job  
  end  
 else  
  begin  
   exec a_kor.dbo.[prepare_raspred_1C] @N , @id_job  
  end  
end  
else -- значит отладочный запуск и все таблицы уже были подготовлены  
 begin  
 delete a_kor..tt_tov_kontr  
 from a_kor..tt_tov_kontr with (rowlock, index (ind1))  
 where Number_r in (@N , -@N )  
   
 insert into [a_kor].[dbo].[tt_tov_kontr]  
   ([Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_v]  
      ,[q_plan_pr]  
      ,[q_min_ost]  
      ,[q_FO]  
      ,[cena_pr]  
      ,[koef_tt]  
      ,[min_ost_tt_tov]  
      ,[date_r1]  
      ,[date_r2]  
      ,[k1]  
      ,[k2]  
      ,[k3]  
      ,[k4]  
      ,[k5]  
      ,[id_kontr_init]  
      ,[date_add_tt_tov_kontr]  
      ,[max_ost_tt_tov]  
      ,[q_rashod_fact]  
      ,[id_zal]  
      ,q_zakaz  
      ,tt_format_rasp  
      ,price_rasp  
      ,koef_ost_pr_rasp  
      ,id_tov_pvz_rasp)  
        
 SELECT   
    [Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_v]  
      ,[q_plan_pr]  
      ,[q_min_ost]  
      ,[q_FO]  
      ,[cena_pr]  
      ,[koef_tt]  
      ,[min_ost_tt_tov]  
      ,[date_r1]  
      ,[date_r2]  
      ,[k1]  
      ,[k2]  
      ,[k3]  
      ,[k4]  
      ,[k5]  
      ,[id_kontr_init]  
      ,[date_add_tt_tov_kontr]  
      ,[max_ost_tt_tov]  
      ,[q_rashod_fact]  
      ,[id_zal]  
      ,q_zakaz  
      ,tt_format_rasp  
      ,price_rasp  
      ,koef_ost_pr_rasp  
      ,id_tov_pvz_rasp  
  FROM [a_kor].[dbo].[tt_tov_kontr_init]  
  where [Number_r]= @N  
  
     
   
end  
  
  
  
 select  @dont_use_wait_sklad = Raspr_zadanie.dont_use_wait_sklad  
 from M2..Raspr_zadanie   where Number_r=@N  
   
   
------------------------------------------------  
  
select @getdate = getdate()   
  
/**  
-- наличие вендинга  
if OBJECT_ID('tempdb..#MCK') is not null drop table #MCK  
create table #MCK (id_tt_mck int)  
  
--declare @N int = 76829  
--delete from #MCK  
insert into #MCK  
select distinct r.id_tt   
from M2..tt_tov_kontr_init r    
--inner join m2..tt on tt.id_TT = r.id_tt   
where r.tt_format_rasp=7 and r.number_r = @N  
and r.q_plan_pr>0  
**/  
  
  
create table #q_zak (id_TT int, id_tov int, id_kontr int, BonusCard char(7) ,q real , ThisManufacturer int)  
create table #q_zak_made (number_r int, id_TT int, id_tov int, id_kontr int, BonusCard char(7)  , ThisManufacturer int)  
  
  
 WHILE 1 = 1  
    BEGIN  
      BEGIN try    
      
  delete from m2..Raspr_tt_waiting  
  where number_r = @N  
  
  --if not exists (select * from M2..tov where Number_r = @n)  
  delete M2..tov  
  where Number_r = @n  
  
  --if not exists (select * from M2..tov_kontr where Number_r = @n)  
  delete M2..tov_kontr   
  where Number_r = @n   
     
  --if not exists (select * from M2..tov_kontr_date where Number_r = @n)  
  delete M2..tov_kontr_date   
  where Number_r = @n   
  
  --if not exists (select * from M2..tov_kontr_zal where Number_r = @n)  
  delete M2..tov_kontr_zal   
  where Number_r = @n           
  
     
  delete M2..rasp_zakaz_pok  
  where Number_r = @n       
     
  
  
     
     
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 11, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 11 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
      RETURN  
   end  
  
   END CATCH   
 END--while  
     
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 111111, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 WHILE 1 = 1  
    BEGIN  
      BEGIN try    
     
  insert into M2..tov  
  Select *  
  from M2..tov_init t  
  where t.Number_r= @N  
   
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 1111112, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 1111113 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while  
        
 WHILE 1 = 1  
    BEGIN  
      BEGIN try    
  
  insert into M2..tov_kontr  
  Select *  
  from M2..tov_kontr_init t  
  where t.Number_r= @N  
   
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 1111114, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 1111115 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )  
   RETURN         
   end  
  
   END CATCH   
   END--while     
  
  
/**  
-- новый алгоритм для Спб. - если в распределении только Спб, то поставить всем товарам rasp_all=0 и skladir = 1  
-- нет ни одного магазина не из Спб  
if not exists(  
select r.*  
from M2..raspr_zadanie_tt r    
  inner join M2..tt on tt.id_TT = r.id_tt and tt.adress not like ('%санкт%')  
where r.number_r= @N)  
 begin  
   
 update t  
 set rasp_all=0 , q_wait_sklad=0  
 from m2..tov_kontr t  
 where t.Number_r=@n  
   
 update t  
 set skladir=1  
 from m2..tov t  
 where t.Number_r=@n  
    
      
 end   
 **/   
  
  
  
 WHILE 1 = 1  
    BEGIN  
      BEGIN try    
 -- убрать равные взаимозаменяемые товары из поля id_tov_vz  
  update m2..tov   
  set id_tov_vz = NULL  
  --select *  
  from  m2..tov with (  index (IX_tov_1))   
  left join   
     (select distinct a.id_tov_vz  
      from(  
       select tov.id_tov_vz  
       from m2..tov with (  index (IX_tov_1))   
       where tov.Number_r=@N    and isnull(tov.id_tov_vz,0)<>0  
       and tov.id_tov_vz <> tov.id_tov  
  
       union all  
  
       select tov.id_tov  
       from m2..tov with (  index (IX_tov_1))   
       where tov.Number_r=@N    and isnull(tov.id_tov_vz,0)<>0  
       and tov.id_tov_vz <> tov.id_tov    
        
      ) a  
      ) b on b.id_tov_vz = tov.id_tov_vz  
   where tov.Number_r=@N and b.id_tov_vz is null  
     
   update M2..tov_kontr   
   set q_ost_sklad = master.dbo.maxz(tk.q_ost_sklad,0) , q_wait_sklad = master.dbo.maxz(tk.q_wait_sklad,0)  
   from M2..tov_kontr (rowlock) tk  
   where tk.Number_r = @N  
   --//+++АК SHEP 2018.10.26  
   AND tk.Kolvo_korob != 0  
   --//---АК SHEP 2018.10.26  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 112, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 112 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while     
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 10020, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
   
    While 1=1  
    BEGIN  
    BEGIN TRY  
  
  insert into M2..tov_kontr_date  
  Select *  
  from M2..tov_kontr_date_init t  
  where t.Number_r= @N  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 10021, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 10021 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while     
    
    While 1=1  
    BEGIN  
    BEGIN TRY  
  
  insert into M2..tov_kontr_zal  
  Select *  
  from M2..tov_kontr_zal_init t  
  where t.Number_r= @N  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 10022, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 10022 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN       
           
   end  
  
   END CATCH   
   END--while     
  
   
 /**  
 select rh.number_r , t.Name_tov , SUM(rh.znach) , COUNT(*)  
 from M2..raspr_hystory rh  
 inner join M2..Raspr_zadanie r on r.Number_r = rh.number_r  
 inner join M2..Tovari t on t.id_tov = rh.id_tov  
 where r.Date_r = {d'2015-05-20'} and rh.znach<>0 and rh.rn_r=4  
 group by rh.number_r , t.Name_tov  
 order by rh.number_r , t.Name_tov  
  
  
 **/  
  
 -- поправить tov_kontr_date - убрать пустые даты и привести к нормальным годам  
     
    While 1=1  
    BEGIN  
    BEGIN TRY  
  -- все верные варианты  
  select case when td.date_ost < {d'2010-01-01'} then @date_rasp   
     when td.date_ost < {d'3016-01-01'} then td.date_ost  
     else DATEADD(year,-2000,td.date_ost) end date_ost  
   , td.id_tov   
   , td.id_kontr   
   , sum(td.q_ost_sklad) q_ost_sklad  
  into #tov_kontr_date  
  from M2..tov_kontr_date td with (rowlock)  
  where Number_r = @N  
  group by case when td.date_ost < {d'2010-01-01'} then @date_rasp   
       when td.date_ost < {d'3016-01-01'} then td.date_ost  
       else DATEADD(year,-2000,td.date_ost) end  
    , td.id_tov , td.id_kontr  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 113, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 113 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while     
   
  
insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par3)   
select @id_job , 10030, DATEDIFF(MILLISECOND , @getdate ,GETDATE()),@test_raspr,SUBSTRING(@result_table,1,50)   
 select @getdate = getdate()    
   
    
    While 1=1  
    BEGIN  
    BEGIN TRY  
      
  --select *  
  -- удалить те, что не нужны  
  delete from m2..tov_kontr_date  
  from M2..tov_kontr_date td with (rowlock)  
  left join #tov_kontr_date tdk on tdk.id_tov = td.id_tov and tdk.id_kontr = td.id_kontr  
  and tdk.date_ost = td.date_ost  
  where td.Number_r = @N and tdk.id_tov is null  
  
  -- обновить колво по суммируемым  
  update m2..tov_kontr_date  
  set q_ost_sklad = tdk.q_ost_sklad  
  from M2..tov_kontr_date td with (rowlock)  
  inner join #tov_kontr_date tdk on tdk.id_tov = td.id_tov and tdk.id_kontr = td.id_kontr  
  and tdk.date_ost = td.date_ost  
  where td.Number_r = @N and tdk.q_ost_sklad <> td.q_ost_sklad      
  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 10031, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 10031 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while  
  
    While 1=1  
    BEGIN  
    BEGIN TRY  
  
  -- добавить новые  
  insert into m2..tov_kontr_date (Number_r , id_tov , id_kontr , q_ost_sklad , date_ost)  
  select @N , tdk.id_tov , tdk.id_kontr , tdk.q_ost_sklad , tdk.date_ost  
  from #tov_kontr_date tdk   
  left join M2..tov_kontr_date td with (rowlock) on td.Number_r = @N and tdk.id_tov = td.id_tov and tdk.id_kontr = td.id_kontr  
  and tdk.date_ost = td.date_ost  
  where td.Number_r is null  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 10032, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 10032 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )    
  
   RETURN  
  
   end  
  
   END CATCH   
   END--while  
  
  
  
  
 -- drop table #tov_kontr_date  
  
 -- поправить tov_kontr_date на схлопывание отрицательных остатков по датам по FIFO с положительными  
 create table #isp  
 (id_tov int , id_kontr int, d1 date, d2 date, q real)  
  
  
    While 1=1  
    BEGIN  
    BEGIN TRY  
  
  insert into #isp  
  select a.id_tov , a.id_kontr , a.d1 , a.d2 , q  
  from   
  (  
  select ttk1.id_tov , ttk1.id_kontr , ttk1.date_ost d1, ttk2.date_ost d2,  
  rn , rn2   
    
  , master.dbo.minz(abs(ttk1.q_ost_sklad) , ttk2.q_ost_sklad) q   
  from   
    
  ( select * ,  
  ROW_NUMBER() over ( partition by ttk1.id_tov , ttk1.id_kontr order by ttk1.date_ost) rn2  
   from M2..tov_kontr_date ttk1 with ( index (ind1))   
   where ttk1.number_r = @N and ttk1.q_ost_sklad < -0.0001 ) ttk1  
  inner join   
  (select * ,  
  ROW_NUMBER() over ( partition by ttk2.id_tov , ttk2.id_kontr order by ttk2.date_ost desc) rn  
  from M2..tov_kontr_date ttk2 with ( index (ind1))   
  where ttk2.number_r = @N and ttk2.q_ost_sklad>0.0001) ttk2 on ttk1.id_tov = ttk2.id_tov  
  and ttk1.id_kontr = ttk2.id_kontr   
       
  ) a   
  where a.rn2 =1 and a.rn=1  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 10033, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 10033 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )  
   RETURN         
   end  
  
   END CATCH   
   END--while  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 while exists (select * from #isp)  
 begin  
  
 update M2..tov_kontr_date with (rowlock)  
 set q_ost_sklad = case when tkd.date_ost = isp.d1 then tkd.q_ost_sklad + q else tkd.q_ost_sklad - q end  
 from M2..tov_kontr_date tkd with (index (ind1))  
 inner join #isp isp on tkd.id_tov = isp.id_tov  
 and tkd.id_kontr = isp.id_kontr and tkd.date_ost in (isp.d1 , isp.d2)  
 where tkd.Number_r = @N  
  
 delete from #isp  
  
 insert into #isp  
 select a.id_tov , a.id_kontr , a.d1 , a.d2 , q  
 from   
 (select ttk1.id_tov , ttk1.id_kontr , ttk1.date_ost d1, ttk2.date_ost d2,  
 ROW_NUMBER() over ( partition by ttk1.id_tov , ttk1.id_kontr order by ttk2.date_ost desc) rn  
 , ROW_NUMBER() over ( partition by ttk1.id_tov , ttk1.id_kontr order by ttk1.date_ost) rn2  
 , master.dbo.minz(abs(ttk1.q_ost_sklad) , ttk2.q_ost_sklad) q  
 from M2..tov_kontr_date ttk1 with ( index (ind1))   
 inner join M2..tov_kontr_date ttk2 with ( index (ind1)) on ttk1.id_tov = ttk2.id_tov  
 and ttk1.id_kontr = ttk2.id_kontr and ttk2.q_ost_sklad>0.0001 and ttk2.Number_r = @N  
  
 where ttk1.number_r = @N   
 and ttk1.q_ost_sklad < -0.0001 ) a   
 where a.rn2 =1 and a.rn=1  
  
 end  
  
     update M2..tov_kontr_date  
     set q_ost_sklad=0  
     from M2..tov_kontr_date ttk1 with ( index (ind1))   
     where ttk1.number_r = @N and ttk1.q_ost_sklad<0  
      
   
 -- drop table #isp  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 2, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- находим по всем тт и товарам рейтинг продаж по дням с учетом коэф дня распределения  
 /**  
 create table #dn (id_tt int , dn int , ПроцДн int)  
  
 insert into #dn  
 SELECT TT._Fld758 id_tt, dates.dn,  
 case dates.dn when 1 then _Fld2899 when 2 Then _Fld2900 when 3 Then _Fld2901   
 when 4 Then _Fld2902 when 5 Then _Fld2903 when 6 Then _Fld2904 when 7 Then _Fld2905 end ПроцДн  
 FROM [IzbenkaFin].[dbo].[_InfoRg2895] as Raspr    
  INNER JOIN IzbenkaFin.dbo._Reference42 as TT    
  ON Raspr._Fld2897RRef = TT._IDRRef  
 inner join  
 (select top 7 ROW_NUMBER() over (order by date_add) dn  
  from jobs..Jobs_log    
  ) dates on 1=1   
 where _Fld2898_RRRef = 0x00000000000000000000000000000000  
    **/  
  
 -- если не первый расчет, то вернуть обратно id_tov, которые могли быть сменены в предыдущий расчет  
 update M2..tov_kontr  
 set id_tov = id_tov_init  
 where Number_r = @N and id_tov <> id_tov_init  
  
 update M2..tov_kontr_date  
 set id_tov = id_tov_init_tkd  
 where Number_r = @N and id_tov <> id_tov_init_tkd  
  
 update M2..tov_kontr_zal  
 set id_tov = id_tov_init_zal  
 where Number_r = @N and id_tov <> id_tov_init_zal  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200001, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
/*  
if @new_alg =0  
begin  
  
  
 delete M2..tt_tov_kontr  
 from M2..tt_tov_kontr with (rowlock, index (ind1))  
 where Number_r in (@N , -@N )  
  
  
   
    if OBJECT_ID('tempdb..#Raspr_tt_tov') is not null drop table #Raspr_tt_tov  
  
  
  
   
 --select 1  
 --declare @date_rasp date = {d'2018-09-07'} , @N int = 61415  
 select  
  Dannye._Fld5948 as Number_r,  
  Dannye._Fld5949 as id_TT,  
  Dannye._Fld5950 as id_tov,  
  Dannye._Fld5951 as id_kontr,  
  Dannye._Fld5952 as id_kontr_v,  
  Dannye._Fld5953 as q_plan_pr,  
  --Dannye._Fld5957 , dn.ПроцДн ,  
  --convert(int,isnull(Dannye._Fld5957 * case when dn.dn in (6,7) then 7.0 / 100 *dn.ПроцДн else 1 end ,0)) as q_min_ost,  
  Dannye._Fld5957 as q_min_ost,  
  isnull(Dannye._Fld7224 ,0) as max_ost_tt_tov,  
  master.dbo.maxz(0,Dannye._Fld5955) as q_FO,  
  Dannye._Fld5956 as Cena_pr,  
  Dannye._Fld5958 as koef_tt,  
  0 as min_ost_tt_tov,  
  dateadd(year,-2000,Dannye._Fld5959) as date_r1,  
  dateadd(year,-2000,Dannye._Fld5960) as date_r2,  
  Dannye._Fld5961 as k1,  
  Dannye._Fld5962 as k2,  
  Dannye._Fld5963 as k3,  
  Dannye._Fld5964 as k4,  
  Dannye._Fld5965 as k5 ,  
  Dannye._Fld7812 q_rashod_fact ,  
  Dannye._Fld7647 id_zal ,   
  ROW_NUMBER() over (partition by Dannye._Fld5949, Dannye._Fld5950 order by Dannye._Fld5953 desc) rn  
 into #Raspr_tt_tov  
 from izbenkafin.dbo._InfoRg5947 as Dannye with   --,index (_InfoRg5947_ByDims_NNNNN))  
 --left join M2..tt_tov_kontr ttk with (  index (ind1)) on ttk.Number_r = @n  
 --left join #dn dn on dn.id_tt=Dannye._Fld5949 and dn.dn = DATEPART(weekday,@date_rasp)  
 where Dannye._Fld5948 = @N and isnull(Dannye._Fld7812,0)=0  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
 select @id_job , 200002, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) , @@ROWCOUNT  
 select @getdate = getdate()   
  
  
declare @strТекстSQLЗапроса nvarchar(max) , @id_tt_str nvarchar(max) , @id_tov_str nvarchar(max)  
Set @strТекстSQLЗапроса =   
'  
select @id_tt_str = SUBSTRING(convert(nvarchar(max),b) , 2,10000000)  
from  
(  
select  
(select distinct '','' + rtrim(id_tt)  
from #Raspr_tt_tov  
FOR XML PATH(''''),TYPE ) b  
) a  
'  
EXEC sp_executeSQL @strТекстSQLЗапроса , N'@id_tt_str nvarchar(max)   OUTPUT' , @id_tt_str = @id_tt_str OUTPUT  
Set @strТекстSQLЗапроса =   
'  
select @id_tov_str = SUBSTRING(convert(nvarchar(max),b) , 2,10000000)  
from  
(  
select  
(select distinct '','' + rtrim(id_tov)  
from #Raspr_tt_tov  
FOR XML PATH(''''),TYPE ) b  
) a  
'  
EXEC sp_executeSQL @strТекстSQLЗапроса , N'@id_tov_str nvarchar(max)   OUTPUT' , @id_tov_str = @id_tov_str OUTPUT  
  
create table #ostatki_tt_tov_currdate (TTUID nvarchar(max), TovarUID nvarchar(max), kol real, id_tov int, id_tt  int)  
  
--/**   
  
declare @ii as char(36)  
select @ii=  replace(convert(char(36),NEWID()) , '-' , '_')   
  
exec m2.dbo.ostatki_tt_tov_currdate @id_tov_str, @id_tt_str , 1 , @ii ,null , @id_job  
select @getdate = getdate()  
  
Set @strТекстSQLЗапроса =   
'  
insert into #ostatki_tt_tov_currdate  
select *  
from ##'+@ii +'    
  
drop table ##'+@ii  
  
EXEC sp_executeSQL @strТекстSQLЗапроса  
  
--select * from #ostatki_tt_tov_currdate  
  
create clustered index ind1 on #ostatki_tt_tov_currdate (id_tov , id_tt)  
  
--**/  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
 select @id_job , 200003, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) , @@ROWCOUNT  
 select @getdate = getdate()   
  
  
  
   
 insert into M2..tt_tov_kontr  
  ([Number_r]  
  ,[id_tt]  
  ,[id_tov]  
  ,[id_kontr]  
  ,[id_kontr_v]  
  ,[id_kontr_init]  
  ,[q_plan_pr]  
  ,[q_min_ost]  
  ,[max_ost_tt_tov]  
  ,[q_FO]  
  ,[cena_pr]  
  ,[koef_tt]  
  ,[min_ost_tt_tov]  
  ,[date_r1]  
  ,[date_r2]  
  ,[k1]  
  ,[k2]  
  ,[k3]  
  ,[k4]  
  ,[k5]  
  ,q_rashod_fact  
  ,id_zal)  
 -- FROM M2..tt_tov_kontr  
    
  --declare @date_rasp date = '2016-07-28' , @N int = 34855                                 
  --kirtoka  
   select   
  Number_r,  
  a.id_TT,  
  a.id_tov,  
  id_kontr,  
  id_kontr_v,  
  id_kontr,  
  q_plan_pr,  
  q_min_ost,  
  max_ost_tt_tov,  
  --q_FO,  
  isnull(o.kol,a.q_FO) ,  
  Cena_pr,  
  koef_tt,  
  min_ost_tt_tov,  
  date_r1,  
  date_r2,  
  k1,  
  k2,  
  k3,  
  k4,  
  k5 ,  
  q_rashod_fact ,  
  id_zal   
 from #Raspr_tt_tov a  
 left join   
 (select oo.id_tt, oo.id_tov, max(oo.kol) as kol from #ostatki_tt_tov_currdate oo group by oo.id_tt, oo.id_tov) o on a.id_tov = o.id_tov and  a.id_tt = o.id_tt    
    where a.rn=1  
/* select   
  Number_r,  
  a.id_TT,  
  a.id_tov,  
  id_kontr,  
  id_kontr_v,  
  id_kontr,  
  q_plan_pr,  
  q_min_ost,  
  max_ost_tt_tov,  
  --q_FO,  
  isnull(o.kol,a.q_FO) ,  
  Cena_pr,  
  koef_tt,  
  min_ost_tt_tov,  
  date_r1,  
  date_r2,  
  k1,  
  k2,  
  k3,  
  k4,  
  k5 ,  
  q_rashod_fact ,  
  id_zal   
 from #Raspr_tt_tov a  
 left join #ostatki_tt_tov_currdate o on a.id_tov = o.id_tov and  a.id_tt = o.id_tt    
    where a.rn=1*/  
       
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200004, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
   
  
  
            
    insert into  [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
 select Number_r,'Не уникальные данные по распределению товара на ТТ (id_TT='+ rtrim(id_tt)+',id_tov='+RTRIM(id_tov)+') - количество записей ' +RTRIM(rn)  
 from #Raspr_tt_tov a  
    where a.rn>1  
      
    if not exists(select top 1 * from IzbenkaFin.dbo._InfoRg5947 as Dannye   where Dannye._Fld5948 = @N )  
    begin  
      --если нет данных по распределению, то инициируем ошибку.  
      insert into  [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
      select @N,'Нет данных по распределению в 1С (IzbenkaFin.dbo._InfoRg5947)'  
        
      if @test_raspr =0 --если не тестовое распределение, то вызываем ошибку с дальнейшей рассылкой уведомлений  
      begin  
         RAISERROR ('Нет данных по распределению в 1С (IzbenkaFin.dbo._InfoRg5947)',   
               16, -- Severity.    
               1 -- State.    
               )    
      end  
      else  
      begin --если тестовое распределение, то завершаем работу  
        return  
      end            
    end  
      
    drop table #Raspr_tt_tov  
   
 end  
       
  
       
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200010, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  **/  
  
-- товары ЕД и ЛЛ - снять ограничения по избытку  
create table #tovLLED (id_tov int)  
insert into #tovLLED  
select t.id_tov  
from M2..tovari t  
where t.id_group in  
(Select gr.id_group  
from M2..[Group tovari] gr  
where --gr.id_group = gr.id_parent  
 id_parent in (10325,10330)  
 )  
create unique clustered index ind1 on #tovLLED (id_tov)  
   
  
------------------------------------------------------ АК новый кусок - для двойных распределений  
-- дневное распределение  - убираем  с остатков склада 30%.  
  
/**  
select distinct ttk.id_tov  
into #tov_pr_raspr  
from archive_tt_tov_kontr ttk   
where ttk.Number_r= @N   
and ttk.q_rashod_fact<>0  
**/  
  
--нет ли разной складируемости у полных аналогов  
--declare @N int = 75722  
create table #err_pla (id_tov int, raspr_double int)  
insert into #err_pla  
select tvp.id_tov_2 , MAX(t.raspr_double) raspr_double  
from   M2..tov t   
inner join Reports..tovari_vse_poln_analogi tvp on t.id_tov = tvp.id_tov_1  
where t.Number_r=@N  
group by tvp.id_tov_2  
having MIN(t.Skladir) <> MAX(t.Skladir)  
  
--declare @N int = 75722  
-- всем поставить складируемость  
update t  
set Skladir=1 , raspr_double = ep.raspr_double  
--select *  
from M2..tov t  
inner join  Reports..tovari_vse_poln_analogi tvp on t.id_tov = tvp.id_tov_1  
inner join #err_pla ep on ep.id_tov = tvp.id_tov_2  
where t.Number_r=@N  
  
-- обнуление для ВВ макс_ост для нескладируемых товаров  
  
update ttk  
set max_ost_tt_tov=0  
from M2..tt_tov_kontr ttk  
--inner join m2..tt on tt.id_TT = ttk.id_tt and tt.tt_format=2  
inner join M2..tov on tov.Number_r = ttk.Number_r and ttk.id_tov = tov.id_tov and tov.Skladir=0  
where ttk.Number_r=@N  
and ttk.tt_format_rasp in (2,4,12,14)  
  
  
-- АК обнуления мин_остатков для срока годности менее 30 дней  --убрал ( 5 овощам/фруктам/зелени)  
-- и еще для сырья заморозки пекарен  Сырье для пекарни 10252  
update ttk  
set q_min_ost = 0 , min_ost_tt_tov=0  
--declare @N int = 78297  
--select *  
from M2..tov_kontr tk  
inner join M2..tt_tov_kontr ttk on ttk.Number_r = tk.Number_r and ttk.id_tov = tk.id_tov and ttk.id_kontr = tk.id_kontr  
inner join M2..tov tov on tov.Number_r = tk.Number_r and tov.id_tov = tk.id_tov  
inner join M2..Tovari t on t.id_tov = tk.id_tov  
--inner join M2..tt on tt.id_TT = ttk.id_tt and tt.tt_format in (2,4,14)  
--left join #MCK m on m.id_tt_mck=ttk.id_tt  
where ttk.Number_r=@n and (tk.srok_godnosti< 30 --(tk.srok_godnosti< case when t.id_group in (10127,10174,10176) then 5 else 30 end  -- по овощам/фруктам/зелени -5 , остальные 30   
--or tov.Skladir=0 )    
or t.id_group= 10252 ) -- всю группу заморозка пекарни  
and (q_min_ost != 0 or min_ost_tt_tov!=0)  
--and m.id_tt_mck is null  
and ttk.tt_format_rasp in (2,4,14,12)  
  
  
-- и добавить еще эксперимент - обнулить для групп   
--Сосиски.Сардельки.Колбаски 1 10147  
--Колбаса вареная. Ветчина 1 10146  
  
  
  
-- таблицы понадобятся для распределения  
create table #q_sobr (id_tov int , q_rashod_fact int ,q_raspr int , q_ost int )  
create table #q_sobr_kontr (id_tov int , id_kontr int ,q_raspr int )  
create table #tov_only_VV (id_tov int )  
  
--select * from #koef_rasp  
-- не менять, если время распределения до 8 утра  
  
if not @hour_raspr between 4 and 7  
  
begin  
  
-- внес изменение, что если t.raspr_d_1_2=1 , но распределять только самые крупные характеристики  
  
  
-- посчитать, сколько всего по товару собрать  
  
--truncate table #koef_rasp  
insert into #q_sobr  
  
Select   
tk.id_tov ,  isnull(sum(ttk.q_rashod_fact),0) q_rashod_fact,   
 master.dbo.maxz(0,(((sum(tk.q_ost_sklad)  + isnull(sum(ttk.q_rashod_fact),0)) * 0.7) - isnull(sum(ttk.q_rashod_fact),0)) ) q_raspr  
 , sum(tk.q_ost_sklad) q_ost  
from M2..tov_kontr tk  
inner join M2..tov t on t.Number_r = tk.Number_r and t.id_tov = tk.id_tov  
  
left join  
(  
select tk.id_tov , tk.id_kontr , SUM(tk.q_rashod_fact) q_rashod_fact  
from M2..tt_tov_kontr tk  
where tk.Number_r=@N  
group by tk.id_tov , tk.id_kontr  
) ttk on ttk.id_tov=tk.id_tov and ttk.id_kontr=tk.id_kontr  
  
where tk.Number_r=@n  
and t.raspr_double=1 and t.raspr_d_1_2=1  
and tk.q_ost_sklad>0  
group by tk.id_tov  
  
-- теперь найти характетиристики,  с которых собрать  
  
  
--truncate table #koef_rasp  
insert into #q_sobr_kontr  
  
select qs.id_tov , s.id_kontr  , master.dbo.MINz(s.q_ost_sklad , qs.q_raspr) q_raspr  
from #q_sobr qs  
inner join  
(select a.id_tov , a.id_kontr , a.q_ost_sklad , SUM(b.q_ost_sklad) Нараст_q ,   
ROW_NUMBER() over (PARTITION by a.id_tov order by a.q_ost_sklad desc , a.id_kontr desc) rn  
from   
(select tk.*  
from M2..tov_kontr tk  
inner join M2..tov t on t.Number_r = tk.Number_r and t.id_tov = tk.id_tov  
where tk.Number_r=@n  
and t.raspr_double=1 and t.raspr_d_1_2=1  
and tk.q_ost_sklad>0  
) a  
inner join  
(select tk.*  
from M2..tov_kontr tk  
inner join M2..tov t on t.Number_r = tk.Number_r and t.id_tov = tk.id_tov  
where tk.Number_r=@n  
and t.raspr_double=1 and t.raspr_d_1_2=1  
and tk.q_ost_sklad>0  
) b on a.id_tov = b.id_tov and (b.q_ost_sklad > a.q_ost_sklad or (b.q_ost_sklad = a.q_ost_sklad and b.id_kontr> a.id_kontr) or a.id_kontr = b.id_kontr)  
group by a.id_tov , a.id_kontr , a.q_ost_sklad  
) s on s.id_tov = qs.id_tov and (s.Нараст_q<= qs.q_raspr   
or rn=1)  
where master.dbo.MINz(s.q_ost_sklad , qs.q_raspr)>0  
  
create unique clustered index ind1 on #q_sobr_kontr (id_tov,id_kontr)  
  
update tk  
set q_ost_sklad =  
 floor( isnull(kr.q_raspr,0)  / tk.Kolvo_korob) * tk.Kolvo_korob , q_wait_sklad =  0   
from M2..tov_kontr tk  
inner join M2..tov t on t.Number_r = tk.Number_r and t.id_tov = tk.id_tov  
left join #q_sobr_kontr kr on kr.id_tov = tk.id_tov and kr.id_kontr = tk.id_kontr  
where tk.Number_r=@n  
and t.raspr_double=1 and t.raspr_d_1_2=1  
--and not( @id_zone in (8,30))  
  
  
--declare @N int = 10213  
insert into #tov_only_VV  
SELECT  distinct t.id_tov   
  FROM [M2].[dbo].[tov_init] t   
  inner join M2..tov_kontr_init tk (nolock) on tk.Number_r = @N and t.id_tov = tk.id_tov  
  where t.Number_r = @N and t.raspr_double=1 and t.raspr_d_1_2=1  
  group by t.Number_r ,t.id_tov , t.z_1  
  having SUM(tk.q_ost_sklad) >0  
  and ( SUM(tk.q_ost_sklad) < t.z_1 * 0.2)  
  
    
    
  
end  
  
  
--- Заказы покупаталей  
  
if @sql_raspr = 0  
begin  
     
   -------------------------------------------------------------------------------------------------------  
--declare @N int = 9339, @Date_Rasp date = '2019-05-25'  
--drop table #q_zak  
truncate table #q_zak  
insert into #q_zak  
  
select tt.id_TT , td.id_tov , isnull(td.id_kontr,0) id_kontr, isnull(td.BonusCard,'') BonusCard,  
sum( case when td.operation_type = 802 then 1 else -1  end * td.Quantity ) q   
, max(case when ISNULL(bor802.ThisManufacturer, 0) =0 then 0 else 1 end) ThisManufacturer   
from sms_repl..td_move td    
LEFT JOIN [SMS_REPL].[dbo].[BuyersOrders802] bor802    
  ON td.Id_doc = bor802.Id_doc  
      AND td.tduid = bor802.tduid  
inner join m2..tt on tt.N  = td.ShopNo_rep  
  
inner join m2..raspr_zadanie_tt t on t.id_tt = tt.id_TT and t.number_r = @N  
inner join M2..tov_init tov on tov.id_tov = td.id_tov and tov.Number_r = @N  
  
left join #tov_only_VV t_vv on t_vv.id_tov = td.id_tov  
   
where td.operation_type in ( 802,803)  
and td.Date_proizv = @date_rasp  
  
and not ( (@hour_raspr between 4 and 7 or t_vv.id_tov is not null) and tov.raspr_double=1 ) -- не распределять иные форматы, если утром или будет еще распр и скоропорт  
  
  
group by tt.id_TT , td.id_tov, isnull(td.id_kontr,0) --, ISNULL(bor802.ThisManufacturer, 0)  
, isnull(td.BonusCard,'')   
having sum( case when td.operation_type = 802 then 1 else -1  end * td.Quantity ) >0  
  
  
  
  
  
  
  
--select * from #q_zak  
  
--declare @N int = 9339, @Date_Rasp date = '2019-05-25'  
truncate table #q_zak_made  
insert into #q_zak_made  
Select top 1 with ties  r.number_r,  r.id_tt , r.id_tov  , qz.id_kontr , qz.BonusCard , qz.ThisManufacturer  
from M2..archive_rasp r  
inner join m2..Raspr_zadanie rz on rz.Number_r = r.number_r  
inner join #q_zak qz on qz.id_TT = r.id_tt and qz.id_tov = r.id_tov and (qz.id_kontr = r.id_kontr or qz.ThisManufacturer=0 )  
inner join M2..tt_tov_kontr_init ttki on ttki.Number_r = @N and ttki.id_tt = qz.id_TT and ttki.id_tov = qz.id_tov and (ttki.id_kontr  =  qz.id_kontr or qz.ThisManufacturer=0) and ttki.q_rashod_fact>0  
where r.number_r<@N and r.number_r> 0 and rz.Date_r = @date_rasp and rz.canceled is null and rz.test_raspr=0 and rz.ErrorMes is null and rz.sql_raspr is null  
order by ROW_NUMBER() over ( partition by  r.id_tt , r.id_tov  , qz.id_kontr , qz.BonusCard , qz.ThisManufacturer order by r.number_r desc)  
  
  
  
insert into m2..rasp_zakaz_pok  
      ([Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[BonusCard]  
      ,[q]  
      ,[ThisManufacturer]  
      ,[q_raspr]  
      ,[id_kontr_new]  
      ,[type_add]  
      ,[kolvo_korob]  
      ,[N_korob]  
      ,[id_tov_pvz]  
      ,[q_raspr_cur])  
  
--declare @N int = 9339, @Date_Rasp date = '2019-05-25'  
select @N Number_r , td.id_TT , td.id_tov , td.id_kontr , td.BonusCard ,    
case when tk.Kolvo_korob is not null then  master.dbo.minz(td.q - (k.Korob-1)*tk.Kolvo_korob,tk.Kolvo_korob) else td.q end q,   
td.ThisManufacturer, 0 q_raspr , Null id_kontr_new, 0,   
case when tk.Kolvo_korob is not null then tk.Kolvo_korob else td.q end Kolvo_korob , k.Korob , ISNULL(tov.id_tov_pvz , tov.id_tov) , 0  
from #q_zak td  
left join M2..tov_kontr_init tk on tk.Number_r = @N and tk.id_tov = td.id_tov and tk.id_kontr=td.id_kontr   
inner join M2..tov_init tov on tov.Number_r = @N and tov.id_tov = td.id_tov   
inner join M2..Korob_add k on 1=1  
--inner join M2..Tovari t on t.id_tov = td.id_tov  
  
left join #q_zak_made qz on  qz.id_TT = td.id_tt and qz.id_tov = td.id_tov and qz.id_kontr = td.id_kontr and qz.BonusCard = td.BonusCard and qz.ThisManufacturer = td.ThisManufacturer  
  
where   
(  
((k.Korob-1)*tk.Kolvo_korob < td.q +0.1 and  ((td.q - (k.Korob-1)*tk.Kolvo_korob)>tk.Kolvo_korob*0.1 or k.Korob=1) and tk.Kolvo_korob is not null)   
or (k.Korob=1 and tk.Kolvo_korob is null )   
)  
and qz.number_r is null  
order by td.id_TT , td.id_tov , td.id_kontr , td.BonusCard, td.ThisManufacturer , k.Korob   
  
  
insert into m2..rasp_zakaz_pok  
      ([Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[BonusCard]  
      ,[q]  
      ,[ThisManufacturer]  
      ,[q_raspr]  
      ,[id_kontr_new]  
      ,[type_add]  
      ,[kolvo_korob]  
      ,[N_korob]  
      ,[id_tov_pvz]  
      ,[q_raspr_cur]  
      ,id_tt_old)  
  
--declare @N int = 9339, @Date_Rasp date = '2019-05-25'  
select @N  
      ,td.[id_tt]  
      ,td.[id_tov]  
      ,td.[id_kontr]  
      ,td.[BonusCard]  
      ,[q]  
      ,td.[ThisManufacturer]  
      ,[q_raspr]  
      ,[id_kontr_new]  
      ,[type_add]  
      ,[kolvo_korob]  
      ,[N_korob]  
      ,[id_tov_pvz]  
      ,0  
      ,td.id_tt_old  
from M2..rasp_zakaz_pok td  
  
inner join #q_zak_made qz on  qz.id_TT = td.id_tt and qz.id_tov = td.id_tov and qz.id_kontr = td.id_kontr and qz.BonusCard = td.BonusCard and qz.ThisManufacturer = td.ThisManufacturer  
and td.Number_r = qz.number_r  
order by   
       td.[id_tt]  
      ,td.[id_tov]  
      ,td.[id_kontr]  
      ,td.[BonusCard]  
      ,td.[ThisManufacturer]  
      ,[N_korob]  
  
  
  
  
if @only_zakaz=1 -- если только по заказам покупателей, то удалить все тт и товары, которые не в заказе  
delete tt_tov_kontr_init  
-- declare @N int = 9096  
from m2..tt_tov_kontr_init ttk  
inner join M2..tov_init ti on ti.Number_r = @N and ti.id_tov = ttk.id_tov  
left join #q_zak qz on qz.id_TT = ttk.id_tt and qz.id_tov = ISNULL(ti.id_tov_pvz , ttk.id_tov )  
where ttk.Number_r = @N and qz.id_TT is  null  
  
  
  
  
end  
  
  
  
-- любое распределение - добавляет к факт_ост уже распределенное  
  
update tk  
set q_FO = tk.q_FO + tk.q_rashod_fact  
from M2..tt_tov_kontr tk  
inner join M2..tov t on t.Number_r = tk.Number_r and t.id_tov = tk.id_tov  
where tk.Number_r=@n  
--and t.raspr_double=1 --and t.raspr_d_1_2=2  
  
  
  
    
  
 ---------------------------- комплекты  
 create table #complect (id_tov int, id_tov_sostav int, kolvo real)  
 -- выбираем комплекты  
 insert into #complect  
 SELECT c.id_tov , c.id_tov_sostav , c.kolvo  
 FROM [M2].[dbo].[complects] (@date_rasp) c  
  
 if exists   
 (select *  
 from M2..tov_kontr   tk  
 inner join #complect c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
 )  
 begin  
 -- значит везде в таблицах меняем по комплектам данные на составяляющие, те остатки по комплектам умножаем на колво во вложениях.  
  
 --  
 update M2..tov_kontr   
 set Kolvo_korob = c.kolvo , q_ost_sklad = tk.q_ost_sklad * c.kolvo , q_wait_sklad = tk.q_wait_sklad * c.kolvo  
 from M2..tov_kontr (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
  
 update M2..tov_kontr_date   
 set q_ost_sklad = tk.q_ost_sklad * c.kolvo   
 from M2..tov_kontr_date (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 update M2..tov_kontr_zal  
 set q_ost_zal = tk.q_ost_zal * c.kolvo   
 from M2..tov_kontr_zal (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 update M2..tt_tov_kontr  
 set q_rashod_fact = tk.q_rashod_fact * c.kolvo  
 from M2..tt_tov_kontr (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 -- и по остальным числам сложить из составляющих по каждому магазину  
  
 update M2..tt_tov_kontr  
 set cena_pr = c.cena_pr ,  
 max_ost_tt_tov = c.max_ost_tt_tov ,  
 min_ost_tt_tov = c.min_ost_tt_tov ,  
 q_FO = c.q_FO ,  
 q_min_ost = c.q_min_ost ,  
 q_plan_pr = c.q_plan_pr  
 from M2..tt_tov_kontr (rowlock) tk  
 inner join   
 (select   
 c.id_tov ,  
 tk.id_tt   
 , avg(tk.cena_pr) cena_pr  
 , SUM(tk.max_ost_tt_tov) max_ost_tt_tov  
 , SUM(tk.min_ost_tt_tov) min_ost_tt_tov  
 , SUM(tk.q_FO) q_FO  
 , SUM(tk.q_min_ost) q_min_ost  
 , SUM(tk.q_plan_pr) q_plan_pr  
 from M2..tt_tov_kontr (rowlock) tk  
 inner join #complect c on c.id_tov_sostav = tk.id_tov  
 where tk.Number_r = @N  
 group by c.id_tov , tk.id_tt) c on c.id_tov = tk.id_tov and c.id_tt = tk.id_tt  
 where tk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200020, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 end  
  
  
 --------------------------------------  
 -- замену делаем на полные аналоги  
  
  -- сначала запишет в ошибки, если нет  
  insert into [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
  select tov.Number_r , 'не прописан основной аналог ' + RTRIM(tp.Name_Zadvoen)  
  from m2..tov with (rowlock)  
  inner join Reports..tov_poln_zamenyaem tp on tp.id_tov_Zadvoen = tov.id_tov  
  where tov.Number_r = @N and isnull(id_tov_pvz,0) <> tp.id_tov_Osnovn  
  
  --select *  
  update m2..tov   
  set id_tov_pvz = tp.id_tov_Osnovn  
  from m2..tov with (rowlock)  
  inner join Reports..tov_poln_zamenyaem tp on tp.id_tov_Zadvoen = tov.id_tov  
  where tov.Number_r = @N and isnull(id_tov_pvz,0) <> tp.id_tov_Osnovn  
  
 -- проставить коэф пересчета для неосновного товара, если он нужен  
  update m2..tov   
  set koef_pvz = t1.ves / t2.ves   
  from m2..tov with (rowlock)  
  inner join Reports..tov_poln_zamenyaem tp on tp.id_tov_Zadvoen = tov.id_tov  
  inner join M2..Tovari t1 on t1.id_tov = tp.id_tov_Zadvoen  
  inner join M2..Tovari t2 on t2.id_tov = tp.id_tov_Osnovn  
  where t1.Ves <> t2.Ves and (t1.Ves=1 or t2.ves =1) and t1.Ed_Izm <> t2.Ed_Izm  
  and tov.Number_r = @N   
  
  insert into [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
  select tov.Number_r , 'не прописан основной аналог у основного аналога' + RTRIM(tp.Name_Zadvoen)  
  from m2..tov with (rowlock)  
  inner join Reports..tov_poln_zamenyaem tp on tp.id_tov_Osnovn = tov.id_tov  
  where tov.Number_r = @N and isnull(id_tov_pvz,0) <> tp.id_tov_Osnovn   
    
  update m2..tov   
  set id_tov_pvz = tp.id_tov_Osnovn  
  from m2..tov with (rowlock)  
  inner join Reports..tov_poln_zamenyaem tp on tp.id_tov_Osnovn = tov.id_tov  
  where tov.Number_r = @N and isnull(id_tov_pvz,0) <> tp.id_tov_Osnovn   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200030, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  create table #err_osn (id_tov int , id_kontr int , id_tt int , Колво int)  
    
    
insert into [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
select @N , 'Нет характеристики в Har_kontr - не будет распределения ' + RTRIM(tovari.Name_tov) + ' ' + RTRIM(kontr.nova_kontr)  
--select distinct tk.id_tov , tk.id_kontr , tovari.Name_tov , kontr.nova_kontr  
from M2..tov_kontr tk    
  inner join M2..Tovari tovari on tovari.id_tov = tk.id_tov  
  inner join M2..kontr  on kontr.id_kontr = tk.id_kontr   
left join M2.dbo.Har_kontr as Hk   on tk.id_tov = Hk.id_tov and tk.id_kontr = Hk.id_kontr  
where Hk.id_tov is null and tk.Number_r = @N   
  
  
------------------------------------------------------------------------------------------  
 -- новая проверка, если в полных аналогах есть задвоенный товар 07.08.2018 Кривенко А.  
  
create table #zamrena_id_kontr (id_tov int, id_kontr int, new_id_kontr int , rn int)  
insert into  #zamrena_id_kontr  
select a.id_tov , a.id_kontr , a.id_kontr+1000000*a.rn new_id_kontr  ,a.rn   
from   
(  
select  tov.id_tov_pvz , tk.*  
, ROW_NUMBER() over (partition by tov.id_tov_pvz , tk.id_kontr order by tk.q_ost_sklad desc) rn  
from M2..tov_kontr   tk  
inner join M2..tov   
on tk.Number_r = tov.Number_r and tk.id_tov = tov.id_tov  
where tk.Number_r = @N  
)a  
where a.rn>1 and id_tov_pvz is not null  
  
  update M2..tov_kontr  
  set id_kontr = zik.new_id_kontr  
  from M2..tov_kontr tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
  
  update M2..tt_tov_kontr  
  set id_kontr = zik.new_id_kontr , id_kontr_v = zik.new_id_kontr  
  from M2..tt_tov_kontr tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
  
  update M2..tov_kontr_date  
  set id_kontr = zik.new_id_kontr  
  from M2..tov_kontr_date tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
  
  update M2..tov_kontr_zal  
  set id_kontr = zik.new_id_kontr  
  from M2..tov_kontr_zal tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
      
------------------------------------------------------------------------------------------  
  
  
          
  -- если не повторяются характеристики  
  insert into #err_osn  
  SELECT tov.id_tov_pvz id_tov_Osnovn ,tk.id_kontr , null , count( distinct tk.id_tov) Колво  
  FROM M2..tov_kontr tk  
  --inner join Reports..tov_poln_zamenyaem t on t.id_tov_Zadvoen = tk.id_tov  
  inner join M2..tov on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
  where tk.Number_r = @N   
  group by tk.id_kontr , tov.id_tov_pvz  
  having count( distinct tk.id_tov) > 1 or max(tk.id_tov_init) is null  
  
  insert into [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
  select @N , 'повторяются характеристики ' + RTRIM(tovari.Name_tov) + ' ' + RTRIM(kontr.nova_kontr)  
  from #err_osn er  
  inner join M2..Tovari tovari on tovari.id_tov = er.id_tov  
  inner join M2..kontr  on kontr.id_kontr = er.id_kontr   
  
  insert into [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
  select tov.Number_r , 'не занесен в tov основной аналог ' + RTRIM(tovari.Name_tov)  
  from M2..tov   
  inner join M2..Tovari tovari on tovari.id_tov = tov.id_tov  
  inner join Reports..tov_poln_zamenyaem t on t.id_tov_Zadvoen = tov.id_tov  
  left join M2..tov tov2 on tov2.Number_r = @N and t.id_tov_Osnovn = tov2.id_tov  
  where tov.Number_r = @N and tov2.id_tov is null  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200040, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- добавляем основной, если его нет  
  insert into M2..tov   
  (  
  [Number_r]  
  ,[id_tov]  
  ,[id_group]  
  ,[id_tov_vz]  
  ,[ves]  
  ,[koef_tov]  
  ,[Skladir]  
  ,[pick_item]  
  ,[id_tov_pvz]  
  )  
    
  select   
  [Number_r]  
  ,[id_tov_Osnovn]  
  ,[id_group]  
  ,[id_tov_vz]  
  ,[ves]  
  ,[koef_tov]  
  ,[Skladir]  
  ,[pick_item]  
  ,[id_tov_Osnovn]  
  from   
  (select   
  tov.[Number_r]  
  ,t.id_tov_Osnovn  
  ,tov.[id_group]  
  ,tov.[id_tov_vz]  
  ,tovari.[ves]  
  ,tov.[koef_tov]  
  ,tov.[Skladir]  
  ,tov.[pick_item]  
  ,tov.[id_tov_pvz]  
  --select *  
  , ROW_NUMBER() over (PARTITION by t.id_tov_Osnovn order by tov.id_tov) rn  
  from M2..tov   
  inner join M2..Tovari tovari on tovari.id_tov = tov.id_tov  
  inner join Reports..tov_poln_zamenyaem t on t.id_tov_Zadvoen = tov.id_tov  
  left join M2..tov tov2 on tov2.Number_r = @N and t.id_tov_Osnovn = tov2.id_tov  
  where tov.Number_r = @N and tov2.id_tov is null  
  ) a where a.rn=1  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200041, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()     
  
  
  -- обновляем tov_kontr  
  update M2..tov_kontr  
  set id_tov = tov.id_tov_pvz ,  
  q_ost_sklad = q_ost_sklad * ISNULL(tov.koef_pvz,1) ,   
  Kolvo_korob = Kolvo_korob * ISNULL(tov.koef_pvz,1) ,  
  q_wait_sklad = q_wait_sklad * ISNULL(tov.koef_pvz,1)  
  --declare @N int = 27540  
  --select tk.id_tov , tov.id_tov_pvz , q_ost_sklad / tov.ves * tov2.ves , q_ost_sklad   
  FROM M2..tov_kontr (rowlock) tk  
  --inner join Reports..tov_poln_zamenyaem t on t.id_tov_Zadvoen = tk.id_tov  
  inner join M2..tov on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
  left join #err_osn er on er.id_tov = tov.id_tov_pvz  
  where tk.Number_r = @N and er.id_tov is null  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200042, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()     
    
    
 -- если на 1 ТТ есть более двух аналогов, то убрать по неосновным  
 --declare @N int = 39637                                 
  select isnull(tov.id_tov_pvz,ttk.id_tov) id_tov, ttk.id_tt ,   
  SUM(ttk.q_plan_pr * ISNULL(tov.koef_pvz,1)) q_plan_pr ,   
  SUM(ttk.q_FO * ISNULL(tov.koef_pvz,1)) q_FO,   
  SUM(ttk.q_rashod_fact * ISNULL(tov.koef_pvz,1)) q_rashod_fact,  
  max(ttk.max_ost_tt_tov * ISNULL(tov.koef_pvz,1)) max_ost_tt_tov,  
  max(ttk.min_ost_tt_tov * ISNULL(tov.koef_pvz,1)) min_ost_tt_tov,  
  max(ttk.q_min_ost * ISNULL(tov.koef_pvz,1)) q_min_ost  
    
  into #zamen_analog  
    
  FROM M2..tt_tov_kontr ttk with (index(ind1) )  
  left join M2..tov_kontr tk with (index(ind2) )   
  on tk.Number_r = @N and tk.id_tov_init = ttk.id_tov  
  and tk.id_kontr = ttk.id_kontr  
    
  inner join M2..tov tov with(index(PK_tov) ) on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
    
  inner join m2..tt on tt.id_TT = ttk.id_tt --and tt.tt_format<>10  
    
  where ttk.Number_r = @N   
  group by isnull(tov.id_tov_pvz,ttk.id_tov) , ttk.id_tt  
  having COUNT(*)>1  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200050, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  -- выбрать характеристику с планом продаж  
  select ttk.id_tt , isnull(tov.id_tov_pvz,ttk.id_tov) id_tov ,   
  ttk.id_kontr , ttk.id_kontr_init , ttk.id_kontr_v, ttk.id_zal , ttk.koef_tt ,  
  ROW_NUMBER() over (partition by ttk.id_tt , isnull(tov.id_tov_pvz,ttk.id_tov) order by q_plan_pr desc) rn  
  into #zamen_analog_2  
  FROM M2..tt_tov_kontr ttk with (index(ind1) )  
  left join M2..tov_kontr tk with (index(ind2) )   
  on tk.Number_r = @N and tk.id_tov_init = ttk.id_tov  
  and tk.id_kontr = ttk.id_kontr  
    
  inner join M2..tov   on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
    
  where ttk.Number_r = @N and q_plan_pr>0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200051, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  delete from #zamen_analog_2  
  where rn<>1  
    
 --select ttk.*  
 --declare @N int = 36717  
 delete M2..tt_tov_kontr  
    from #zamen_analog a  
 inner join M2..tov t  on t.id_tov_pvz = a.id_tov and t.Number_r = @N  
 inner join M2..tt_tov_kontr (rowlock) ttk  
 on ttk.Number_r = @N and ttk.id_tt = a.id_tt and ttk.id_tov = t.id_tov  
  
 left join #zamen_analog_2 a2 on a.id_tt = a2.id_tt and ttk.id_tov = a2.id_tov  
 where (a2.id_tov is null or ttk.q_plan_pr =  0)  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200052, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
 -- заменить у тех, кто схлопнулся  
   
    update M2..tt_tov_kontr  
 set q_plan_pr = a.q_plan_pr , q_FO= a.q_FO, q_rashod_fact= a.q_rashod_fact ,  
 max_ost_tt_tov =a.max_ost_tt_tov , min_ost_tt_tov = a.min_ost_tt_tov , q_min_ost = a.q_min_ost  
 , id_kontr = isnull(a2.id_kontr, ttk.id_kontr)  
 , id_kontr_init = isnull(a2.id_kontr_init, ttk.id_kontr_init)  
 , id_kontr_v = isnull(a2.id_kontr_v, ttk.id_kontr_v)  
 , id_zal = isnull(a2.id_zal, ttk.id_zal)  
 , koef_tt = isnull(a2.koef_tt, ttk.koef_tt)  
   
 --declare @N int = 43433  
 --select *  
   
 from #zamen_analog a  
 inner join M2..tt_tov_kontr (rowlock) ttk  
 on ttk.Number_r = @N and ttk.id_tt = a.id_tt and ttk.id_tov = a.id_tov  
  
 left join #zamen_analog_2 a2 on a.id_tt = a2.id_tt and a.id_tov = a2.id_tov  
   
 -- а теперь по товарамс полными аналогами, но без двойного заведения    
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200053, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
 --declare @N int = 43433     
   --insert into #zamen_analog                             
  select ttk.id_tov id_tov_init , isnull(tov.id_tov_pvz,ttk.id_tov) id_tov, ttk.id_tt ,   
  SUM(ttk.q_plan_pr * ISNULL(tov.koef_pvz,1)) q_plan_pr ,   
  SUM(ttk.q_FO * ISNULL(tov.koef_pvz,1)) q_FO,   
  SUM(ttk.q_rashod_fact * ISNULL(tov.koef_pvz,1)) q_rashod_fact,  
  max(ttk.max_ost_tt_tov * ISNULL(tov.koef_pvz,1)) max_ost_tt_tov,  
  max(ttk.min_ost_tt_tov * ISNULL(tov.koef_pvz,1)) min_ost_tt_tov,  
  max(ttk.q_min_ost * ISNULL(tov.koef_pvz,1)) q_min_ost  
  into #zamen_analog_3  
  FROM M2..tt_tov_kontr ttk with (index(ind1) )  
  left join M2..tov_kontr tk with (index(ind2) )   
  on tk.Number_r = @N and tk.id_tov_init = ttk.id_tov  
  and tk.id_kontr = ttk.id_kontr  
    
  inner join M2..tov tov with(index(PK_tov) ) on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
    
  where ttk.Number_r = @N --and ttk.id_tov  in ( 18784  , 20637)  
  group by ttk.id_tov, isnull(tov.id_tov_pvz,ttk.id_tov) , ttk.id_tt  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200054, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
 --select ttk.*  
 update M2..tt_tov_kontr  
 set q_plan_pr = a.q_plan_pr , q_FO= a.q_FO, q_rashod_fact= a.q_rashod_fact ,  
 max_ost_tt_tov =a.max_ost_tt_tov , min_ost_tt_tov = a.min_ost_tt_tov , q_min_ost = a.q_min_ost  
 , id_kontr = isnull(a2.id_kontr, ttk.id_kontr)  
 , id_kontr_init = isnull(a2.id_kontr_init, ttk.id_kontr_init)  
 , id_kontr_v = isnull(a2.id_kontr_v, ttk.id_kontr_v)  
 , id_zal = isnull(a2.id_zal, ttk.id_zal)  
 , koef_tt = isnull(a2.koef_tt, ttk.koef_tt)  
   
 --declare @N int = 43433  
 --select *  
   
 from #zamen_analog_3 a  
 inner join M2..tt_tov_kontr (rowlock) ttk  
 on ttk.Number_r = @N and ttk.id_tt = a.id_tt and ttk.id_tov = a.id_tov_init  
  
 left join #zamen_analog_2 a2 on a.id_tt = a2.id_tt and a.id_tov = a2.id_tov  
  
    left join #zamen_analog a3 on a.id_tt = a3.id_tt and a.id_tov = a3.id_tov  
    where a3.id_tt is null  
      
 -- drop table #zamen_analog  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200060, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  -- обновляем tov_kontr_date  
  update M2..tov_kontr_date  
  set id_tov = tk.id_tov ,   
  q_ost_sklad = tkd.q_ost_sklad * ISNULL(tov.koef_pvz,1)  
  FROM M2..tov_kontr_date (rowlock) tkd  
  inner join M2..tov_kontr tk with (index(ind2) ) on tk.Number_r = @N and tk.id_tov_init = tkd.id_tov  
  and tk.id_kontr = tkd.id_kontr  
  
  inner join M2..tov on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
    
  where tkd.Number_r = @N and tkd.id_tov <> tk.id_tov  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200061, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()     
    
  -- обновляем tov_kontr_date  
  update [M2].[dbo].[tov_kontr_zal]  
  set id_tov = tk.id_tov ,   
  q_ost_zal = tkd.q_ost_zal * ISNULL(tov.koef_pvz,1)  
  FROM [M2].[dbo].[tov_kontr_zal] (rowlock) tkd  
  inner join M2..tov_kontr tk with (index(ind2) ) on tk.Number_r = @N and tk.id_tov_init = tkd.id_tov  
  and tk.id_kontr = tkd.id_kontr  
  
  inner join M2..tov on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
    
  where tkd.Number_r = @N and tkd.id_tov <> tk.id_tov  
    
    
  -- обновляем tt_tov_kontr  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200062, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()     
    
  
  update M2..tt_tov_kontr  
  set id_tov =  tov.id_tov_pvz ,  
  q_plan_pr = q_plan_pr * ISNULL(tov.koef_pvz,1),  
  q_min_ost = q_min_ost * ISNULL(tov.koef_pvz,1),  
  min_ost_tt_tov = min_ost_tt_tov * ISNULL(tov.koef_pvz,1) ,  
  q_FO = q_FO * ISNULL(tov.koef_pvz,1),  
  max_ost_tt_tov = max_ost_tt_tov * ISNULL(tov.koef_pvz,1),  
  q_rashod_fact = q_rashod_fact * ISNULL(tov.koef_pvz,1)  
    
  /**  
  declare @N int =  47492  
  select ISNULL(tov.koef_pvz,1), tov.id_tov_pvz ,  
   q_plan_pr * ISNULL(tov.koef_pvz,1),  
   q_min_ost * ISNULL(tov.koef_pvz,1),  
   min_ost_tt_tov * ISNULL(tov.koef_pvz,1) ,  
   q_FO * ISNULL(tov.koef_pvz,1),  
   max_ost_tt_tov * ISNULL(tov.koef_pvz,1),  
   q_rashod_fact * ISNULL(tov.koef_pvz,1)  
  **/  
  FROM M2..tt_tov_kontr ttk with (rowlock, INDEX (PK_tt_tov_kontr))   
  inner join M2..tov with ( INDEX(PK_tov))on tov.Number_r = @N and tov.id_tov = ttk.id_tov and tov.id_tov_pvz is not null  
    
  inner join m2..tt on tt.id_TT = ttk.id_tt --and tt.tt_format<>10  
    
  ---kirtoka  
  --and tov.id_tov_pvz not in (select distinct ttk2.id_tov from M2..tt_tov_kontr ttk2 where tov.Number_r = @N)  
  ---   
    
    
  --inner join M2..tt_tov_kontr ttk with (rowlock, INDEX (PK_tt_tov_kontr)) on   
 -- ttk.Number_r = @N and tov.id_tov_pvz = ttk.id_tov and tk.id_kontr = ttk.id_kontr  
  
  --inner join M2..tov with ( INDEX(PK_tov))on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
  
  where ttk.Number_r = @N --and ttk.id_tov <> tk.id_tov  
    
  /**  
      
  --declare @N int =  343263  
  update M2..tt_tov_kontr  
  set id_tov = tk.id_tov ,  
  q_plan_pr = q_plan_pr * ISNULL(tov.koef_pvz,1),  
  q_min_ost = q_min_ost * ISNULL(tov.koef_pvz,1),  
  min_ost_tt_tov = min_ost_tt_tov * ISNULL(tov.koef_pvz,1) ,  
  q_FO = q_FO * ISNULL(tov.koef_pvz,1),  
  max_ost_tt_tov = max_ost_tt_tov * ISNULL(tov.koef_pvz,1),  
  q_rashod_fact = q_rashod_fact * ISNULL(tov.koef_pvz,1)  
    
  /**  
  declare @N int =  343263  
  select tk.id_tov ,  
   q_plan_pr * ISNULL(tov.koef_pvz,1),  
   q_min_ost * ISNULL(tov.koef_pvz,1),  
   min_ost_tt_tov * ISNULL(tov.koef_pvz,1) ,  
   q_FO * ISNULL(tov.koef_pvz,1),  
   max_ost_tt_tov * ISNULL(tov.koef_pvz,1),  
   q_rashod_fact * ISNULL(tov.koef_pvz,1)  
  **/  
  FROM M2..tov_kontr tk  with (index(IX_tov_kontr_1) )   
  inner join M2..tt_tov_kontr ttk with (rowlock, INDEX (PK_tt_tov_kontr)) on   
  ttk.Number_r = @N and tk.id_tov_init = ttk.id_tov and tk.id_kontr = ttk.id_kontr  
  
  inner join M2..tov with ( INDEX(PK_tov))on tov.Number_r = @N and tov.id_tov = tk.id_tov and tov.id_tov_pvz is not null  
  
  where tk.Number_r = @N and ttk.id_tov <> tk.id_tov  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200063, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()     
  
**/  
  
  
 -- убираем колво короб =0  
  
 update M2..tov_kontr  
 set Kolvo_korob = 1  
 from M2..tov_kontr tk with (rowlock, index(PK_tov_kontr))  
 where tk.Number_r= @N and Kolvo_korob = 0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200070, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- проставляем корректное rasp_all   
  
 update M2..tov_kontr  
 set rasp_all = case tov.Skladir when 1 then 0 else 1 end  
 from M2..tov_kontr tk with (rowlock , index (PK_tov_kontr))  
 inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=tk.id_tov and tov.Number_r=@N  
 where tk.Number_r = @N  
   
   
 ----------------------------------------- обнулить мин_остатки, если всего товара на кладе менее 70% от того что нужно по плану продаж  
  
  
  
    select ttk.id_tov , SUM(q_plan_pr) q_plan_pr , SUM(q_FO) q_FO  
    into #rq_min_ost1  
    from M2..tt_tov_kontr ttk with ( index(ind1))  
    where ttk.Number_r = @n  
    group by ttk.id_tov  
      
      
  SELECT id_tov , SUM(t.q_ost_sklad) q_ost_sklad   
  into #rq_min_ost2  
  FROM [M2].[dbo].[tov_kontr] t    
  where t.Number_r = @N  
  group by id_tov  
    
    
  update ttk1  
  set q_min_ost = 0  
    
  --declare @n int =45253  
  --select *  
  from M2..tt_tov_kontr ttk1 with (rowlock,index(ind1))  
  inner join  
    #rq_min_ost1 ttk on ttk.id_tov = ttk1.id_tov  
 inner join  #rq_min_ost2 q_ost on  ttk.id_tov = q_ost.id_tov and  0.70 * (ttk.q_plan_pr -  ttk.q_FO) >  q_ost .q_ost_sklad  
 --inner join m2..tt on tt.id_TT = ttk1.id_tt and tt.tt_format<>10  
  where  ttk1.Number_r = @n and q_min_ost>0  
  and ttk1.tt_format_rasp<>10  
    
    
  /**  
  ----------------------------------------- обнулить макс_остатки, если всего товара на складе более 100% от того что нужно по плану продаж +  1 коробка  
    
 -- declare @N int = 49333  
    
  select ttk.Number_r, ttk.id_tov ,   
  SUM( ceiling(q_plan_pr/tk.Kolvo_korob ) * tk.Kolvo_korob   + case when ttk.max_ost_tt_tov=0 then tk.Kolvo_korob  else 0 end ) q_plan_pr , SUM(q_FO) q_FO  
    ,COUNT(distinct  ttk.id_tt ) q_tt  
   into #rq_max_ost1  
    from M2..tt_tov_kontr ttk     
    inner join  M2..tov_kontr tk       
    on tk.Number_r = ttk.Number_r and tk.id_tov_init = ttk.id_tov and tk.id_kontr = ttk.id_kontr  
    where ttk.q_plan_pr>0 and ttk.Number_r = @n  
    group by ttk.id_tov , ttk.Number_r  
      
      
  SELECT t.Number_r, id_tov , SUM(t.q_ost_sklad) q_ost_sklad   
  into #rq_max_ost2  
  FROM [M2].[dbo].[tov_kontr] t    
  where t.rasp_all=1 and t.srok_godnosti<30 and t.Number_r = @N  
  group by id_tov, t.Number_r  
    
    
  create table #rq_max_ost3 (Number_r int,id_tov int)  
    
  insert into #rq_max_ost3  
  select ttk.Number_r, ttk.id_tov  
  from    #rq_max_ost1 ttk   
 inner join  #rq_max_ost2 q_ost on    
 ttk.Number_r = q_ost.Number_r and ttk.id_tov = q_ost.id_tov and  ( ttk.q_FO + q_ost .q_ost_sklad ) >   ttk.q_plan_pr   
  --where  ttk1.Number_r = @n and q_min_ost>0  
  inner join M2..Tovari t on t.id_tov = ttk.id_tov  
  where q_ost .q_ost_sklad>10  
    
  if exists (select * from #rq_max_ost3)  
  begin  
    
  -- обнулить максимальный остаток  
  update ttk  
  set max_ost_tt_tov = 0  
  from M2..tt_tov_kontr ttk (rowlock)  
  inner join #rq_max_ost3 r on ttk.id_tov = r.id_tov and ttk.Number_r = r.Number_r  
  --inner join M2..tt on tt.id_TT = ttk.id_tt and tt.tt_format not in (4,10,14)  
  left join #MCK mck on ttk.id_tt = mck.id_tt_mck  
  where ttk.max_ost_tt_tov>0 and mck.id_tt_mck is null  
  and ttk.tt_format_rasp not in (4,10,12,14)  
    
-- обновить нулевой план  
  update ttk  
  set q_plan_pr = w.Fact  
  from #rq_max_ost3 r   
  inner join M2..w_all w   on w.id_tov = r.id_tov and w.rn=2  
  inner join M2..tt_tov_kontr ttk (rowlock) on ttk.id_tov = r.id_tov and ttk.Number_r = r.Number_r and ttk.id_tt = w.id_tt  
  where (ttk.q_plan_pr=0 ) and w.Fact>0.001  
    
    
  
/**- АК . убрал 21.08 по Письму Максима Федорова  
  -- добавить в распределение магазины, что были в последние 14 дней  
  insert into M2..tt_tov_kontr  
  Select   
       b.[Number_r]  
      ,w.id_tt  
      ,b.[id_tov]  
      ,b.[id_kontr]  
      ,b.[id_kontr_v]  
      ,w.Fact [q_plan_pr]  
      ,b.[q_min_ost]  
      ,isnull(b2.q_FO,b.[q_FO]) q_FO  
      ,b.[cena_pr]  
      ,b.[koef_tt]  
      ,b.[min_ost_tt_tov]  
      ,b.[date_r1]  
      ,b.[date_r2]  
      ,b.[k1]  
      ,b.[k2]  
      ,b.[k3]  
      ,b.[k4]  
      ,b.[k5]  
      ,b.[id_kontr_init]  
      ,b.[date_add_tt_tov_kontr]  
      ,b.[max_ost_tt_tov]  
      ,b.[q_rashod_fact]  
      ,b.[id_zal]  
        
  from #rq_max_ost3 r   
  inner join M2..w_all w   on w.id_tov = r.id_tov and w.rn_2=1  
  inner join M2..raspr_zadanie_tt   rztt on rztt.number_r=r.Number_r and rztt.id_tt=w.id_tt  
  left join M2..tt_tov_kontr ttk   on ttk.id_tov = r.id_tov and ttk.Number_r = r.Number_r and ttk.id_tt = w.id_tt  
  
 -- поиск среди магазинов, с которых выведено, но могут быть остатки  
  left join  
  (  
  select   
    
       r.[Number_r]  
      ,[id_tt]  
      ,ttk.[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_v]  
      , [q_plan_pr]  
      ,0 [q_min_ost]  
      ,ttk.q_FO [q_FO]  
      ,[cena_pr]  
      ,0 [koef_tt]  
      ,0 [min_ost_tt_tov]  
      ,[date_r1]  
      ,[date_r2]  
      ,[k1]  
      ,[k2]  
      ,[k3]  
      ,[k4]  
      ,[k5]  
      ,[id_kontr_init]  
      ,GETDATE() [date_add_tt_tov_kontr]  
      ,0 [max_ost_tt_tov]  
      ,0 [q_rashod_fact]  
      ,[id_zal]  
        
  from #rq_max_ost3 r   
  inner join M2..tt_tov_kontr ttk   on ttk.id_tov = r.id_tov and  -ttk.Number_r = r.Number_r and ttk.q_FO>1   
    
  ) b2 on b2.id_tov = r.id_tov and b2.Number_r = r.Number_r and b2.id_tt = w.id_tt   
    
  inner join  
  (  
  select top 1 with ties  
    
       ttk.[Number_r]  
      ,[id_tt]  
      ,ttk.[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_v]  
      , [q_plan_pr]  
      ,0 [q_min_ost]  
      ,1  [q_FO]  
      ,[cena_pr]  
      ,0 [koef_tt]  
      ,0 [min_ost_tt_tov]  
      ,[date_r1]  
      ,[date_r2]  
      ,[k1]  
      ,[k2]  
      ,[k3]  
      ,[k4]  
      ,[k5]  
      ,[id_kontr_init]  
      ,GETDATE() [date_add_tt_tov_kontr]  
      ,0 [max_ost_tt_tov]  
      ,0 [q_rashod_fact]  
      ,[id_zal]  
        
  from #rq_max_ost3 r   
  inner join M2..tt_tov_kontr ttk   on ttk.id_tov = r.id_tov and ttk.Number_r = r.Number_r    
  order by row_number() over (partition by ttk.id_tov order by  q_plan_pr desc)  
    
  ) b on b.id_tov = r.id_tov and b.Number_r = r.Number_r  
    
    where ttk.Number_r is null and w.Fact>0.001  
    
 **/  
    
    
  end  
    
    
  **/  
    
     
  
  
  
 ---------------- учет ожидаемого товара - распределить,   
 -- а потом направить на самые первые маршруты - этот алгоритм убран 17 .11. 2018 за ненадобностью  
  
 --declare @N int =80357  
 create table #wait (id_tov int)  
  
 insert into #wait  
 SELECT tk.id_tov   
  FROM M2..tov_kontr (rowlock) tk  
 where tk.Number_r = @N --and 1=0  
 and tk.rasp_all=1 -- только для нескладируемого товара, который под 0 распределяется  
 group by tk.id_tov  
 having SUM( floor( q_ost_sklad / tk.Kolvo_korob ) ) >0 and SUM( floor(q_wait_sklad / tk.Kolvo_korob)) >0 -- есть и остаток и ожидаемый  
   
 --and MIN(case when q_ost_sklad > 0 then tk.id_kontr end) <>  
 --Max(case when q_wait_sklad > 0 then tk.id_kontr end) -- есть контрагент, по которыму ожидаемый не равен остатку  
 -- может быть ситуация, что один и тот же контаргент и на складе и в ожидаемом  
  
 -- добавить к остатку на складе для распределения ожидаемый товар  
 update m2..tov_kontr  
 set q_ost_sklad = tk.q_ost_sklad + tk.q_wait_sklad  
 from m2..tov_kontr tk (rowlock)  
 inner join #wait w on w.id_tov = tk.id_tov  
 where tk.Number_r = @N   
  
 --------------------------------------------   
  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 3, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 delete M2..raspr_hystory  
  FROM M2..raspr_hystory with (index (ind1_h) )  
 where Number_r=@N   
  
 delete M2..rasp_smena_kontr  
  FROM M2..rasp_smena_kontr with (index (ind1) )  
 where Number_r=@N   
  
 delete M2..prev_Group_raspr  
  FROM M2..prev_Group_raspr   
 where number_r=@N   
    
   
  
  
  
   
  
 -- обновляем id_kontr_init в tt_tov_kontr  
/**  
 update M2..tt_tov_kontr  
 set id_kontr_init = tk.id_kontr  
 from M2..tt_tov_kontr tk with (rowlock,index (ind1))  
 where tk.Number_r= @N  
**/  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 10, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 /**  
 -- наполнить тестовыми товарами для распределения  
  
 insert into [M2].[dbo].[Raspr_zadanie_tov]  
  ([Number_r]  
  ,[id_tov]  
  ,[id_kontr])  
 select top 50 tk.Number_r , tk.id_tov, tk.id_kontr  
 from M2..tov_kontr   tk   
 where tk.Number_r=@N  
 **/  
  
 -- добавляем в план продаж заказанные хозтовары  
 --declare @date_rasp date = {d'2015-05-14'}  
 create table #zht ( id_tt int, id_tov int , колво int )  
  
 if exists (select *  
 from m2..tt_tov_kontr ttk with ( index (ind1))  
 inner join M2..tovari t on ttk.id_tov = t.id_tov   
 where ttk.Number_r=@N and t.hoz_tovar =1)  
 begin  
  
  
/**  
 select  id_tt , id_tov , колво колво  
 from M2..Zakaz_Hoz_Tovari zht  
 where zht.date_ch = {d'2017-06-29'}DATEADD(day , -1, @date_rasp )  
 group by  id_tt , id_tov  
**/  
  
-- добавить не только сегодня ( DATEADD(day , -1, @date_rasp )) размещенные заказы, но и все что не выполнено за 2 дня.  
insert into #zht  
select a.id_tt , a.id_tov , MAX(a.колво) колво  
from   
(  
 select zht.id_tt , zht.id_tov , zht.колво  - master.dbo.maxz(0,ISNULL(f.q,0)) колво  
 from M2..Zakaz_Hoz_Tovari zht  
 left join   
 (  
   
 select CONVERT(date, td.closedate) date_f , tt.id_TT ,td.id_tov , SUM(td.Quantity * t_o.znak) q  
 from SMS_REPL.dbo.TD_move AS td   
  inner join M2..tt on tt.N=td.ShopNo_rep  
  INNER JOIN  SMS_REPL.dbo.Types_Operation AS t_o     
   ON td.operation_type = t_o.code_operation   
    AND t_o.table_operation = 'Td_move'   
    AND t_o.field_operation = 'operation_type_orig'  
    WHERE (td.operation_type IN (400, 401))   
     AND (ISNULL(td.Confirm_type, 0) IN (1))   
     AND (CONVERT(date, td.closedate) >= CONVERT(date, DATEADD(DAY,- 2, GETDATE())))  
    group by CONVERT(date, td.closedate) , tt.id_TT , td.id_tov  
     
    union all  
     
    SELECT date_ch, id_tt_cl AS id_tt, id_tov_cl AS id_tov  
  , CONVERT(int, SUM(CASE OperationType_cl WHEN 700 THEN abs(Quantity) ELSE - abs(Quantity) END)) AS колво  
    FROM SMS_IZBENKA.dbo.CheckLine AS chl WITH (INDEX (IX_CheckLine_13) )  
    WHERE (OperationType_cl IN (400, 401)) AND (date_ch >= CONVERT(date, DATEADD(DAY, - 2, GETDATE())))  
    GROUP BY date_ch, id_tt_cl, id_tov_cl  
     
 ) f on f.date_f = dateadd(day,1,zht.date_ch) and f.id_tov = zht.id_tov and f.id_TT = zht.id_tt  
 where zht.date_ch = DATEADD(day , -3, @date_rasp )  
and (zht.колво  - ISNULL(f.q,0))>0 --and ISNULL(f.q,0)>0  
  
   union all  
     
 select zht.id_tt , zht.id_tov , zht.колво  - master.dbo.maxz(0,ISNULL(f.q,0))  
 from M2..Zakaz_Hoz_Tovari zht  
 left join   
 (  
   
 select CONVERT(date, td.closedate) date_f , tt.id_TT ,td.id_tov , SUM(td.Quantity * t_o.znak) q  
 from SMS_REPL.dbo.TD_move AS td   
 inner join M2..tt on tt.N=td.ShopNo_rep  
 INNER JOIN  
   SMS_REPL.dbo.Types_Operation AS t_o   ON td.operation_type = t_o.code_operation AND t_o.table_operation = 'Td_move' AND   
   t_o.field_operation = 'operation_type_orig'  
  WHERE (td.operation_type IN (400, 401)) AND (ISNULL(td.Confirm_type, 0) IN (1)) AND (CONVERT(date, td.closedate) >= CONVERT(date, DATEADD(DAY,   
   - 2, GETDATE())))  
   group by CONVERT(date, td.closedate) , tt.id_TT , td.id_tov  
     
   union all  
     
    SELECT date_ch, id_tt_cl AS id_tt, id_tov_cl AS id_tov, CONVERT(int, SUM(CASE OperationType_cl WHEN 700 THEN abs(Quantity) ELSE - abs(Quantity)   
   END)) AS колво  
  FROM SMS_IZBENKA.dbo.CheckLine AS chl WITH (INDEX (IX_CheckLine_13) )  
  WHERE (OperationType_cl IN (400, 401)) AND (date_ch >= CONVERT(date, DATEADD(DAY, - 2, GETDATE())))  
  GROUP BY date_ch, id_tt_cl, id_tov_cl  
     
 ) f on f.date_f = dateadd(day,1,zht.date_ch) and f.id_tov = zht.id_tov and f.id_TT = zht.id_tt  
 where zht.date_ch = DATEADD(day , -2, @date_rasp )  
and (zht.колво  - ISNULL(f.q,0))>0 --and ISNULL(f.q,0)>0  
   
    
 union all   
   
 select  id_tt , id_tov , колво  
 from M2..Zakaz_Hoz_Tovari zht  
 where zht.date_ch = DATEADD(day , -1, @date_rasp )  
   
 ) a  
group by a.id_tt , a.id_tov   
   
  
    
  
  
 update m2..tt_tov_kontr with (rowlock)  
 set q_plan_pr = zht.колво * tk.Kolvo_korob , koef_tt=0 , q_min_ost = 0 ,  
 date_r1 = @date_rasp , date_r2 = DATEADD(day,1,@date_rasp) , q_FO=0  
 from m2..tt_tov_kontr ttk with (index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) )   
 on ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
 inner join #zht zht on zht.id_tov=ttk.id_tov and zht.id_tt=ttk.id_tt  
 where ttk.Number_r=@N  
  
 end  
  
  
 update M2..tov_kontr with (rowlock)  
 set q_ost_sklad = 0  
 from M2..tov_kontr with (rowlock , index (IX_tov_kontr_1))  
 where Number_r=@N and q_ost_sklad<0  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 20, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- здесь поменять знак всем товарам в M2..tt_tov_kontr, которые не нужно рапределять  
   
 ---vert_automacon Добавлен костыль временный, чтобы не было дублей выбираются строки уже с измененным знаком и исключаются из замены(  
 --select id_tov, id_tt, number_r into #negative_ttk from m2..tt_tov_kontr #negative_ttk where Number_r = -@N   
 -------  
  
    -- АК - новое обновление max_ost_tt_tov - для тов_тт, попавших в ZC_tt_tov  
    --declare @N int = 66832   
      
      
    update ttk  
    set q_plan_pr=0 , Number_r = - ttk.Number_r  
    from M2..tt_tov_kontr ttk with (index(ind1))  
    inner join M2..tov_kontr tk   on ttk.Number_r = tk.Number_r and ttk.id_tov = tk.id_tov and ttk.id_kontr = tk.id_kontr  
    inner join [M2].[dbo].[ZC_tt_tov] zc   on zc.id_tov = ttk.id_tov and zc.id_tt = ttk.id_tt and zc.status=3  
    where ttk.Number_r=@n and ttk.min_ost_tt_tov=0  
      
      
      
  insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 21, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
  
    --------------------------------------------------------------   
     
     
   While 1=1  
    BEGIN  
    BEGIN TRY  
  
     
 update m2..tt_tov_kontr  
 set Number_r = -ttk.Number_r  
 from m2..tt_tov_kontr ttk with (index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
 --left join #negative_ttk negative_ttk  on ttk.id_tov=negative_ttk.id_tov and negative_ttk.Number_r=-@N  
 --left join M2..Raspr_zadanie_tov   rz on rz.id_tov=ttk.id_tov and rz.id_kontr=ttk.id_kontr_v   
 --and rz.Number_r=@N  
 where ttk.Number_r=@N and  
 (--rz.Number_r is null or   
  ttk.date_r1<>@date_rasp or   
  ttk.date_r1 is null or   
  ttk.date_r2 is null or isnull(ttk.q_plan_pr,0)=0   
  )    
  ------vert удалить с верхним костылем  
  --and negative_ttk.Number_r is null  
  --drop table #negative_ttk  
  ---------  
  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 30032, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 30032 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 30, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  create table #t (id_tov int )  
  insert into #t  
  SELECT distinct [id_tov]  
  FROM [Reports].[dbo].[tovar_temp_regim]  
  where id_TempRegim =1  
  
 --------------------------------------------------------------------------------------------------  
  
 -- снять максимальные остатки в отгрузки 6,7,8 Марта  
 /**  
 Торт Киевский 12 249  
 Торт Монтблан 15 370  
 Торт Пралине Роял 15 371  
 **/  
/**  
 update m2..tt_tov_kontr  
 set max_ost_tt_tov = 0   
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 where ttk.Number_r=@N and max_ost_tt_tov>0 and ttk.id_tov in (12249,15370,15371) and @date_rasp in ({d'2016-03-06'},{d'2016-03-07'},{d'2016-03-08'})  
**/  
 --------------------------------------------------------------------------------------------------  
  
  
  
 -- поднять максимальный до кванта и минимальный на квант больше максимального  
 --if not CONVERT(date,getdate()) between {d'2015-12-26'} and {d'2015-12-31'}  
 update m2..tt_tov_kontr   
 set max_ost_tt_tov = case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob+1,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 --declare @N int = 22941  
 --select ttk.max_ost_tt_tov , tk.Kolvo_korob ,  
 --case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
   
 --left join #MCK mck on ttk.id_tt = mck.id_tt_mck  
      
    --inner join m2..tt on tt.id_TT = ttk.id_tt and tt.tt_format<>10  
    
 where -- mck.id_tt_mck is null and   
 ttk.Number_r=@N   
 and max_ost_tt_tov <> case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob+1,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 and ttk.tt_format_rasp<>10  
   
 /**  
 else  
 -- значит на все кроме заморозки поднять 2 раза макс, но не меньше 2 кор +1  
 update m2..tt_tov_kontr  
 set max_ost_tt_tov = case when ttk.max_ost_tt_tov>0 then   
 master.dbo.maxz (tk.Kolvo_korob* case when t.id_tov is null then 2 else 1 end +1,  
 ttk.max_ost_tt_tov * case when t.id_tov is null then 2 else 1 end ) else ttk.max_ost_tt_tov end  
 --declare @N int = 22941  
 --select ttk.max_ost_tt_tov , tk.Kolvo_korob ,  
 --case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
 left join #t t on t.id_tov = ttk.id_tov  
   
 left join #MCK mck on ttk.id_tt = mck.id_tt_mck  
    
    
 where mck.id_tt_mck is null and ttk.Number_r=@N   
 and max_ost_tt_tov <>   
 case when ttk.max_ost_tt_tov>0 then   
 master.dbo.maxz (tk.Kolvo_korob* case when t.id_tov is null then 2 else 1 end +1,  
 ttk.max_ost_tt_tov * case when t.id_tov is null then 2 else 1 end ) else ttk.max_ost_tt_tov end  
    **/  
      
 -- drop table #t  
  
 --   
  
  
  
 update m2..tt_tov_kontr  
 set q_min_ost= case when ttk.q_min_ost>0 and ttk.max_ost_tt_tov>0  
 then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.max_ost_tt_tov - tk.Kolvo_korob)) else ttk.q_min_ost end  
 --select ttk.q_min_ost, ttk.max_ost_tt_tov , tk.Kolvo_korob ,   
 --case when ttk.q_min_ost>0 and ttk.max_ost_tt_tov>0  
 --then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.max_ost_tt_tov - tk.Kolvo_korob)) else ttk.q_min_ost end  
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
 --inner join m2..tt on tt.id_TT = ttk.id_tt and tt.tt_format<>10  
 where ttk.Number_r=@N and  
 ttk.q_min_ost <> case when ttk.q_min_ost>0 and ttk.max_ost_tt_tov>0  
 then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.max_ost_tt_tov - tk.Kolvo_korob)) else ttk.q_min_ost end  
    and ttk.tt_format_rasp<>10  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 31, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- здесь заполняем поле q_ost_sklad_calc_calc до полной коробки, если остатки есть в неполных коробках  
 update m2..tov_kontr  
 set q_ost_sklad_calc = tk.q_ost_sklad +   
 case when   
 FLOOR (q_ost_sklad / tk.Kolvo_korob ) - q_ost_sklad / tk.Kolvo_korob<-0.1  
 and FLOOR (q_ost_sklad / tk.Kolvo_korob ) +1 - q_ost_sklad / tk.Kolvo_korob>0  
 then  
  (floor(q_ost_sklad / tk.Kolvo_korob ) +1 ) * tk.Kolvo_korob -q_ost_sklad + 0.1  
  else case when FLOOR(tk.q_ost_sklad)<>tk.q_ost_sklad then 0.1 else 0 end end  
    
 from m2..tov_kontr tk with (rowlock , INDEX(IX_tov_kontr_1) )  
 where tk.Number_r= @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 /**  
 select top 14 dateadd(day,- ROW_NUMBER() over (order by date_add),CONVERT(date,r.Date_r ) ) date_sales  
 into #dates  
 from jobs..Jobs_log    
 inner join M2..Raspr_zadanie   r on r.Number_r=@N  
 **/  
 --insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 ---select @id_job , 47, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 --select @getdate = getdate()   
  
 -- посчитать продажи с учетом потерянных  
  
 --declare @n int = 37584 , @id_job int = 77 , @date_rasp date = {d'2016-12-24'} , @getdate datetime = getdate()  
  
  
 create table #ttk_tt_tov_0 (id_tt int , id_tov int , q_plan_pr real)  
 insert into #ttk_tt_tov_0  
 select ttk.id_tt , ttk.id_tov ,q_plan_pr  
 from M2..tt_tov_kontr ttk with ( index (ind1))  
 where ttk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40010, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
  
 create table #ttk_tt_tov (id_tt int , date_tk date)  
  
   
 insert into #ttk_tt_tov  
 select distinct ttk.id_tt ,  d.days  
 from #ttk_tt_tov_0 ttk   
 inner join   
 (select DATEADD(day,- row_number() over (order by time_add) +1, @date_rasp ) days  
 from M2..Raspr_zadanie   r  
 ) d on d.days> = DATEADD(day,-31,@date_rasp )   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40020, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
 ---- а вот здесь выбор данных из предрасчитанных данных в vv03 таблица vv03..w_all  
  
 -- если там данные, есть то забрать уже готовые #w и #w_all.   
 -- причем две соритровки - по коэф с учетом кож недели, а второй просто по данным, чтоб перемножить на коэф плана  
 -- в  vv03..w_all уже собрано по аналогам и есть статистика по комплектам  
 -- также добавлены минимальные планы, если не хватает данных  
  
  
 -- выбрать вторую большую продажу  
 create table #w (id_tt int, id_tov int , Fact real)  
 -- заберем из расчитанной в m2..w_all  
  
 -- сделать матрицу по номерам более 2 по убыванию  
 create table #w_all (id_tt int, id_tov int , Fact real , rn int)  
 -- заберем из расчитанной в m2..w_all  
  
 -- create table #dtt (id_tt int , id_tov int , date_tt date , q real , week_tt int)  
 -- таблицу #dtt не считаем  
  
 create table #dtt_z (id_tt int , date_tt date , sum_z int )  
 -- #dtt_z считаем, тк   
  
  
  Declare @ttk as nchar(36)   
  select @ttk= replace(convert(char(36),NEWID()) , '-' , '_')  
    
  declare @strТекстSQLЗапроса as nvarchar(max)  
    
 SET @strТекстSQLЗапроса = '  
  select ttk.id_tt , ttk.date_tk  
  into Temp_tables..[' + @ttk + '] ' +  
  'from #ttk_tt_tov ttk  
  
  EXEC( ''  
  select * into Temp_tables.dbo.[' + @ttk + '] from [SRV-SQL01].Temp_tables.dbo.[' + @ttk + ']   
  create unique clustered index ind1 on Temp_tables.dbo.[' + @ttk + '] (date_tk,id_tt)  
    
  '') at [SRV-SQL06]  
  
 insert into #dtt_z  
 exec ( ''  
  
 select dtt.id_tt , dtt.date_tt, sum(dtt.summa) Sum_z  
 from  Reports..dt dtt     
 inner join  Temp_tables.dbo.[' + @ttk + '] ttk  
 on ttk.id_tt = dtt.id_tt and ttk.date_tk = dtt.date_tt  
 group by dtt.id_tt , dtt.date_tt  
   
  '') at [SRV-SQL06]  
  
 drop table Temp_tables..[' + @ttk + ']  
 EXEC( '' drop table Temp_tables.dbo.[' + @ttk + ']'') at [SRV-SQL06]   
  
 '  
  
 --print @strТекстSQLЗапроса     
 exec sp_executeSQL @strТекстSQLЗапроса     
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40030, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 create clustered index ind2 on #dtt_z (date_tt , id_tt )  
  
  
    -- выбрать очень низкие продажи и удалить их из статистики  
    delete #dtt_z  
    from #dtt_z d  
    inner join  
    (select d1.id_tt , d1.date_tt   
    from #dtt_z d1  
    inner join #dtt_z d2 on d1.id_tt = d2.id_tt  
    group by d1.sum_z  , d1.id_tt , d1.date_tt   
    having d1.sum_z < 0.2 * MAX(d2.sum_z)  
    )a on a.id_tt = d.id_tt and a.date_tt = d.date_tt  
  
  
 -- убрать не адекватные планы продаж на завтра  
  --/**  
  insert into [M2].[dbo].[Raspr_Err] ([Number_N] ,[Descr_err])  
    
  --declare @n int = 37584  
  select distinct @N , 'удален по ' + rtrim(tt.name_TT) + ' не верный план ' + RTRIM( rtt.plan_z)  
  --, Min(dz.sum_z),  MAX(dz.sum_z) , rtt.plan_z  
  --select distinct dz.id_tt , tt.name_TT , rtt.plan_z  
 from #dtt_z dz   
 inner join M2..raspr_zadanie_tt   rtt on rtt.number_r = @N and dz.id_tt = rtt.id_tt and rtt.plan_z > 0  
  and dz.sum_z>0  
  inner join M2..tt on tt.id_TT = dz.id_tt  
 --where not 1.0 * rtt.plan_z / dz.sum_z between 0.33 and 3  
 group by rtt.plan_z, tt.name_TT  
 having rtt.plan_z> MAX(dz.sum_z)* 3 or rtt.plan_z< 0.3 * min(dz.sum_z)  
    
 -- убрать планы левые  
 update M2..raspr_zadanie_tt  
 set plan_z = 0  
 from M2..raspr_zadanie_tt rtt (rowlock)  
 inner join   
 (select dz.id_tt , tt.name_TT , rtt.plan_z  
 from #dtt_z dz   
 inner join M2..raspr_zadanie_tt   rtt on rtt.number_r = @N and dz.id_tt = rtt.id_tt and rtt.plan_z > 0  
  and dz.sum_z>0  
  inner join M2..tt on tt.id_TT = dz.id_tt  
 --where not 1.0 * rtt.plan_z / dz.sum_z between 0.33 and 3  
 group by dz.id_tt , tt.name_TT , rtt.plan_z  
 having rtt.plan_z> MAX(dz.sum_z)* 3 or rtt.plan_z< 0.3 * min(dz.sum_z)  
 )a on rtt.number_r = @N and a.id_tt = rtt.id_tt  
  
 --**/  
  
   CREATE TABLE #w_all_v  
   (  
 [date_r] [date] NULL,  
 [id_tt] [int] NULL,  
 [id_tov] [int] NULL,  
 [Fact] [real] NULL,  
 [q] [real] NULL,  
 [rn] [int] NULL,  
 [date_add] [datetime] NULL,  
 [rn_2] [int] NULL,  
 [Date_f] [date] NULL  
     )  
  
  
        
      if  (  
      select max(v.date_r)  
      from M2..w_all   v  
      ) <= @date_rasp   
        
      begin  
  
      insert into #w_all_v  
      select v.*   
      from M2..w_all   v  
      inner join   
 (select distinct ttk.id_tov , ttk.id_tt from  #ttk_tt_tov_0 ttk ) ttk on v.id_tov = ttk.id_tov and v.id_tt = ttk.id_tt   
       
      end  
        
      else  
      begin  
       -- взять в 03 сервера  
   
   
     --declare @strТекстSQLЗапроса nvarchar(max) , @date_rasp date = '2019.04.26'   
     Declare @ins as nchar(36)   
     select @ins= replace(convert(char(36),NEWID()) , '-' , '_')  
    
     SET @strТекстSQLЗапроса = '  
   
  select *  
  into Temp_tables..[' + @ins + ']   
  from   
  (select distinct ttk.id_tov , ttk.id_tt from  #ttk_tt_tov_0 ttk ) a  
  
  EXEC( ''select * into vv03.dbo.[' + @ins + ']  from [SRV-SQL01].Temp_tables.dbo.[' + @ins + '] '') at [SRV-SQL03]      
       
  insert into #w_all_v  
      exec(''  
        
  
        
      select v.*   
      from  vv03..w_all   v  
      inner join vv03.dbo.[' + @ins + '] ttk on v.id_tov = ttk.id_tov and v.id_tt = ttk.id_tt   
     and v.date_r =  ''''' + rtrim( @date_rasp)  + '''''  
      '') at [srv-sql03]  
        
      '  
        
      --print @strТекстSQLЗапроса  
      exec sp_executeSQL @strТекстSQLЗапроса    
     
     
   SET @strТекстSQLЗапроса = '  
          
   drop table Temp_tables..[' + @ins + ']  
   EXEC( ''drop table vv03.dbo.[' + @ins + ']'') at [SRV-SQL03]    
       '  
        
      exec sp_executeSQL @strТекстSQLЗапроса    
          
        
        
      end  
        
  
  
  
     create clustered index ind1 on #w_all_v (id_tt ,date_f )  
       
       
 insert into #w   
 --declare @n int = 34880  
 -- нет планов на завтра  
 select v.id_tt , v.id_tov , v.Fact  
 from #w_all_v v  
 left join M2..raspr_zadanie_tt   rtt on rtt.number_r = @N and v.id_tt = rtt.id_tt and rtt.plan_z > 0  
 where v.rn = 2 and rtt.plan_z is null  
 --and v.id_tt =333 and v.id_tov = 417  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40040, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 insert into #w   
 --Declare @N int = 59669  
 -- есть план на завтра  
 select v.id_tt , v.id_tov , 1.0 * rtt.plan_z / dz.sum_z * v.Fact   
 from #w_all_v v   
 inner join M2..raspr_zadanie_tt   rtt on rtt.number_r = @N and v.id_tt = rtt.id_tt and rtt.plan_z > 0  
 inner join #dtt_z dz on v.id_tt = dz.id_tt and v.date_f = dz.date_tt  
 where v.rn = 2 and dz.sum_z * v.Fact <>0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40050, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 insert into #w_all   
 --Declare @N int = 59669  
 -- нет планов на завтра  
 select v.id_tt , v.id_tov , v.Fact , v.rn  
 from #w_all_v v   
 left join M2..raspr_zadanie_tt   rtt on rtt.number_r = @N and v.id_tt = rtt.id_tt and rtt.plan_z > 0  
 where v.rn > 2 and rtt.plan_z is null  
 --and v.id_tt =333 and v.id_tov = 417  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40060, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
        
  
       
 insert into #w_all  
 -- есть план на завтра  
   
 --Declare @N int = 59669  
   
   
 select v.id_tt , v.id_tov , 1.0 * rtt.plan_z / dz.sum_z * v.Fact , v.rn   
 from #w_all_v v   
 inner join M2..raspr_zadanie_tt   rtt on rtt.number_r = @N and v.id_tt = rtt.id_tt and rtt.plan_z > 0  
 inner join #dtt_z dz on v.id_tt = dz.id_tt and v.date_f = dz.date_tt  
 where v.rn > 2 and dz.sum_z * v.Fact <>0  
  
  
  
 CREATE CLUSTERED INDEX ind1  
 ON [dbo].[#w_all] ([id_tt],[id_tov],[rn])  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40070, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
--новый кусок  
  
  
/**  
алгоритм корректировки 2 наибольшей  
в w_all смотрим продажи за последние дни срока годности  
для товара со сроком годности менее 7 дней  
  
если макс менее в 1,5 раза чем 2наибольшая, и менее, чем  остаток_факт,/2 но меняет 2 наибольшую  
статус в status = 7, p3 = макс/2наиб , p4 макс/ост_факт  
в w_all таблицу еще вывести макс за дни  
если более, чем в 1,5 раза больше 2наибольшая  или нет 2 наибольшей, то 2наибольшая  
 и status = 8, p3 = макс/2наиб  
   
   
в итоге результат в #rasp_for_sales_3  Fact real , q_max real, q_fo    
**/  
  
   --drop table #rasp_for_sales  
   create table #rasp_for_sales (id_tt int, id_tov int ,q_fo real, srok_godnosti int , Kolvo_korob real)  
   insert into #rasp_for_sales  
  
  select r.id_tt , r.id_tov , r.q_FO , MAX(tk.srok_godnosti) srok_godnosti, MAX(tk.Kolvo_korob) Kolvo_korob  
  from M2..tt_tov_kontr r    
  inner join M2..tov_kontr tk on tk.Number_r = r.number_r and r.id_tov = tk.id_tov  
  where r.number_r = @N  
  group by r.id_tt , r.id_tov , r.q_FO  
  having  MAX(tk.Kolvo_korob)<=7  
  
  create unique clustered index ind1 on #rasp_for_sales (id_tt , id_tov )  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40071, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   --drop table #rasp_for_sales_2  
   create table #rasp_for_sales_2 (date_r date, id_tt int, id_tov int, q_nuzno real, q_raspr real, q_sales real ,q_zc real , q_fo_fact real , q_fo real , Kolvo_korob real)  
   insert into #rasp_for_sales_2  
     
   --declare @N int = 71366, @date_rasp date = '2018-10-16'  
   SELECT   top 1 with ties  rz.Date_r, r.id_tt , isnull(tov.id_tov_pvz ,r.id_tov) id_tov , r.q_nuzno , r.q_raspr , r.q_sales  ,r.q_zc , r.q_fo_fact , r2.q_fo , r2.Kolvo_korob  
   FROM  M2..archive_Rasp   r   
   INNER JOIN M2..Raspr_zadanie  rz  ON r.Number_r = rz.Number_r  
   inner join m2..tov tov   on tov.Number_r = r.number_r and tov.id_tov = r.id_tov  
   inner join #rasp_for_sales r2 on r2.id_tov = r.id_tov and r2.id_tt = r.id_tt  
   --inner join M2..rasp tk   on tk.Number_r = @N and tk.id_tt =r.id_tt and tk.id_tov=r.id_tov  
   where rz.Date_r between DATEADD(day,-3,CONVERT(date,getdate())) and  DATEADD(day,-1,CONVERT(date,getdate()))  
   order by ROW_NUMBER() over (partition by rz.Date_r , r.id_tt , isnull(tov.id_tov_pvz ,r.id_tov) order by r.Number_r desc)   
     
   create unique clustered index ind1 on #rasp_for_sales_2 ( id_tt , id_tov , Date_r )  
     
     
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40072, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   create table #rasp_for_sales_3 ( id_tt int, id_tov int, Fact real, q_max real, q_fo real)  
   insert into #rasp_for_sales_3  
     
   select w.id_tt , w.id_tov , w.Fact , a.q_max , a.q_fo  
   from #w w  
   left join (  
   select id_tt , r.id_tov ,  max(q_sales - q_zc) q_max, MIN(q_fo_fact) min_ost , r.q_fo , r.Kolvo_korob  
   from #rasp_for_sales_2 r  
   group by id_tt , r.id_tov , r.q_fo , r.Kolvo_korob  
   having COUNT(*)=3 and MIN(q_fo_fact)>0.1  
   ) a on a.id_tov = w.id_tov and a.id_tt = w.id_tt  
   where  w.Fact>a.q_max*1.5  
   and q_fo>Kolvo_korob *0.5  
     
     
   create unique clustered index ind1 on #rasp_for_sales_3 ( id_tt , id_tov  )  
     
     
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 40073, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   ------------------------------------------------------------------------------------------  
     
     
     
 --**/  
  
  
 --create index ind1 on #w (id_tt , id_tov )  
  
  
  
 -- обнулить все ограничения по МаксОстатку на более 40% ТТ по каждому товару.  
 --Declare @N int = 24973  
   
    if OBJECT_ID('tempdb..#ttk_max_ost_tt_tov') is not null drop table #ttk_max_ost_tt_tov  
  
  
 select ttk.Number_r , ttk.id_tt , ttk.id_tov  
 into #ttk_max_ost_tt_tov  
 from m2..tt_tov_kontr ttk with ( index (ind1))  
   
    --left join #MCK mck on ttk.id_tt = mck.id_tt_mck   
   
 inner join  
 (  
 select ttk.id_tov , ttk.tt_format_rasp tt_format , ttk.id_tt , ISNULL( rs.q_max ,ISNULL(w.Fact , ttk.q_plan_pr)) fact ,  
 ROW_NUMBER() over (partition by ttk.id_tov, ttk.tt_format_rasp order by ISNULL(w.Fact , ttk.q_plan_pr)) rn   
 from m2..tt_tov_kontr ttk with ( index (ind1))  
   inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N and tk.rasp_all=1  
 --inner join M2..tt on tt.id_TT = ttk.id_tt  
 left join #w w on w.id_tov = ttk.id_tov and w.id_tt = ttk.id_tt  
   
 left join #rasp_for_sales_3 rs on rs.id_tov = ttk.id_tov and rs.id_tt = ttk.id_tt  
   
 where ttk.Number_r=@N and ttk.max_ost_tt_tov > 0   
 )a on a.id_tt = ttk.id_tt and a.id_tov = ttk.id_tov  
 inner join   
 (select ttk.id_tov , ttk.tt_format_rasp tt_format , convert(int,0.4 *count(ttk.id_tt) ) макс_макс  
 from m2..tt_tov_kontr ttk with ( index (ind1))  
 --inner join M2..tt on tt.id_TT = ttk.id_tt  
 where ttk.Number_r=@N   
 group by ttk.id_tov , ttk.tt_format_rasp )b on a.id_tov = b.id_tov and a.tt_format = b.tt_format  
 where --mck.id_tt_mck is null and   
 ttk.Number_r=@N and a.rn > b.макс_макс  
 and ttk.max_ost_tt_tov<>0  
 and a.tt_format not in (4,10,12,14)  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 4800001, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
 update m2..tt_tov_kontr   
 set max_ost_tt_tov = 0  
 from m2..tt_tov_kontr ttk with (rowlock)  
 inner join  #ttk_max_ost_tt_tov ttk1 on ttk.Number_r = ttk1.Number_r  and ttk.id_TT = ttk1.id_TT and ttk.id_tov = ttk1.id_tov  
   
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 49, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 create table #rasp_1 (id_tt int , id_tov int, Fact int )  
 CREATE unique CLUSTERED INDEX ind1 ON #rasp_1 (id_tt , id_tov)  
  
   
  
  
 insert into #rasp_1   
 select ttk.id_tt , ttk.id_tov , isnull(rs.q_max ,isnull( w.fact ,ttk.q_plan_pr)) fact  
 from #ttk_tt_tov_0 ttk   
 left join #w w   on ttk.id_tt = w.id_tt and ttk.id_tov = w.id_tov  
 left join #rasp_for_sales_3 rs on rs.id_tov = ttk.id_tov and rs.id_tt = ttk.id_tt  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 53, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 --declare @n int = 15446 , @date_rasp date = {d'2015-05-08'}  
 -- таблица со вторыми большими продажами  
 /**   
 insert into #rasp_1   
 Select b.id_tt , b.id_tov , b.Fact  
 from   
 (select a.* , dn2.ПроцДн пд2, dn.ПроцДн , dates.date_sales ,   
 isnull(dtt.quantity - dtt.discount50_qty - dtt.discount50_sms_qty + isnull(ls.lost1,0) *   
 isnull(dn2.ПроцДн / case dn.ПроцДн when 0 then 1.0/7 else dn.ПроцДн end ,1) , a.q_plan_pr) Fact ,  
 ROW_NUMBER() over (partition by a.id_tt , a.id_tov  
 order by isnull(dtt.quantity - dtt.discount50_qty - dtt.discount50_sms_qty + isnull(ls.lost1,0) *   
 isnull(dn2.ПроцДн / case dn.ПроцДн when 0 then 1.0/7 else dn.ПроцДн end ,1) , a.q_plan_pr) desc  
  ) rn  
 from   
 (  
  
 select ttk.id_tt , ttk.id_tov , ttk.q_plan_pr  
 from M2..tt_tov_kontr ttk with ( index (ind1))  
 where ttk.Number_r=@N --and tk.rasp_all=0   
 ) a  
  
 inner join #dates dates on 1=1  
  
 left join #dn dn on dn.id_tt=a.id_tt and dn.dn = DATEPART(weekday,dates.date_sales)  
  
 left join #dn dn2 on dn2.id_tt=a.id_tt and dn2.dn = DATEPART(weekday,@date_rasp)  
  
  
 left join Reports..dtt   dtt on dtt.date_tt=dates.date_sales  
 and dtt.id_tt= a.id_tt and dtt.id_tov=a.id_tov and dtt.date_tt not in ({d'2014-12-30'},{d'2014-12-31'},{d'2015-05-12'},{d'2015-05-13'})  
 left join M2..Lost_sales   ls on ls.date_ls=dates.date_sales  
 and ls.id_tt_ls= a.id_tt and ls.id_tov_ls=a.id_tov   
 and ls.date_ls not in ({d'2014-12-30'},{d'2014-12-31'},{d'2015-05-12'},{d'2015-05-13'})  
 ) b  
 where b.rn=2  
  
 -- drop table #dates  
 **/  
  
 --b.id_tov=559 and b.id_tt=509  
  
 ---- drop table #dtt  
 -- drop table #dtt_z  
 -- drop table #w  
 -- drop table #ttk_tt_tov  
 -- drop table #ttk_tt_tov_0  
  
  
 ------------------------------------------------------------------------------------------------  
 -- если есть ПланПродаж на одной характеристике, но остатки = 0 - то перекинуть на любую другую, на которой есть остатки  
  
 select  distinct r.id_tov , r.id_kontr , b.id_kontr id_kontr_2  
 into #add_r1_2  
  from M2..tt_tov_kontr r with ( index (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 inner join  
 (   
   
 Select tk.id_tov , tk.id_kontr ,  
 ROW_NUMBER() over (partition by tk.id_tov  order by tk.q_ost_sklad_calc desc) rn  
  from  M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )  
 where tk.number_r = @N and tk.q_ost_sklad_calc>0  
   
 ) b on b.rn = 1 and r.id_tov = b.id_tov   
 where r.number_r = @N and r.q_plan_pr > 0 and tk.q_ost_sklad_calc<=0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 54, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tt_tov_kontr with (rowlock)  
 set id_kontr = a.id_kontr_2  
  from #add_r1_2 a    
  inner join M2..tt_tov_kontr r with (index (PK_tt_tov_kontr))   
  on r.id_tov = a.id_tov and r.id_kontr = a.id_kontr and r.number_r = @N   
    
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
    Select @N , 0 , id_tov ,  id_kontr_2 ,id_kontr , 54   
    from #add_r1_2   
  
 -- drop table #add_r1_2  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 55, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- если не нужна характеристика, но есть осткатки - перенести на самую с большим планом  
 select r.id_tov , r.id_kontr , b.id_kontr id_kontr_2  
 into #add_r2_2  
  from M2..tt_tov_kontr r with ( index (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 inner join  
 (Select r.id_tov , r.id_kontr ,  
 ROW_NUMBER() over (partition by r.id_tov , r.id_kontr order by sum(r.q_plan_pr) desc) rn  
  from M2..tt_tov_kontr r with ( index (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 where r.number_r = @N and tk.q_ost_sklad_calc>0  
 group by r.id_tov , r.id_kontr  
 having sum(r.q_plan_pr)>0  
 ) b on b.rn = 1 and r.id_tov = b.id_tov  
 where r.number_r = @N and tk.q_ost_sklad_calc>0   
 group by r.id_tov , r.id_kontr , b.id_kontr   
 having sum(r.q_plan_pr) = 0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 56, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 update M2..tt_tov_kontr with (rowlock)  
 set id_kontr = b.id_kontr_2  
 from M2..tt_tov_kontr r with (index (ind1))  
 inner join #add_r2_2 b on r.id_tov = b.id_tov and r.id_kontr = b.id_kontr  
 where r.number_r = @N   
  
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
    Select @N , 0 , id_tov ,  id_kontr_2 ,id_kontr , 56   
    from #add_r2_2   
      
 -- drop table #add_r2_2  
  
 ------------------------------------------------------------------------------------------------  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 57, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- drop table #dn  
   
   
 -- проставить в план продаж наибольшее из вторую наибольшей, плана продаж и минимального остатка для складируемого товара  
 -- в случае если есть отрицат коэф на товар, то выставить план продаж * (1+коэф)  
 select ttk.id_tt , ttk.id_tov , master.dbo.maxz(master.dbo.maxz(r.Fact,ttk.q_plan_pr)  
 * case when ISNULL(tov.koef_tov,0)<=5 then (1+tov.koef_tov) else 1 end , ttk.q_min_ost ) q_new  
 into #ttk1  
 from m2..tt_tov_kontr as ttk with ( index(ind1))  
 inner join #rasp_1 r on ttk.id_tov=r.id_tov and ttk.id_tt = r.id_tt   
 inner join m2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on ttk.id_tov=tk.id_tov and ttk.id_kontr=tk.id_kontr and tk.Number_r=@N  
 left join M2..tov   on tov.id_tov = ttk.id_tov and tov.Number_r = @N  
 --inner join M2..tt on tt.id_TT = ttk.id_tt and tt.tt_format not in (7,10)  
 where ttk.Number_r=@N   
 and tk.rasp_all=0 and q_plan_pr <> master.dbo.maxz(master.dbo.maxz(r.Fact,ttk.q_plan_pr)   
 * case when ISNULL(tov.koef_tov,0)<=5 then (1+tov.koef_tov) else 1 end ,ttk.q_min_ost )  
    and ttk.tt_format_rasp not in (7,10)  
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 10571, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update m2..tt_tov_kontr  
 set q_plan_pr = ttk1.q_new  
 from m2..tt_tov_kontr ttk with (index (PK_tt_tov_kontr))  
    inner join #ttk1 ttk1 on ttk.Number_r=@N and ttk.id_tt = ttk1.id_tt and ttk.id_tov = ttk1.id_tov  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 10572, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 update m2..rasp  
 set q_ko_ost = r.q_ko_ost + r.q_plan_pr - ttk1.q_new  
 from m2..rasp r with (rowlock,index (ind1))  
 inner join #ttk1 ttk1 on r.id_tt = ttk1.id_tt and r.id_tov = ttk1.id_tov   
 where r.number_r = @N  
  
  
 -- drop table #ttk1  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 10573, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 --------------------------------------------------------------------------------------------------  
 -- в случае если есть положит коэф на товар, то выставить мин остаток, как план продаж * (1 + коэф)  
  
 update m2..tt_tov_kontr  
 set q_min_ost = master.dbo.maxz(ttk.q_min_ost , ttk.q_plan_pr * case when ISNULL(tov.koef_tov,0)<=5 then (1+tov.koef_tov) else 1 end )  
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 inner join M2..tov with ( index(IX_tov_1)) on tov.id_tov = ttk.id_tov and tov.Number_r = @N  
 inner join m2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on ttk.id_tov=tk.id_tov and ttk.id_kontr=tk.id_kontr and tk.Number_r=@N  
 --inner join M2..tt on tt.id_TT = ttk.id_tt and tt.tt_format not in (7,10)  
 where ttk.Number_r=@N and tov.koef_tov>-1 and tk.rasp_all=1  
    and ttk.tt_format_rasp not in (7,10)  
 --------------------------------------------------------------------------------------------------  
  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1057, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 declare @j1 int =1  
  
  
  
   /**  
 create table #ttk2 (id_tt int , id_tov int , q_new real)  
  
 create table #tov_dolya (id_tov int , СредДоля real)  
 create table #a (id_tov int, id_kontr int, id_tt int, q_nuzno real , Kolvo_korob real)  
 create table #b (id_tov int, id_kontr int, id_tt int, q_nuzno real , Kolvo_korob real , q_FO real, q_max_ost real , id_kontr_init int)  
 create clustered index  ind1 on #a (id_tov)  
 create clustered index ind1 on #b (id_tov)  
   
 /**  
 create table #rasp_tov_kontr (id_tov int, id_kontr int, q_nuzno real, q_ost_sklad_calc real  
 , Kolvo_korob real , id_kontr_init int, id_tt int  
  ,  [id] [bigint] IDENTITY(1,1) NOT NULL PRIMARY KEY , q_FO real, q_max_ost real, ww int)  
  **/  
    
 create table #rasp_tov_kontr (id_tov int  , id_kontr int, q_nuzno real, q_ost_sklad_calc real  
 , Kolvo_korob real , id_kontr_init int, id_tt int  
 , q_FO real, q_max_ost real, ww int)  
   create clustered index ind1 on   #rasp_tov_kontr (id_tov)  
   
 --select top 0 1 id_tov , convert(real,0.01) Избыток, convert(real,0.01) Нехватка  
 create table #id_tov_perebros (id_tov int ,Избыток real , Нехватка real )  
  
 --select top 0 1 id_tov , 1 id_kontr , convert(real,0.01) q_nuzno ,   
 --convert(real,0.01) Ost , convert(real,0.01) Избыток, convert(real,0.01) Нехватка  
 create table #id_tov_kontr_perebros (id_tov int , id_kontr int, q_nuzno real  
 ,Ost real , Избыток real , Нехватка real )  
      
  
 create table #id_tov_perebros_init (id_tov int ,Избыток real , Нехватка real )  
  
 --select top 0 1 id_tov , 1 id_kontr , convert(real,0.01) q_nuzno ,   
 --convert(real,0.01) Ost , convert(real,0.01) Избыток, convert(real,0.01) Нехватка  
 create table #id_tov_kontr_perebros_init (id_tov int , id_kontr int, q_nuzno real  
 ,Ost real , Избыток real , Нехватка real )  
   
 --select top 0 1 id_tov , 1 id_kontr_1 ,1 id_kontr_2 ,1 id_tt_1 , 1 id_tt_2, convert(real,0.01) q_nuzno_1  
 --into #perebros  
 create table #perebros (id_tov int , id_kontr_1 int, id_kontr_2 int ,id_tt_1 int , id_tt_2 int ,q_nuzno_1 real , napr_smena int)  
  
 create table #add_r2 (id_tov int , id_kontr int , id_kontr_2 int, rn int , id_tt int)  
 create table #add_r1 (id_tov int , id_kontr int , id_kontr_2 int)  
 **/  
  
  
 -- в два приема сделать   
 while @j1<=2  
  
 begin  
  
--declare @n int = 35479,@id_job int = 2345 , @getdate datetime = getdate() , @i int = 1 , @date_rasp date = {d'2016-08-29'}  
  
 delete M2..rasp   
  FROM M2..rasp with (rowlock , INDEX (ind1))  
 where Number_r=@N   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 58, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
/**  
if not exists(select * from jobs..jobs as j with    
 where date_add>=CONVERT(date,getdate()) and job_name like 'm2..make_rasp_new%' and date_exc is  null and type_exec=1   
   and j.id_job<>@id_job  )  
begin try  
 ALTER INDEX ALL ON m2..tt_tov_kontr REBUILD   
 ALTER INDEX ALL ON m2..tov_kontr REBUILD  
 ALTER INDEX ALL ON m2..tov REBUILD  
   
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 581, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
  
end try  
begin catch  
 insert into jobs..error_jobs(job_name, message, number_step)  
 select 'm2..make_rasp_new', ERROR_MESSAGE(), 0  
end catch  
**/  
  
  
    -- АК - новое обновление max_ost_tt_tov - для тов_тт, попавших в ZC_tt_tov  
    --declare @N int = 66832   
      
    update ttk  
    set max_ost_tt_tov = tk.Kolvo_korob * (1 + isnull(zc.koef_ost,0)) --, min_ost_tt_tov = 0 , q_min_ost=0  
    from M2..tt_tov_kontr ttk with (index(ind1))  
    inner join M2..tov_kontr tk   on ttk.Number_r = tk.Number_r and ttk.id_tov = tk.id_tov and ttk.id_kontr = tk.id_kontr  
    inner join [M2].[dbo].[ZC_tt_tov] zc   on zc.id_tov = ttk.id_tov and zc.id_tt = ttk.id_tt and zc.status=2  
      
    --left join #MCK mck on ttk.id_tt = mck.id_tt_mck   
          
    where ttk.Number_r=@n --and mck.id_tt_mck is null-- and ttk.min_ost_tt_tov=0  
    and @date_rasp not in ('2018-12-29','2018-12-30')  
  
    update ttk  
    set min_ost_tt_tov = 0 , q_min_ost=0  
    from M2..tt_tov_kontr ttk with (index(ind1))  
    inner join M2..tov_kontr tk   on ttk.Number_r = tk.Number_r and ttk.id_tov = tk.id_tov and ttk.id_kontr = tk.id_kontr  
    inner join [M2].[dbo].[ZC_tt_tov] zc   on zc.id_tov = ttk.id_tov and zc.id_tt = ttk.id_tt and zc.status=5  
      
    --left join #MCK mck on ttk.id_tt = mck.id_tt_mck   
    -- inner join m2..tt on tt.id_TT = ttk.id_tt and tt.tt_format<>10  
          
    where ttk.Number_r=@n --and mck.id_tt_mck is null-- and ttk.min_ost_tt_tov=0  
    and ttk.tt_format_rasp not in (10)    
          
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 58077, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
     
  
  
 -- расчет нужно для товара складируемого  
   
 if OBJECT_ID('tempdb..#t_rastr_tmp') is not null drop table #t_rastr_tmp  
    
  select ttk.id_tt , ttk.id_tov , ttk.id_kontr ,   
  q_FO ,   
  master.dbo.maxz( r.Fact, ttk.q_plan_pr ) --+ isnull(ttk.q_zakaz,0)   
  q_plan_pr ,  
  ttk.q_min_ost --+ isnull(ttk.q_zakaz,0)   
  q_min_ost,   
  case when max_ost_tt_tov>0 then max_ost_tt_tov --+ ceiling( isnull(ttk.q_zakaz,0)/ tk.Kolvo_korob  ) *  tk.Kolvo_korob    
  else 0 end [q_max_ost],  
  0 [q_raspr],   
  q_FO -master.dbo.maxz( r.Fact, ttk.q_plan_pr ) --- isnull(ttk.q_zakaz,0)   
  q_ko_ost,   
    
  case when zht.id_tt is null then  
    
  -- теперь максимум между минимальным остатком, планом продаж, второй наибольшей  
  case when ttk.max_ost_tt_tov <= 0 then -- значит нет макс остатка  
  ceiling(master.dbo.maxz(0, master.dbo.maxz( master.dbo.maxz( r.Fact, ttk.q_plan_pr ), ttk.q_min_ost ) - q_FO --+ isnull(ttk.q_zakaz,0)    
   
  /**  
  +  master.dbo.maxz(0 ,   
    case   
    when tov.Skladir_1C <> 1 then 0 -- Значит на самом деле это нескладируемый товар, но просто он распределяется как складир, те СПб или нет поставки  
      
    when @id_zone in (338,4550) then 0.2 else 0.5 end  *  master.dbo.maxz( r.Fact, ttk.q_plan_pr )  )   
  
  --**/    
  )/ case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end   
  
    
  )* case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end  
  else -- значит есть макс остаток, не больше распределить  
  master.dbo.maxz( 0 , master.dbo.minz(  
  ceiling(master.dbo.maxz(0, master.dbo.maxz( master.dbo.maxz( r.Fact, ttk.q_plan_pr ), ttk.q_min_ost ) - q_FO --+ isnull(ttk.q_zakaz,0)  
  )  
  / case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end )* case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end  
  , floor( ( ttk.max_ost_tt_tov - q_FO --+ isnull(ttk.q_zakaz,0)  
  )   
  / case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end )* case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end ))  
  end  
    
  else ttk.q_plan_pr end  q_nuzno ,  
    
  -- здесь первый расчет количество, кратного коробкам, которое нужно, чтобы не было минуса  
    
  0 [sort_ost] , 0 [sort_pr] , @N [number_r], ttk.id_kontr  [id_kontr_init], ttk.id_zal , ttk.q_zakaz , ttk.tt_format_rasp ,ttk.price_rasp, ttk.koef_ost_pr_rasp , ttk.id_tov_pvz_rasp  
  into #t_rastr_tmp  
  FROM M2..tt_tov_kontr ttk with ( index (ind1))   
  inner join m2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on ttk.id_tov=tk.id_tov and ttk.id_kontr=tk.id_kontr and tk.Number_r=@N  
  inner join #rasp_1 r on ttk.id_tov=r.id_tov and ttk.id_tt=r.id_tt  
  inner join M2..tov   tov on tov.Number_r = ttk.Number_r and tov.id_tov = ttk.id_tov  
    
  left join #zht zht on zht.id_tov=ttk.id_tov and zht.id_tt=ttk.id_tt  
    
  --inner join M2..tt on tt.id_TT = ttk.id_tt  
    
  where ttk.Number_r=@N  
  and q_min_ost is not null and q_FO is not null and q_plan_pr is not null  
  and tk.rasp_all=0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 591, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()      
  
   
  
 insert into M2..rasp   
  ([id_tt]  
  ,[id_tov]  
  ,[id_kontr]  
  ,[q_FO]  
  ,[q_plan_pr]  
  ,[q_min_ost]  
  ,[q_max_ost]  
  ,[q_raspr]  
  ,[q_ko_ost]  
  ,[q_nuzno]  
  ,[sort_ost]  
  ,[sort_pr]  
  ,[number_r]  
  ,[id_kontr_init]  
  ,[id_zal]  
  , q_nuzno_init  
      ,zc_status   
      ,[zc_type_add]  
      ,[zc_koef_ost]  
      ,[zc_date_add]  
      ,p3  
      ,p4   
      ,q_zakaz  
      ,tt_format_rasp   
      ,price_rasp  
      ,koef_ost_pr_rasp  
      ,id_tov_pvz_rasp  
  )  
  select   
  t.id_tt ,t.id_tov ,id_kontr , t.q_FO  
  ,q_plan_pr ,q_min_ost ,[q_max_ost]  
  ,[q_raspr], q_ko_ost,  q_nuzno   
  ,[sort_ost] , [sort_pr] , [number_r]  
  , [id_kontr_init], id_zal,  q_nuzno ,  
  
      zc.Status   
   ,[type_add]  
      ,[koef_ost]  
      ,[date_add]  
      ,0 p3 --case when isnull(rs.q_max,0)>0 and rs.Fact / rs.q_max < 100000000 then 100* rs.Fact / rs.q_max end p3 --VERT добавил защиту от переполнения  
      ,0 p4 --case when isnull(rs.q_max,0)>0 and rs.q_fo / rs.q_max < 100000000 then 100* rs.q_fo / rs.q_max end p4 --VERT добавил защиту от переполнения  
      ,t.q_zakaz  
      ,tt_format_rasp  
      ,price_rasp  
      ,koef_ost_pr_rasp  
      ,id_tov_pvz_rasp  
               
  from #t_rastr_tmp t  
  left join M2..ZC_tt_tov zc   on zc.id_tt = t.id_tt and zc.id_tov = t.id_tov  
  left join #rasp_for_sales_3 rs on rs.id_tt = t.id_tt and rs.id_tov = t.id_tov  
    
    
  --drop table #t_rastr_tmp  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 59, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()      
   
 WHILE 1=1  
 BEGIN  
 BEGIN TRY   
  -- расчет нужно для товара нескладируемого  
  insert into M2..rasp  
   ([id_tt]  
   ,[id_tov]  
   ,[id_kontr]  
   ,[q_FO]  
   ,[q_plan_pr]  
   ,[q_min_ost]  
   ,[q_max_ost]  
   ,[q_raspr]  
   ,[q_ko_ost]  
   ,[q_nuzno]  
   ,[sort_ost]  
   ,[sort_pr]  
   ,[number_r]  
   ,[id_kontr_init]  
   ,id_zal  
   ,q_nuzno_init  
         ,[zc_status]  
         ,[zc_type_add]  
         ,[zc_koef_ost]  
         ,[zc_date_add]   
         ,p3  
         ,p4    
         ,q_zakaz    
         ,tt_format_rasp    
         ,price_rasp   
         ,koef_ost_pr_rasp   
         ,id_tov_pvz_rasp  
   )  
     
  
   select ttk.id_tt , ttk.id_tov , ttk.id_kontr , ttk.q_FO , q_plan_pr --+ isnull(ttk.q_zakaz,0)   
   q_plan_pr, q_min_ost --+ isnull(ttk.q_zakaz,0)   
   q_min_ost,   
   case when max_ost_tt_tov>0 then max_ost_tt_tov --+ ceiling(isnull(ttk.q_zakaz,0)/tk.Kolvo_korob) *tk.Kolvo_korob   
   else 0 end  ,  
   0 q_raspr , ttk.q_FO - q_plan_pr -- -  isnull(ttk.q_zakaz,0)   
   q_ko_ost,   
   case   
     
   when ttk.tt_format_rasp =10  -- для всех маркетплейсов , последнюю коробку округляем вниз, кроме первой  
         then  master.dbo.maxz( case when  ttk.q_FO =0 then 1 else 0 end , floor (( ttk.q_min_ost - ttk.q_FO ) / tk.Kolvo_korob +0.01)) * tk.Kolvo_korob   
  
     
   when - (ttk.q_FO -- - isnull(ttk.q_zakaz,0)   
   - master.dbo.maxz(q_plan_pr, ttk.q_min_ost) ) > 0 -- значит есть потребность  
   then   
     
   case when ttk.max_ost_tt_tov <= 0 then -- значит нет макс остатка  
   master.dbo.maxz( case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end ,( - (ttk.q_FO -- - isnull(ttk.q_zakaz,0)   
   -master.dbo.maxz(q_plan_pr, ttk.q_min_ost) )) )   
     
  
              
   else -- значит есть макс остаток, не больше распределить  
     
    
     
   master.dbo.maxz( 0 ,master.dbo.minz(  
   master.dbo.maxz( case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end,( - (ttk.q_FO -- - isnull(ttk.q_zakaz,0)   
   -master.dbo.maxz(q_plan_pr, ttk.q_min_ost) )) )   
   , ttk.max_ost_tt_tov - ttk.q_FO -- + isnull(ttk.q_zakaz,0)  
   ))  
   end  
     
   --case when ttk.q_min_ost - (q_FO ) > 0 -- значит есть потребность  
   --then master.dbo.maxz( tk.Kolvo_korob ,(ttk.q_min_ost - (q_FO )) )  
   else 0 end  q_nuzno ,  
   -- здесь первый расчет количество, кратного коробкам, которое нужно, чтобы не было минуса  
   0 , 0 , @N , ttk.id_kontr , id_zal   
  
   , case when - (ttk.q_FO -- - isnull(ttk.q_zakaz,0)    
   - master.dbo.maxz(q_plan_pr, ttk.q_min_ost) ) > 0 -- значит есть потребность  
   then   
     
   case when ttk.max_ost_tt_tov <= 0 or zc.Status=2 then -- значит нет макс остатка или статус 2  
   master.dbo.maxz( case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end ,( - (ttk.q_FO -- - isnull(ttk.q_zakaz,0)  
    -master.dbo.maxz(q_plan_pr, ttk.q_min_ost) )) )   
     
   else -- значит есть макс остаток, не больше распределить  
   master.dbo.maxz( 0 ,master.dbo.minz(  
   master.dbo.maxz( case when ttk.tt_format_rasp =7  then 1 else tk.Kolvo_korob end ,( - (ttk.q_FO  -- -  isnull(ttk.q_zakaz,0)  
    -master.dbo.maxz(q_plan_pr, ttk.q_min_ost) )) )   
   , ttk.max_ost_tt_tov - ttk.q_FO -- + isnull(ttk.q_zakaz,0)  
   ))  
   end  
     
   --case when ttk.q_min_ost - (q_FO ) > 0 -- значит есть потребность  
   --then master.dbo.maxz( tk.Kolvo_korob ,(ttk.q_min_ost - (q_FO )) )  
   else 0 end q_nuzno_init , -- для 2 статуса расчет без учета Макс_остатка  
     
    [Status]  
   ,[type_add]  
      ,[koef_ost]  
      ,[date_add]  
      ,case when isnull(rs.q_max,0)>0 then 100* rs.Fact / rs.q_max end p3  
      ,case when isnull(rs.q_max,0)>0 then 100* rs.q_fo / rs.q_max end p4     
      ,ttk.q_zakaz , ttk.tt_format_rasp , ttk.price_rasp, ttk.koef_ost_pr_rasp, ttk.id_tov_pvz_rasp  
     
   FROM M2..tt_tov_kontr ttk with ( index (ind1))   
   inner join m2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on ttk.id_tov=tk.id_tov and ttk.id_kontr=tk.id_kontr and tk.Number_r=@N  
  
      left join M2..ZC_tt_tov zc   on zc.id_tt = ttk.id_tt and zc.id_tov = ttk.id_tov  
  
      left join #rasp_for_sales_3 rs on rs.id_tt = ttk.id_tt and rs.id_tov = ttk.id_tov  
    
       --inner join M2..tt on tt.id_TT = ttk.id_tt  
    
   where ttk.Number_r=@N  
   and ttk.q_FO is not null and q_plan_pr is not null  
   and tk.rasp_all=1  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 77061, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 62 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )  
   RETURN         
   end  
  
   END CATCH   
 END--while    
    
    
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 60, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
-- новый алгоритм для Спб. - если в распределении только Спб, то убрать малые остатки  
-- нет ни одного магазина не из Спб  
/**  
if not exists(  
select r.*  
from M2..raspr_zadanie_tt r    
  inner join M2..tt on tt.id_TT = r.id_tt and tt.adress not like ('%санкт%')  
where r.number_r= @N)  
**/  
  
if @dont_use_wait_sklad=1  
 begin  
   
  
update tk  
set q_ost_sklad = 0  
--Declare @N int = 79737  
--select *  
from M2..tov_kontr tk   
inner join  
(  
Select tk.id_tov , tk.id_kontr   
from M2..tov_kontr tk   
inner join m2..tt_tov_kontr ttk on ttk.Number_r = tk.Number_r and tk.id_tov = ttk.id_tov and tk.id_kontr = ttk.id_kontr  
where tk.Number_r = @N   
group by tk.id_tov , tk.id_kontr, tk.q_ost_sklad  
having tk.q_ost_sklad<sum(ttk.q_plan_pr*2)  
) a on a.id_tov = tk.id_tov and a.id_kontr = tk.id_kontr  
where tk.Number_r = @N   
  
    
    
    
 end   
   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 60001, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
  
  
/**  
---------------------------------------------------------------------------------------------------------------------------  
--сделать распределение на МЦК - штучное, по одной характеристике все вместе, в первый приоритет и потом еще, чтоб суммарно было кратно коробке.  
-- при распределении всех других ТТ не учитывать распределение на МЦК  
-- для этого после расчета Распределения Number_N + 1000000 в rasp , tt_tov_kontr и поправить остаток на складе в tov_kontr, tov_kontr_date , tov_kontr_zal  
-- в конце распределения вернуть обратно.  
if @j1=1  
begin  
  
  
if exists (Select * from #MCK)  
begin   
  
--declare @N int = 91192  
  
if OBJECT_ID('tempdb..#rasp_mck') is not null drop table #rasp_mck  
create table #rasp_mck (id_tt int, id_tov int ,q_max_ost int, q_FO int , id_kontr int , q_raspr int , q_plan_pr int , srok_godnosti int , q_min_ost int, id_kontr_init int)  
delete from #rasp_mck  
insert into #rasp_mck  
--declare @N int =  91192  
Select r.id_tt , r.id_tov ,r.q_max_ost, r.q_FO , r.id_kontr , r.q_raspr , r.q_plan_pr , tk.srok_godnosti , r.q_min_ost , r.id_kontr_init  
from M2..rasp r   
inner join #MCK tt on tt.id_tt_mck = r.id_tt  
inner join M2..tov_kontr tk on tk.Number_r = r.number_r and tk.id_tov = r.id_tov and tk.id_kontr = r.id_kontr  
where r.number_r =@N  
and r.q_plan_pr>0  
--and r.id_tt = 12203 and r.id_tov in (22806,15360)  
  
--select * from #rasp_mck  
  
-- найти товары, которые нужно отгрузить  
if OBJECT_ID('tempdb..#nuzno_mck') is not null drop table #nuzno_mck  
create table #nuzno_mck (id_tov int , q_nuzno int, q_nuzno_fact int null, id_kontr int null , id_kontr_rasp int null , id_kontr_init int)  
delete from #nuzno_mck  
insert into #nuzno_mck (id_tov  , q_nuzno  ,  id_kontr_init)  
Select r.id_tov , sum(  case when r.q_max_ost > 0 then master.dbo.maxz( 0 ,  master.dbo.minz( r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO ,  r.q_max_ost - r.q_FO))  
                                                  else master.dbo.maxz( 0,   r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO) end  
  ) q_nuzno  , min(r.id_kontr) id_kontr  
from #rasp_mck r  
group by  r.id_tov   
  
-- проставить более частого id_kontr из rasp   
--declare @N int = 91192  
update n  
set id_kontr_rasp = a.id_kontr  
from #nuzno_mck n  
inner join   
(  
--declare @N int = 91192  
Select top 1 with ties r.id_tov , r.id_kontr   
from #rasp_mck r    
inner join M2..tov_kontr tk   on tk.Number_r=@N and tk.id_tov = r.id_tov and tk.id_kontr = r.id_kontr and (tk.id_tov_init = tk.id_tov or r.id_kontr = r.id_kontr_init)  
group by r.id_tov , r.id_kontr  
order by ROW_NUMBER() over (partition by r.id_tov order by count(*) desc)  
) a on a.id_tov = n.id_tov  
  
  
-- остатки на складе найти  
-- и прописать реальную отгрузку и контрагента  
-- если основная характеристика есть на остатках, то с нее берем, если нет, то с большей  
--select *  
--declare @N int = 91192  
update n  
set q_nuzno_fact = f.q_nuzno_fact , id_kontr = f.id_kontr  
from #nuzno_mck n  
inner join  
(  
--declare @N int = 91192  
select  top 1 with ties   
r.id_tov , r.id_kontr , r.q_ost_sklad -r.q_wait_sklad q_ost_sklad, r.Kolvo_korob , tt.q_nuzno ,  ceiling(1.0* tt.q_nuzno/r.Kolvo_korob) * r.Kolvo_korob q_nuzno_fact    
--,case when r.q_ost_sklad -r.q_wait_sklad >=tt.q_nuzno and r.id_kontr = tt.id_kontr_init then 1000000 when r.q_ost_sklad-r.q_wait_sklad>=tt.q_nuzno then r.q_ost_sklad -r.q_wait_sklad else 0 end  
from M2..tov_kontr r    
inner join #nuzno_mck tt on tt.id_tov = r.id_tov   
where r.id_tov = r.id_tov and r.number_r = @N  
--and r.id_tov=22806  
order by ROW_NUMBER() over (partition by r.id_tov   
order by case when r.q_ost_sklad -r.q_wait_sklad >=tt.q_nuzno and r.id_kontr = tt.id_kontr_init then 1000000 when r.q_ost_sklad-r.q_wait_sklad>=tt.q_nuzno then r.q_ost_sklad -r.q_wait_sklad else 0 end desc)  
) f on f.id_tov = n.id_tov  
where f.q_ost_sklad>=f.q_nuzno  
  
--select * from #nuzno_mck  
  
--declare @N int = 76829  
-- проставить id_kontr  
update r  
set id_kontr = n.id_kontr  
--select *  
from #rasp_mck r  
inner join #nuzno_mck n on n.id_tov = r.id_tov  
  
-- теперь избытки раскидать по тт, определить максим колво штук  
  
declare @max_mck int -- макс колво штук на 1 тт  
select @max_mck = max (ceiling ( 1.0* (n.q_nuzno_fact - n.q_nuzno) / a.q))   
from #nuzno_mck n  
inner join (Select count(*) q from #MCK) a on 1=1  
where n.q_nuzno_fact - n.q_nuzno>0  
  --//+++АК SHEP 2018.10.26  
  AND a.q != 0  
  --//---АК SHEP 2018.10.26  
  
-- проставить колво распределения  
update r  
set q_raspr = g.q  
--select *  
from #rasp_mck r  
inner join   
(select f.id_tt , f.id_tov , sum(f.q) q  
from   
(  
-- посчитать сколько каждого товара добавить на каждую тт  
select s.id_tt , s.id_tov, 1 q  
from   
(  
Select r.id_tt , r.id_tov  , a.i    
, ROW_NUMBER() over (partition by  r.id_tov order by a.i , r.q_fo) rn  
from #rasp_mck r  
inner join (select ROW_NUMBER() over (order by date_add) i from jobs..Jobs_log  ) a on a.i<= @max_mck  
where case when r.q_max_ost > 0 then master.dbo.maxz(0, master.dbo.minz( r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO ,  r.q_max_ost - r.q_FO))  
                                else master.dbo.maxz(0, r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO) end >0  
)s  
inner join #nuzno_mck n on n.id_tov = s.id_tov and s.rn<=n.q_nuzno_fact - n.q_nuzno  
  
  
-- и первоначальное распределение  
union all  
Select r.id_tt , r.id_tov ,case when r.q_max_ost > 0 then master.dbo.maxz(0, master.dbo.minz( r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO ,  r.q_max_ost - r.q_FO))  
                                                     else master.dbo.maxz(0, r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO) end q_nuzno   
from #rasp_mck r  
where case when r.q_max_ost > 0 then master.dbo.maxz(0, master.dbo.minz(  r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO ,  r.q_max_ost - r.q_FO))  
                                else master.dbo.maxz(0, r.q_plan_pr + master.dbo.maxz( r.q_min_ost,r.q_plan_pr* case when srok_godnosti>9 then 1 else 0 end) - r.q_FO) end >0  
  
)f  
group by f.id_tt , f.id_tov  
) g on g.id_tt = r.id_tt and g.id_tov = r.id_tov  
  
  
-- теперь вносить изменения в Распределения Number_N + 1000000 в rasp , tt_tov_kontr и поправить остаток на складе в tov_kontr, tov_kontr_date , tov_kontr_zal  
  
update r  
set q_raspr = case when rm.id_kontr is not null then rm.q_raspr else 0 end,   
id_kontr = isnull(rm.id_kontr,r.id_kontr) , number_r =  @N + 1000000 , q_ko_ost = r.q_ko_ost+case when rm.id_kontr is not null then rm.q_raspr else 0 end - r.q_raspr  
from M2..rasp r  
inner join #rasp_mck rm on rm.id_tt = r.id_tt and rm.id_tov = r.id_tov  
where r.number_r = @n  
and rm.id_kontr is not null    
  
update r  
set  id_kontr = isnull(rm.id_kontr,r.id_kontr) , number_r = @N + 1000000  
from M2..tt_tov_kontr r  
inner join #rasp_mck rm on rm.id_tt = r.id_tt and rm.id_tov = r.id_tov  
where r.number_r = @n  
and rm.id_kontr is not null    
  
update r  
set q_ost_sklad = r.q_ost_sklad - n.q_nuzno_fact , q_ost_sklad_calc = r.q_ost_sklad_calc - n.q_nuzno_fact  
from M2..tov_kontr r  
inner join #nuzno_mck n on n.id_tov = r.id_tov and n.id_kontr = r.id_kontr  
where r.number_r = @n  
  
  
if OBJECT_ID('tempdb..#zc_tov_kontr_date') is not null drop table #zc_tov_kontr_date  
  
select top 1 with ties r.id_tov , r.id_kontr , r.q_ost_sklad , r.date_ost , n.q_nuzno_fact  
into #zc_tov_kontr_date  
from M2..tov_kontr_date r  
inner join #nuzno_mck n on n.id_tov = r.id_tov and n.id_kontr = r.id_kontr  
where r.number_r = @n  
order by ROW_NUMBER() over (partition by r.id_tov , r.id_kontr order by r.q_ost_sklad desc)  
  
--select *  
update r  
set q_ost_sklad = r.q_ost_sklad - a.q_nuzno_fact  
from M2..tov_kontr_date r  
inner join  #zc_tov_kontr_date a on a.id_tov = r.id_tov and a.id_kontr = r.id_kontr and a.date_ost = r.date_ost  
where r.number_r = @n  
  
if OBJECT_ID('tempdb..#zc_tov_kontr_zal') is not null drop table #zc_tov_kontr_zal  
  
select top 1 with ties r.id_tov , r.id_kontr , r.q_ost_zal , r.id_zal , n.q_nuzno_fact  
into #zc_tov_kontr_zal  
from M2..tov_kontr_zal r  
inner join #nuzno_mck n on n.id_tov = r.id_tov and n.id_kontr = r.id_kontr  
where r.number_r = @n  
order by ROW_NUMBER() over (partition by r.id_tov , r.id_kontr order by r.q_ost_zal desc)  
  
  
update r  
set q_ost_zal = r.q_ost_zal - a.q_nuzno_fact  
--select *  
from M2..tov_kontr_zal r  
inner join #zc_tov_kontr_zal a on a.id_tov = r.id_tov and a.id_kontr = r.id_kontr and a.id_zal = r.id_zal  
where r.number_r =@n  
  
  
  
  
  
end  
end  
**/  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 60002, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
----------------------------------------------------------------------------------------------------------------------------------  
  
  
 -- проставить в план продаж наибольшее из вторую наибольшей, плана продаж и минимального остатка.  
 -- в случае если есть отрицат коэф на товар, то выставить план продаж * (1+коэф)  
   
      
  
   
   if OBJECT_ID('tempdb..#ttk2') is not null drop table #ttk2  
   create table #ttk2 (id_tt int , id_tov int , q_new real)  
      
 --delete from #ttk2  
 insert into #ttk2  
   
 select ttk.id_tt , ttk.id_tov , master.dbo.maxz(master.dbo.maxz(r.Fact,ttk.q_plan_pr)  
 * case when ISNULL(tov.koef_tov,0)<=5 then (1+tov.koef_tov) else 1 end --+ ISNULL(ttk.q_zakaz,0)   
  ,ttk.q_min_ost --+ ISNULL(ttk.q_zakaz,0)  
  ) q_new  
 from m2..tt_tov_kontr ttk with ( index (ind1))  
 inner join #rasp_1 r on ttk.id_tov=r.id_tov and ttk.id_tt = r.id_tt   
 inner join m2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on ttk.id_tov=tk.id_tov and ttk.id_kontr=tk.id_kontr and tk.Number_r=@N  
 left join M2..tov   on tov.id_tov = ttk.id_tov and tov.Number_r = @N  
 --inner join M2..tt on tt.id_TT = ttk.id_tt and tt.tt_format not in (7,10)  
 where ttk.Number_r=@N and tk.rasp_all=1  
 and q_plan_pr <> master.dbo.maxz(master.dbo.maxz(r.Fact,ttk.q_plan_pr)  
 * case when ISNULL(tov.koef_tov,0)<=5 then (1+tov.koef_tov) else 1 end ,ttk.q_min_ost )  
 and ttk.tt_format_rasp not in (10)  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 600120, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 --------------------------------------------------------------------------------------------------  
  
  
    /**  
 update m2..tt_tov_kontr  
 set q_plan_pr = ttk1.q_new  
 from m2..tt_tov_kontr ttk with (rowlock,index (PK_tt_tov_kontr))  
 inner join #ttk2 ttk1 on ttk.id_tt = ttk1.id_tt and ttk.id_tov = ttk1.id_tov and ttk.Number_r=@N  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 600130, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
     **/  
  
  
 update m2..rasp  
 set q_ko_ost = r.q_ko_ost + r.q_plan_pr - ttk1.q_new , q_plan_pr = ttk1.q_new  
 from m2..rasp r with (index (PK_rasp))  
 inner join #ttk2 ttk1 on r.Number_r=@N and r.id_tt = ttk1.id_tt and r.id_tov = ttk1.id_tov   
   
  
  
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 600140, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
    
 -- drop table #ttk2  
  
  
 -- процедура перекидки товаров между харакетеристиками  
 ------ ______________________________________  
  
 -- если есть nuzno на одной характеристике, но остатки = 0 - то перекинуть на любую другую, на которой есть остатки  
  
    if OBJECT_ID('tempdb..#add_r1') is not null drop table #add_r1  
    if OBJECT_ID('tempdb..#add_r1600140') is not null drop table #add_r1600140  
 create table #add_r1 (id_tov int , id_kontr int , id_kontr_2 int)  
  
  
--declare @n int = 42312   
 Select top 1 with ties tk.id_tov , tk.id_kontr   
   
 into #add_r1600140  
  from M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )   
  left join M2..rasp r with (  INDEX (PK_rasp)) on r.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 where tk.number_r = @N and (tk.q_ost_sklad_calc>0 or r.number_r is null)   
    
 order by ROW_NUMBER() over (partition by tk.id_tov , tk.id_kontr order by tk.q_ost_sklad_calc desc)   
    
 --delete from #add_r1  
 insert into #add_r1  
   
  
   
--declare @n int = 42312 --,@id_job int = 2345 , @getdate datetime = getdate() , @i int = 1 , @date_rasp date = {d'2016-08-29'}  
  
 select top 1 with ties r.id_tov , r.id_kontr , b.id_kontr id_kontr_2  
  from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 inner join #add_r1600140 b on r.id_tov = b.id_tov   
 where r.number_r = @N and r.q_nuzno > 0 and tk.q_ost_sklad_calc<=0  
    order by ROW_NUMBER() over (partition by r.id_tov order by tk.q_ost_sklad_calc desc)  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 61, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tt_tov_kontr with (rowlock)  
 set id_kontr = a.id_kontr_2  
  from M2..tt_tov_kontr r with (index (ind1))  
  inner join #add_r1 a on r.id_tov = a.id_tov and r.id_kontr = a.id_kontr  
 where r.number_r = @N   
  
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
    Select @N , 0 , id_tov ,  id_kontr_2 ,id_kontr , 61  
    from #add_r1   
      
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 62, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..rasp   
 set id_kontr = a.id_kontr_2  
  from M2..rasp r with ( index(ind1))  
  inner join #add_r1 a on r.id_tov = a.id_tov and r.id_kontr = a.id_kontr  
 where r.number_r = @N   
  
 -- drop table #add_r1  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 63, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 if OBJECT_ID('tempdb..#b640') is not null drop table #b640  
   
  -- складируемые товары, по которым остатки на складе меньше, чем q_nuzno по магазинам, где они есть  
 --declare @N int = 64318  
 select v.id_tov   
 into #b640  
 from   
 (  
 -- сумма q_nuzno и остатка на складе по характеристикам по всем характеристикам, которые нужны в rasp sum(r.q_nuzno)  
 Select r.id_tov , r.id_kontr , SUM(r.q_nuzno) q_nuzno, tk.q_ost_sklad_calc  
  from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
  and tk.rasp_all=0  
 where r.number_r = @N and tk.q_ost_sklad_calc>0  
 and r.q_nuzno>0   
 group by r.id_tov , r.id_kontr, tk.q_ost_sklad_calc  
 having sum(r.q_nuzno)>0  
 )v  
 group by v.id_tov  
 having sum(v.q_ost_sklad_calc)<SUM(v.q_nuzno)   
   
  
  
 -- если не нужна характеристика, но есть осткатки - перенести на самую больше нуждающуюся   
  
 if OBJECT_ID('tempdb..#b641') is not null drop table #b641  
  
    --declare @N int = 39909  
 Select top 1 with ties r.id_tov , r.id_kontr   
 into #b641  
  from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 where r.number_r = @N and tk.q_ost_sklad_calc>0  
 and r.q_nuzno>0  
  
 and  
 ( tk.rasp_all = 1 or  
 tk.id_tov in   
 (select * from #b640  
 ))  
   
  group by r.id_tov , r.id_kontr  
 having sum(r.q_nuzno)>0  
 order by ROW_NUMBER() over (partition by r.id_tov , r.id_kontr order by sum(r.q_nuzno) desc)   
   
   
    if OBJECT_ID('tempdb..#add_r2') is not null drop table #add_r2  
    create table #add_r2 (id_tov int , id_kontr int , id_kontr_2 int, rn int , id_tt int)  
  
      
 --delete from #add_r2  
 --declare @N int = 35058  
 insert into #add_r2  
   
 --declare @N int = 39548  
   
 select r.id_tov , r.id_kontr , b.id_kontr id_kontr_2,   
 ROW_NUMBER() over (partition by r.id_tov , b.id_kontr order by r.id_kontr ) rn ,0   
  from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 inner join #b641 b on r.id_tov = b.id_tov  
 where r.number_r = @N and tk.q_ost_sklad_calc>0   
 group by r.id_tov , r.id_kontr , b.id_kontr   
 having sum(r.q_nuzno) = 0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 641, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
  
 update #add_r2  
 set id_tt = b.id_tt  
 from #add_r2 a  
 inner join  
 (  
   
 --declare @N int = 37573  
   
 select r.id_tov , r.id_kontr , r.id_tt ,ROW_NUMBER() over (partition by  r.id_tov , r.id_kontr order by id_tt) rn  
 from M2..rasp r with (  INDEX (ind1))  
 inner join  
 (Select r.id_tov , r.id_kontr  
  from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) ) on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 where r.number_r = @N and tk.q_ost_sklad_calc>0  
 and r.q_nuzno>0  
 group by r.id_tov , r.id_kontr  
 having sum(r.q_nuzno)>0  
 ) a on a.id_tov = r.id_tov and a.id_kontr = r.id_kontr  
 where r.number_r = @N and r.q_nuzno>0  
   
   
 ) b on b.id_tov = a.id_tov and b.id_kontr = a.id_kontr_2 and b.rn=a.rn    
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 64, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update  M2..rasp   
 set id_kontr = b.id_kontr  
 from M2..rasp r with (rowlock, index(ind1))  
 inner join #add_r2 b on r.id_tov = b.id_tov and r.id_kontr = b.id_kontr_2 and r.id_tt=b.id_tt  
 where r.number_r= @N  
  
  
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
    Select @N , id_tt, id_tov ,  id_kontr ,id_kontr_2 , 64  
    from #add_r2   
      
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 65, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tt_tov_kontr with (rowlock)  
 set id_kontr = b.id_kontr  
 from M2..tt_tov_kontr r with (index (ind1))  
 inner join #add_r2 b on r.id_tov = b.id_tov and r.id_kontr = b.id_kontr_2 and r.id_tt=b.id_tt  
 where r.number_r = @N   
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 66, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
 -- drop table #add_r2  
 -----------------------------------------------------------------------------------------------------  
  
  
  
 -- новый алгоритм перераспределения   
   
 --посчитать среднюю долю избытка от того, что нужно, чтоб считать избытки или нехватку именно от этой доли, а не от 0  
  
    --declare @N int = 68772  
      
  if OBJECT_ID('tempdb..#b67') is not null drop table #b67  
       
    create table #b67 (id_tov int, id_kontr int)  
    insert into #b67  
    select r.id_tov , r.id_kontr   
 from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )   
  on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr and tk.rasp_all=0  
 where r.number_r = @N   
 group by r.id_tov , r.id_kontr, tk.q_ost_sklad_calc   
 having sum(r.q_nuzno) > tk.q_ost_sklad_calc   
   
  
 --declare @N int = 68772   
   if OBJECT_ID('tempdb..#rasp_tov_kontr') is not null drop table #rasp_tov_kontr   
   create table #rasp_tov_kontr (id_tov int  , id_kontr int, q_nuzno real, q_ost_sklad_calc real  
 , Kolvo_korob real , id_kontr_init int, id_tt int  
 , q_FO real, q_max_ost real, ww int , rn int , q_plan_pr real )  
   create clustered index ind1 on   #rasp_tov_kontr (id_tov)  
   
 --delete from #rasp_tov_kontr  
   
  
 --declare @N int = 67701   
 insert into #rasp_tov_kontr  
 select r.id_tov , r.id_kontr , r.q_nuzno , tk.q_ost_sklad_calc , tk.Kolvo_korob, r.id_kontr_init, r.id_tt , r.q_FO , r.q_max_ost , 0 ,  
 row_number() over (partition by r.id_tov, r.id_kontr order by r.q_nuzno) rn , r.q_plan_pr  
 from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )   
  on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
  
 where r.number_r = @N and q_nuzno>0  
 and (tk.rasp_all=1 -- АК 23.09 - только для нескладируемых товаров   
 or r.id_tov in  
   
 (select distinct id_tov from #b67)   
 ) -- и для складируемых, если по одной их характеристик не хватает полностью  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 67, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  -- определить долю в коробках того, что нужно к тому, что есть на складе.  СредДоля - Остаток/Нужно  
 if OBJECT_ID('tempdb..#tov_dolya') is not null drop table #tov_dolya  
 create table #tov_dolya (id_tov int , СредДоля real)  
   
/**  
1665 0,819896142568124  
**/  
   
 --delete from #tov_dolya  
 insert into #tov_dolya  
  
 select a.id_tov ,  
    sum(ost) / sum(q_nuzno) --,  sum(ost) , sum(q_nuzno)  
 --master.dbo.minz(10,SUM(Избыток) / SUM(q_nuzno)) СредДоля  
 --master.dbo.maxz(-1, master.dbo.minz(1.0, 1.0* SUM(Избыток) / SUM(q_nuzno)) -1 ) СредДоля  
 from   
 (select r.id_tov , r.id_kontr ,   
 sum(r.q_nuzno/r.Kolvo_korob ) q_nuzno , r.q_ost_sklad_calc/r.Kolvo_korob  Ost ,  
  r.q_ost_sklad_calc/r.Kolvo_korob  -sum(r.q_nuzno/r.Kolvo_korob ) Избыток , r.Kolvo_korob  
  from #rasp_tov_kontr r  
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc , r.Kolvo_korob   
 having sum(r.q_nuzno/r.Kolvo_korob )>0.01  
 ) a  
 group by a.id_tov   
 having  COUNT(distinct a.id_kontr)>1 and sum(ost) / sum(q_nuzno)>0  
  
-- определить по каждой характиристике избытки и нехатку коробок. По товару в сумме 0.  
  
 if OBJECT_ID('tempdb..#perebros_kor') is not null drop table #perebros_kor  
    create table #perebros_kor (id_tov int, id_kontr_нехв int,id_kontr_изб int, q_kor int)  
  
 if OBJECT_ID('tempdb..#perebros_kor_itog') is not null drop table #perebros_kor_itog  
    create table #perebros_kor_itog (id_tov int, id_kontr_нехв int,id_kontr_изб int, q_kor int , rn_max int)  
          
  
 if OBJECT_ID('tempdb..#pereb_1') is not null drop table #pereb_1  
    create table #pereb_1 (id_tov int, id_kontr int, Избыток int, Нехватка int)  
    insert into #pereb_1  
 select r.id_tov , r.id_kontr ,-- r.Kolvo_korob, r.q_ost_sklad_calc , sum(r.q_nuzno),  
 --sum(r.q_nuzno)/r.Kolvo_korob q_nuzno , r.q_ost_sklad_calc/r.Kolvo_korob Ost ,  
-- floor(master.dbo.maxz(r.q_ost_sklad_calc -sum(r.q_nuzno* ( case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 1 end)) ,0 )/r.Kolvo_korob) Избыток ,  
-- -floor(master.dbo.minz(r.q_ost_sklad_calc -sum(r.q_nuzno* ( case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 1 end)),0 )/r.Kolvo_korob) Нехватка   
 floor(master.dbo.maxz(r.q_ost_sklad_calc/ СредДоля  -sum(r.q_nuzno  ),0 ) /r.Kolvo_korob) Избыток ,  
 floor(-master.dbo.minz(r.q_ost_sklad_calc / СредДоля -sum(r.q_nuzno ),0 ) /r.Kolvo_korob) Нехватка   
  
  from #rasp_tov_kontr r  
  inner join #tov_dolya td on r.id_tov = td.id_tov   
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc , r.Kolvo_korob, СредДоля  
   
 --Select * from [M2].[dbo].[rasp_smena_kontr]  
 --where number_r =  67701 and id_tov = 20354  
 --select * from #rasp_tov_kontr  
 --where  id_tov = 20354 and id_kontr<>id_kontr_init  
   
/**  
id_tov id_kontr Избыток Нехватка  
1665 369           31 0  
1665 12091        0 32  
**/   
  
   -- теперь найти все пары id_kontr по замене в коробках  
     
   -- находим максимальную нехватку и избыток и зачитываем и так в цикле  
     
   insert into #perebros_kor  
   select a.id_tov , a.id_kontr , b.id_kontr , master.dbo.minz(a.Нехватка , b.Избыток) Зачет  
   from  
   (select top 1 with ties p.id_tov , p.id_kontr , Нехватка  
   from #pereb_1 p  
   where p.Нехватка>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Нехватка desc))a  
  inner join  
   (select top 1 with ties p.id_tov , p.id_kontr , Избыток  
   from #pereb_1 p  
   where p.Избыток>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Избыток desc)) b  
   on b.id_tov = a.id_tov  
   where master.dbo.minz(a.Нехватка , b.Избыток)>0  
     
   while exists (select * from  #perebros_kor)   
   begin  
     
   --select * from #perebros_kor_itog  
     
   insert into #perebros_kor_itog  
   select *,0 from #perebros_kor  
     
   update #pereb_1  
   set Избыток = p.Избыток - k.q_kor  
   --select *  
   from #pereb_1 p  
   inner join #perebros_kor k on p.id_tov = k.id_tov and k.id_kontr_изб = p.id_kontr  
     
   update #pereb_1  
   set Нехватка = p.Нехватка - k.q_kor  
   --select *  
   from #pereb_1 p  
   inner join #perebros_kor k on p.id_tov = k.id_tov and k.id_kontr_нехв = p.id_kontr  
     
     
   delete from #perebros_kor  
   insert into #perebros_kor  
   select a.id_tov , a.id_kontr , b.id_kontr , master.dbo.minz(a.Нехватка , b.Избыток) Зачет  
   from  
   (select top 1 with ties p.id_tov , p.id_kontr , Нехватка  
   from #pereb_1 p  
   where p.Нехватка>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Нехватка desc))a  
  inner join  
   (select top 1 with ties p.id_tov , p.id_kontr , Избыток  
   from #pereb_1 p  
   where p.Избыток>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Избыток desc)) b  
   on b.id_tov = a.id_tov  
   where master.dbo.minz(a.Нехватка , b.Избыток)>0     
     
   end  
       
     
   -- теперь по каждой паре подобрать тт из id_kontr Избыток  
     
   update #perebros_kor_itog  
   set rn_max = max_rn  
   from   
   #perebros_kor_itog p  
   inner join   
   (select p.id_tov , p.id_kontr_изб , p.id_kontr_нехв , p.q_kor , max(r.rn) max_rn  
   from #perebros_kor_itog p  
   inner join   
     
   (select r.id_tov , r.id_kontr , r.rn , sum( r2.q_nuzno/r.Kolvo_korob ) sum_kor  
       from #rasp_tov_kontr r  
    inner join #rasp_tov_kontr r2 on r.id_tov=r2.id_tov and r.id_kontr = r2.id_kontr and r2.rn <= r.rn  
    --where r.id_tov = 20354 and r.id_kontr=14694  
   group by r.id_tov , r.id_kontr ,r.rn  
      
    ) r  
     on p.id_tov=r.id_tov and p.id_kontr_нехв= r.id_kontr and p.q_kor>=r.sum_kor  
     group by p.id_tov ,  p.id_kontr_изб , p.id_kontr_нехв , p.q_kor  
     --having   
   ) r on p.id_tov = r.id_tov and p.id_kontr_изб = r.id_kontr_изб and r.id_kontr_нехв = p.id_kontr_нехв  
    
  
  --select * from #rasp_tov_kontr r  
  --where  r.id_tov = 20354 and r.id_kontr=133  
    
   --select * from #pereb_1  
   --select * from #perebros_kor_itog  
    
  
    
 --declare @N int = 67701  
    
     insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena]  
      ,q_smena  
      ,id_tt_2  
      ,napr_smena)  
    --declare @N int = 67701     
    Select @N , r.id_tt , r.id_tov ,   p1.id_kontr_нехв ,  p1.id_kontr_изб  , 677  --, r.rn   
    , r.q_nuzno , r.id_tt , 0  
 from #rasp_tov_kontr r   
 inner join  #perebros_kor_itog p1 on r.id_tov= p1.id_tov and r.id_kontr=p1.id_kontr_нехв and p1.rn_max >= r.rn  
  
  
  
 -- поменять 2  
 update M2..rasp  
 set id_kontr = p1.id_kontr_изб  
 --declare @N int = 67701  
 --select *  
 from M2..rasp r with ( index(ind1))  
 inner join #rasp_tov_kontr p on r.id_tov= p.id_tov and r.id_tt=p.id_tt  
 inner join  #perebros_kor_itog p1 on r.id_tov= p1.id_tov and r.id_kontr=p1.id_kontr_нехв and p1.rn_max >= p.rn  
 where r.number_r=@N --and r.id_tov=15540  
  
  
  
  
  
 update M2..tt_tov_kontr  
 set id_kontr = p1.id_kontr_изб  
  
 -- declare @N int = 68772  
 --select *  
 from M2..tt_tov_kontr r with (rowlock,index (ind1))  
 inner join #rasp_tov_kontr p on r.id_tov= p.id_tov and r.id_tt=p.id_tt  
 inner join  #perebros_kor_itog p1 on r.id_tov= p1.id_tov and r.id_kontr=p1.id_kontr_нехв and p1.rn_max >= p.rn  
 where r.number_r=@N   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 82, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
------------------------------------------------------------------------------------------------------------------------  
-- а теперь распределить по характеристикам равномерно по плану продаж  
  
    --declare @N int = 68772  
      
  --if OBJECT_ID('tempdb..#b67') is not null drop table #b67  
       
    --create table #b67 (id_tov int, id_kontr int)  
    delete from #b67  
    insert into #b67  
    select r.id_tov , r.id_kontr   
 from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )   
  on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr and tk.rasp_all=0  
 where r.number_r = @N   
 group by r.id_tov , r.id_kontr, tk.q_ost_sklad_calc   
 having sum(r.q_nuzno) > tk.q_ost_sklad_calc   
   
  
 --declare @N int = 68772   
   --if OBJECT_ID('tempdb..#rasp_tov_kontr') is not null drop table #rasp_tov_kontr   
  -- create table #rasp_tov_kontr (id_tov int  , id_kontr int, q_nuzno real, q_ost_sklad_calc real  
-- , Kolvo_korob real , id_kontr_init int, id_tt int  
-- , q_FO real, q_max_ost real, ww int , rn int , q_plan_pr real )  
--   create clustered index ind1 on   #rasp_tov_kontr (id_tov)  
   
 --delete from #rasp_tov_kontr  
   
  
 --declare @N int = 68903   
 delete from #rasp_tov_kontr  
 insert into #rasp_tov_kontr  
 select r.id_tov , r.id_kontr , r.q_nuzno , tk.q_ost_sklad_calc , tk.Kolvo_korob, r.id_kontr_init, r.id_tt , r.q_FO , r.q_max_ost , 0 ,  
 row_number() over (partition by r.id_tov, r.id_kontr order by r.q_nuzno) rn , r.q_plan_pr  
 from M2..rasp r with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )   
  on tk.Number_r=@N and tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
  
 where r.number_r = @N --and q_nuzno>0  
 and (tk.rasp_all=1 -- АК 23.09 - только для нескладируемых товаров   
 or r.id_tov in  
   
 (select distinct id_tov from #b67)   
 ) -- и для складируемых, если по одной их характеристик не хватает полностью  
   
 -------------------------------------------  
   
 delete from #tov_dolya  
 insert into #tov_dolya  
  
 select a.id_tov ,  
    sum(ost) / sum(q_plan_pr) --,  sum(ost) , sum(q_plan_pr) , sum(q1)  
 --master.dbo.minz(10,SUM(Избыток) / SUM(q_nuzno)) СредДоля  
 --master.dbo.maxz(-1, master.dbo.minz(1.0, 1.0* SUM(Избыток) / SUM(q_nuzno)) -1 ) СредДоля  
 from   
 (select r.id_tov , r.id_kontr , sum(r.q_plan_pr) q1 ,  
 sum(r.q_plan_pr/r.Kolvo_korob ) q_plan_pr , r.q_ost_sklad_calc/r.Kolvo_korob  Ost ,  
  r.q_ost_sklad_calc/r.Kolvo_korob  -sum(r.q_plan_pr/r.Kolvo_korob ) Избыток , r.Kolvo_korob  
  from #rasp_tov_kontr r  
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc , r.Kolvo_korob   
 having sum(r.q_plan_pr/r.Kolvo_korob )>0.01  
 ) a  
 group by a.id_tov   
 having  COUNT(distinct a.id_kontr)>1 and sum(ost) / sum(q_plan_pr)>0  
  
-- определить по каждой характиристике избытки и нехатку коробок. По товару в сумме 0.  
  
 --if OBJECT_ID('tempdb..#perebros_kor') is not null drop table #perebros_kor  
    --create table #perebros_kor (id_tov int, id_kontr_нехв int,id_kontr_изб int, q_kor int)  
  
 --if OBJECT_ID('tempdb..#perebros_kor_itog') is not null drop table #perebros_kor_itog  
    --create table #perebros_kor_itog (id_tov int, id_kontr_нехв int,id_kontr_изб int, q_kor int , rn_max int)  
          
  
  
 --if OBJECT_ID('tempdb..#pereb_1') is not null drop table #pereb_1  
    --create table #pereb_1 (id_tov int, id_kontr int, Избыток int, Нехватка int)  
    delete from #pereb_1  
    insert into #pereb_1  
 select r.id_tov , r.id_kontr ,-- r.Kolvo_korob, r.q_ost_sklad_calc , sum(r.q_nuzno),  
 --sum(r.q_nuzno)/r.Kolvo_korob q_nuzno , r.q_ost_sklad_calc/r.Kolvo_korob Ost ,  
-- floor(master.dbo.maxz(r.q_ost_sklad_calc -sum(r.q_nuzno* ( case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 1 end)) ,0 )/r.Kolvo_korob) Избыток ,  
-- -floor(master.dbo.minz(r.q_ost_sklad_calc -sum(r.q_nuzno* ( case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 1 end)),0 )/r.Kolvo_korob) Нехватка   
 floor(master.dbo.maxz(r.q_ost_sklad_calc/ СредДоля  -sum(r.q_plan_pr  ),0 ) /r.Kolvo_korob) Избыток ,  
 floor(-master.dbo.minz(r.q_ost_sklad_calc / СредДоля -sum(r.q_plan_pr ),0 ) /r.Kolvo_korob) Нехватка   
  
  from #rasp_tov_kontr r  
  inner join #tov_dolya td on r.id_tov = td.id_tov   
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc , r.Kolvo_korob, СредДоля  
   
 --Select * from [M2].[dbo].[rasp_smena_kontr]  
 --where number_r =  67701 and id_tov = 20354  
 --select * from #rasp_tov_kontr  
 --where  id_tov = 20354 and id_kontr<>id_kontr_init  
   
/**  
id_tov id_kontr Избыток Нехватка  
1665 369           31 0  
1665 12091        0 32  
**/   
  
   -- теперь найти все пары id_kontr по замене в коробках  
     
   -- находим максимальную нехватку и избыток и зачитываем и так в цикле  
   delete from #perebros_kor  
   insert into #perebros_kor  
   select a.id_tov , a.id_kontr , b.id_kontr , master.dbo.minz(a.Нехватка , b.Избыток) Зачет  
   from  
   (select top 1 with ties p.id_tov , p.id_kontr , Нехватка  
   from #pereb_1 p  
   where p.Нехватка>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Нехватка desc))a  
  inner join  
   (select top 1 with ties p.id_tov , p.id_kontr , Избыток  
   from #pereb_1 p  
   where p.Избыток>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Избыток desc)) b  
   on b.id_tov = a.id_tov  
   where master.dbo.minz(a.Нехватка , b.Избыток)>0  
     
   while exists (select * from  #perebros_kor)   
   begin  
     
   --select * from #perebros_kor_itog  
     
   delete from #perebros_kor_itog  
   insert into #perebros_kor_itog  
   select *,0 from #perebros_kor  
     
   update #pereb_1  
   set Избыток = p.Избыток - k.q_kor  
   --select *  
   from #pereb_1 p  
   inner join #perebros_kor k on p.id_tov = k.id_tov and k.id_kontr_изб = p.id_kontr  
     
   update #pereb_1  
   set Нехватка = p.Нехватка - k.q_kor  
   --select *  
   from #pereb_1 p  
   inner join #perebros_kor k on p.id_tov = k.id_tov and k.id_kontr_нехв = p.id_kontr  
     
     
   delete from #perebros_kor  
   insert into #perebros_kor  
   select a.id_tov , a.id_kontr , b.id_kontr , master.dbo.minz(a.Нехватка , b.Избыток) Зачет  
   from  
   (select top 1 with ties p.id_tov , p.id_kontr , Нехватка  
   from #pereb_1 p  
   where p.Нехватка>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Нехватка desc))a  
  inner join  
   (select top 1 with ties p.id_tov , p.id_kontr , Избыток  
   from #pereb_1 p  
   where p.Избыток>0  
   order by ROW_NUMBER() over (partition by p.id_tov order by p.Избыток desc)) b  
   on b.id_tov = a.id_tov  
   where master.dbo.minz(a.Нехватка , b.Избыток)>0     
     
   end  
       
     
   -- теперь по каждой паре подобрать тт из id_kontr Избыток  
     
   update #perebros_kor_itog  
   set rn_max = max_rn  
   from   
   #perebros_kor_itog p  
   inner join   
   (select p.id_tov , p.id_kontr_изб , p.id_kontr_нехв , p.q_kor , max(r.rn) max_rn  
   from #perebros_kor_itog p  
   inner join   
     
   (select r.id_tov , r.id_kontr , r.rn , sum( r2.q_plan_pr/r.Kolvo_korob ) sum_kor  
       from #rasp_tov_kontr r  
    inner join #rasp_tov_kontr r2 on r.id_tov=r2.id_tov and r.id_kontr = r2.id_kontr and r2.rn <= r.rn  
    where r2.q_nuzno=0 and r.q_nuzno=0  
    --where r.id_tov = 20354 and r.id_kontr=14694  
   group by r.id_tov , r.id_kontr ,r.rn  
      
    ) r  
     on p.id_tov=r.id_tov and p.id_kontr_нехв= r.id_kontr and p.q_kor>=r.sum_kor  
     group by p.id_tov ,  p.id_kontr_изб , p.id_kontr_нехв , p.q_kor  
     --having   
   ) r on p.id_tov = r.id_tov and p.id_kontr_изб = r.id_kontr_изб and r.id_kontr_нехв = p.id_kontr_нехв  
    
  
  --select * from #rasp_tov_kontr r  
  --where  r.id_tov = 20354 and r.id_kontr=133  
    
   --select * from #pereb_1  
   --select * from #perebros_kor_itog  
    
  
    
 --declare @N int = 67701  
    
     insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena]  
      ,q_smena  
      ,id_tt_2  
      ,napr_smena)  
    --declare @N int = 67701     
    Select @N , r.id_tt , r.id_tov ,   p1.id_kontr_нехв ,  p1.id_kontr_изб  , 6771  --, r.rn   
    , r.q_plan_pr , r.id_tt , 0  
 from #rasp_tov_kontr r   
 inner join  #perebros_kor_itog p1 on r.id_tov= p1.id_tov and r.id_kontr=p1.id_kontr_нехв and p1.rn_max >= r.rn and r.q_nuzno=0  
  
  
  
 -- поменять 2  
 update M2..rasp  
 set id_kontr = p1.id_kontr_изб  
 --declare @N int = 67701  
 --select *  
 from M2..rasp r with ( index(ind1))  
 inner join #rasp_tov_kontr p on r.id_tov= p.id_tov and r.id_tt=p.id_tt  
 inner join  #perebros_kor_itog p1 on r.id_tov= p1.id_tov and r.id_kontr=p1.id_kontr_нехв and p1.rn_max >= p.rn and p.q_nuzno=0  
 where r.number_r=@N --and r.id_tov=15540  
  
  
  
  
  
 update M2..tt_tov_kontr  
 set id_kontr = p1.id_kontr_изб  
  
 -- declare @N int = 68772  
 --select *  
 from M2..tt_tov_kontr r with (rowlock,index (ind1))  
 inner join #rasp_tov_kontr p on r.id_tov= p.id_tov and r.id_tt=p.id_tt  
 inner join  #perebros_kor_itog p1 on r.id_tov= p1.id_tov and r.id_kontr=p1.id_kontr_нехв and p1.rn_max >= p.rn and p.q_nuzno=0  
 where r.number_r=@N   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 8201, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
   
  
  
    
  
/**  
  
  
  
  
  
  
  
  
 declare @i as int = 1   
  
 /**  
  declare @N as int =94  
  , @id_job as int =17  
 declare @date_rasp as date , @id_sklad as int   
 declare @getdate as datetime = getdate()   
 -- взять параметры распределения  
 select @date_rasp=Date_r , @id_sklad=id_sklad  
 from M2..Raspr_zadanie   where Number_r=@N  
 **/  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 68, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  --declare @n int = 35479,@id_job int = 2345 , @getdate datetime = getdate() , @i int = 1  
  
  
 declare @i_max int  
  
  
 select @i_max= max (b.колвоКор)  
 from  
 (  
 select a.id_tov ,   
 sum(case when Избыток>0 then Избыток end) Избыток,  
 sum(case when Избыток<0 then Избыток end) Нехватка ,  
 master.dbo.minz(ABS(sum(case when Избыток<0 then Избыток end)) ,   
 sum(case when Избыток>0 then Избыток end) ) Перекидывать,  
 SUM(q_nuzno) q_nuzno ,  
 ceiling(master.dbo.minz(ABS(sum(case when Избыток<0 then Избыток/Kolvo_korob end)) ,   
 sum(case when Избыток>0 then Избыток/Kolvo_korob end) ))   
 колвоКор  
 from   
 (select r.id_tov , r.id_kontr ,   
 sum(r.q_nuzno) q_nuzno , r.q_ost_sklad_calc Ost ,  
  r.q_ost_sklad_calc -sum(r.q_nuzno * (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end)) Избыток , r.Kolvo_korob  
  from #rasp_tov_kontr r  
  inner join #tov_dolya td on td.id_tov = r.id_tov  
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc , r.Kolvo_korob   
 ) a  
 group by a.id_tov  
 having sum(case when Избыток>0 then Избыток end)>0  
 and sum(case when Избыток<0 then Избыток end)<0  
 ) b  
  
 --select @i_max  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
 select @id_job , 69, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) , @i_max   
 select @getdate = getdate()   
  
 -- найти все товары, по которым требуется переброс  
 delete from #id_tov_perebros_init  
 insert into #id_tov_perebros_init  
 select a.id_tov ,   
 sum(case when Избыток>0 then Избыток end) Избыток,  
 sum(case when Избыток<0 then Избыток end) Нехватка   
 from   
 (select r.id_tov , r.id_kontr ,   
 --sum(r.q_nuzno) q_nuzno , tk.q_ost_sklad_calc Ost ,  
  r.q_ost_sklad_calc -sum(r.q_nuzno * (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end)) Избыток   
  from #rasp_tov_kontr r  
  inner join #tov_dolya td on td.id_tov = r.id_tov  
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc -- , tk.Kolvo_korob   
 ) a  
 group by a.id_tov  
 having sum(case when Избыток>0 then Избыток end)>0  
 and sum(case when Избыток<0 then Избыток end)<0  
   
 -- найти контрагентов, по которым требуется переборос  
 delete from #id_tov_kontr_perebros_init  
 insert into #id_tov_kontr_perebros_init  
 select r.id_tov , r.id_kontr ,   
 sum(r.q_nuzno) q_nuzno , r.q_ost_sklad_calc Ost ,  
 master.dbo.maxz(r.q_ost_sklad_calc -sum(r.q_nuzno* (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end)),0 ) Избыток ,  
 master.dbo.minz(r.q_ost_sklad_calc -sum(r.q_nuzno* (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end)),0 ) Нехватка   
  from #rasp_tov_kontr r  
  inner join #id_tov_perebros_init idp on r.id_tov = idp.id_tov  
  inner join #tov_dolya td on r.id_tov = td.id_tov   
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc --, tk.Kolvo_korob  
   
   
  
 select @i = 1  
 while (@i < = @i_max)   
 begin  
  
  
--declare @n int = 35479,@id_job int = 2345 , @getdate datetime = getdate() , @i int = 1 , @date_rasp date = {d'2016-08-29'}  
  
  
 -- найти все товары, по которым требуется переброс  
 delete from #id_tov_perebros  
 insert into #id_tov_perebros  
   
 select a.id_tov ,   
 sum(case when Избыток>0 then Избыток end) Избыток,  
 sum(case when Избыток<0 then Избыток end) Нехватка   
 from   
 (select r.id_tov , r.id_kontr ,   
 --sum(r.q_nuzno) q_nuzno , tk.q_ost_sklad_calc Ost ,  
  r.q_ost_sklad_calc -sum(r.q_nuzno * (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end )) Избыток   
  from #rasp_tov_kontr r  
  inner join #tov_dolya td on td.id_tov = r.id_tov  
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc -- , tk.Kolvo_korob   
 ) a  
 group by a.id_tov  
 having sum(case when Избыток>0 then Избыток end)>0  
 and sum(case when Избыток<0 then Избыток end)<0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 70, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- найти контрагентов, по которым требуется переборос  
 delete from #id_tov_kontr_perebros  
 insert into #id_tov_kontr_perebros  
 select r.id_tov , r.id_kontr ,   
 sum(r.q_nuzno) q_nuzno , r.q_ost_sklad_calc Ost ,  
 master.dbo.maxz(r.q_ost_sklad_calc -sum(r.q_nuzno* (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end)),0 ) Избыток ,  
 master.dbo.minz(r.q_ost_sklad_calc -sum(r.q_nuzno* (1 + case when r.q_ost_sklad_calc > 2 * r.q_nuzno then СредДоля else 0 end)),0 ) Нехватка   
  from #rasp_tov_kontr r  
  inner join #id_tov_perebros idp on r.id_tov = idp.id_tov  
  inner join #tov_dolya td on r.id_tov = td.id_tov   
 group by r.id_tov , r.id_kontr , r.q_ost_sklad_calc --, tk.Kolvo_korob  
  
    -- удалить те, что не совпали направление с начальным  
    delete from #id_tov_kontr_perebros  
    from  #id_tov_kontr_perebros r   
     inner join #id_tov_kontr_perebros_init i on  r.id_tov  = i.id_tov and r.id_kontr = i.id_kontr  
    where r.Избыток * i.Нехватка < 0 or i.Избыток * r.Нехватка <0  
  
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 71, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
--    return  
--end  
  
 --select * from #id_tov_perebros  
 --select * from #id_tov_kontr_perebros  
 --select * from #rasp_tov_kontr  
  
  
  
 delete from #a  
 insert into #a  
 select r.id_tov , r.id_kontr , r.id_tt , r.q_nuzno , r.Kolvo_korob  
 from #rasp_tov_kontr r  
 inner join #id_tov_kontr_perebros idkp on r.id_tov = idkp.id_tov and r.id_kontr = idkp.id_kontr  
 inner join #id_tov_perebros idt on r.id_tov = idt.id_tov  
 where r.q_nuzno >0  
 --and r.id_kontr_init=r.id_kontr   
 and ww=0 -- значит не меняли еще  
 and r.q_nuzno < idt.Избыток * 1.15  
 and idkp.Избыток > 0   
  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7201, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
    -- новое 29,08,2016  
    -- если если обратный переброс, то в первую очередь по нему перебросить  
    -- найти обратные перебросы... те сначала смотрим избытки среди тт, с которых эта характ уже перебрасывалась  
      
  
  
   
 delete from #perebros  
  
 insert into #perebros  
  
--declare @n int = 35479,@id_job int = 2345 , @getdate datetime = getdate() , @i int = 1 , @date_rasp date = {d'2016-08-29'}  
  
  
   
    select d.id_tov , d.id_kontr_init , d.id_kontr , d.id_tt , d.id_tt_2 , d.q_smena , -1  
    from  
    (select a.id_tov , d.id_kontr_init , d.id_kontr , d.id_tt , d.id_tt_2 , d.q_smena ,  
    ROW_NUMBER() over ( partition by a.id_tov order by   
 idkp.Нехватка ,    
 abs( a.q_nuzno - d.q_smena ) desc ) rn    
    from #a a   
    inner join  
    (  
      
    select rsk.id_tov , rsk.id_kontr_init , rsk.id_tt , rsk.id_tt_2 ,  rsk.id_kontr     
    , sum(q_smena * rsk.napr_smena) q_smena   
    from M2..rasp_smena_kontr   rsk   
    where rsk.number_r = @N and rsk.type_smena > 1000000   
    group by rsk.id_tov , rsk.id_kontr_init , rsk.id_tt , rsk.id_tt_2 ,  rsk.id_kontr  
    having floor(sum(q_smena * rsk.napr_smena)) = 0  
      
      
        ) d on  a.id_tov = d.id_tov and a.id_kontr = d.id_kontr_init  
          
   inner join #id_tov_kontr_perebros idkp on a.id_tov = idkp.id_tov and a.id_kontr = idkp.id_kontr          
   ) d  
   where d.rn=1  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7202, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
   
 delete from #b  
 insert into #b  
 select r.id_tov , r.id_kontr , r.id_tt , r.q_nuzno , r.Kolvo_korob , r.q_FO , r.q_max_ost , r.id_kontr_init  
  from #rasp_tov_kontr r  
  inner join #id_tov_kontr_perebros idkp on r.id_tov = idkp.id_tov and r.id_kontr = idkp.id_kontr  
  inner join #id_tov_perebros idt on r.id_tov = idt.id_tov  
    
 where r.q_nuzno >0  
 --and r.id_kontr_init=r.id_kontr -- теперь всех смотрим, даже тех, кого поменяли  
 and ww=0 -- значит не меняли еще  
 and r.q_nuzno < - idt.Нехватка * 1.15  
 and idkp.Нехватка < 0  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 73, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- найти самые близкие по нужде контрагента с избытком и нехваткой и поменять их местами  
 -- наилучшие те, у кого равно количество коробок, а из этих выбираем, у кого больше колво коробок  
     
  
      
  
  
  
--select * from #a  
--select * from #b  
  
 insert into #perebros  
 select c.id_tov , c.id_kontr_1 , c.id_kontr_2 , c.id_tt_1 , c.id_tt_2 , c.q_nuzno_1 , 1  
 from   
 ( select a.id_tov , a.id_kontr id_kontr_1 , b.id_kontr id_kontr_2,   
 a.id_tt id_tt_1 , b.id_tt id_tt_2 ,  
 a.q_nuzno q_nuzno_1, b.q_nuzno q_nuzno_2 ,  
 ROW_NUMBER() over ( partition by a.id_tov order by   
 abs( ceiling(a.q_nuzno * a.Kolvo_korob ) / a.Kolvo_korob   
  - ceiling(b.q_nuzno * b.Kolvo_korob ) / b.Kolvo_korob ) / (a.q_nuzno + b.q_nuzno) ,   
  ceiling(a.q_nuzno * a.Kolvo_korob ) / a.Kolvo_korob + ceiling(b.q_nuzno * b.Kolvo_korob ) / b.Kolvo_korob desc  
  ) rn  
 from #a a   
 inner join #b b on b.id_tov=a.id_tov   
      
    left join #perebros p on p.id_tov = a.id_tov -- не считать по тем, по которым обратный перенос  
      
 --where b.q_max_ost < -0.001 or ( b.q_FO + a.q_nuzno - b.q_max_ost)<-0.001  
 where (b.q_max_ost <= 0 or ( b.q_FO + a.q_nuzno ) <= b.q_max_ost)  
     and p.id_tov is null  
  
 ) c   
 where c.rn=1  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 80, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 --select * from #perebros  
  
 /**  
 -- поменять 2  
 update M2..rasp  
 set id_kontr = p.id_kontr_1   
 -- select *  
 from M2..rasp r with (rowlock, index(ind1))  
 inner join #perebros p on r.id_tov= p.id_tov and r.id_kontr=p.id_kontr_2  
 and r.id_tt=p.id_tt_2  
 where r.number_r=@N   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 81, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tt_tov_kontr  
 set id_kontr = p.id_kontr_1  
 -- select *  
 from M2..tt_tov_kontr r with (rowlock,index (ind1))  
 inner join #perebros p on r.id_tov= p.id_tov and r.id_kontr=p.id_kontr_2  
 and r.id_tt=p.id_tt_2  
 where r.number_r=@N  
 **/  
  
 -- здесь меняем только в #rasp_tov_kontr  
  
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena]  
      ,q_smena  
      ,id_tt_2  
      ,napr_smena)  
        
    Select @N ,  r.id_tt , r.id_tov ,   p.id_kontr_1 ,  p.id_kontr_2  , 1000000  +   @i   
    , p.q_nuzno_1 , p.id_tt_1 , p.napr_smena  
 from #rasp_tov_kontr r   
 inner join #perebros p on r.id_tov= p.id_tov and r.id_kontr=p.id_kontr_2  
 and r.id_tt=p.id_tt_2  
  
  
   
 update #rasp_tov_kontr  
 set id_kontr = p.id_kontr_1, q_ost_sklad_calc = r2.q_ost_sklad_calc , Kolvo_korob = r2.Kolvo_korob ,  
 ww = 1 -- значит меняли  
 -- select *  
 from #rasp_tov_kontr r   
 inner join #perebros p on r.id_tov= p.id_tov and r.id_kontr=p.id_kontr_2  
 and r.id_tt=p.id_tt_2  
 inner join   
 (select *  
 from #rasp_tov_kontr r) r2 on r2.id_tov=p.id_tov and r2.id_kontr = p.id_kontr_1  
 and r2.id_tt = p.id_tt_1  
  
/**  
 update #rasp_tov_kontr  
 set id_kontr = p.id_kontr_2, q_ost_sklad_calc = r2.q_ost_sklad_calc , Kolvo_korob = r2.Kolvo_korob ,  
 ww = 1 -- значит меняли  
 -- select *  
 from #rasp_tov_kontr r   
 inner join #perebros p on r.id_tov= p.id_tov --and r.id_kontr=p.id_kontr_1  
 and r.id_tt=p.id_tt_1  
 inner join   
 (select *  
 from #rasp_tov_kontr r) r2 on r2.id_tov=p.id_tov --and r2.id_kontr = p.id_kontr_1  
 and r2.id_tt = p.id_tt_2  
**/       
      
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 82, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 if exists(select * from #perebros)  
 select @i=@i+1  
 else   
 select @i=@i_max+1  
  
 /**  
 delete from #rasp_tov_kontr  
  
 delete from #rr  
 insert into #rr  
 select r.id_tov , r.id_kontr , r.q_nuzno , r.id_kontr_init, r.id_tt  
 from M2..rasp r with (  INDEX (ind1))  
 where r.number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 83, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 insert into #rasp_tov_kontr  
 --declare @N int = 17676  
 select r.id_tov , r.id_kontr , r.q_nuzno , tk.q_ost_sklad_calc , tk.Kolvo_korob, r.id_kontr_init, r.id_tt  
 from #rr r with    
 inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1) )   
  on tk.id_tov=r.id_tov and tk.id_kontr=r.id_kontr  
 where tk.Number_r=@N   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 84, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 **/  
  
  
      
 end  
  
--select r.id_tov, r.id_kontr , r.id_kontr_init , SUM(r.q_nuzno) , COUNT(*)  
--from #rasp_tov_kontr r  
--where r.id_tov=185  
--group by r.id_tov, r.id_kontr , r.id_kontr_init  
  
 -- поменять 2  
 update M2..rasp  
 set id_kontr = p.id_kontr  
 -- select *  
 from M2..rasp r with (rowlock, index(ind1))  
 inner join #rasp_tov_kontr p on r.id_tov= p.id_tov and r.id_tt=p.id_tt  
 where r.number_r=@N and r.id_kontr <> p.id_kontr  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 81, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tt_tov_kontr  
 set id_kontr = p.id_kontr  
 -- select *  
 from M2..tt_tov_kontr r with (rowlock,index (ind1))  
 inner join #rasp_tov_kontr p on r.id_tov= p.id_tov and r.id_tt=p.id_tt  
 where r.number_r=@N and r.id_kontr <> p.id_kontr  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 82, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
 -- drop table #id_tov_perebros  
 -- drop table #id_tov_kontr_perebros  
 -- drop table #perebros  
 -- drop table #tov_dolya  
 -- drop table #rasp_tov_kontr  
  
  
 -- drop table #a  
 -- drop table #b  
  
   
  
**/   
select @j1 = @j1+1   
  
 end  
  
  
  
 -- drop table #zht  
  
    
 -----------------------------------------------------------------------------------------   
 -- поднять максимальный до кванта и минимальный на квант больше максимального  
 --после перекидки характеристик  
  
 update m2..tt_tov_kontr  
 set max_ost_tt_tov = case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob+1,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 --declare @N int = 22941  
 --select ttk.max_ost_tt_tov , tk.Kolvo_korob ,  
 --case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
   
 --left join #MCK mck on ttk.id_tt = mck.id_tt_mck   
    
 --inner join m2..tt on tt.id_TT = ttk.id_tt and tt.tt_format<>10  
   
       
 where --mck.id_tt_mck is null and   
 ttk.Number_r=@N   
 and max_ost_tt_tov <> case when ttk.max_ost_tt_tov>0 then master.dbo.maxz (tk.Kolvo_korob+1,ttk.max_ost_tt_tov ) else ttk.max_ost_tt_tov end  
 and ttk.tt_format_rasp not in (10)  
  
 update m2..rasp  
 set q_max_ost = case when ttk.q_max_ost>0 then master.dbo.maxz (tk.Kolvo_korob,ttk.q_max_ost ) else ttk.q_max_ost end  
 --declare @N int = 22941  
 --select ttk.q_max_ost , tk.Kolvo_korob ,  
 --case when ttk.q_max_ost>0 then master.dbo.maxz (tk.Kolvo_korob,ttk.q_max_ost) else ttk.q_max_ost end  
 from m2..rasp ttk with (index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
   
 --left join #MCK mck on ttk.id_tt = mck.id_tt_mck   
       
 where-- mck.id_tt_mck is null and    
 ttk.Number_r=@N   
 and ttk.q_max_ost <> case when ttk.q_max_ost>0 then master.dbo.maxz (tk.Kolvo_korob,ttk.q_max_ost ) else ttk.q_max_ost end  
  
  
 update m2..tt_tov_kontr  
 set q_min_ost= case when ttk.q_min_ost>0 and ttk.max_ost_tt_tov>0  
 then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.max_ost_tt_tov - tk.Kolvo_korob)) else ttk.q_min_ost end  
 --select ttk.q_min_ost, ttk.max_ost_tt_tov , tk.Kolvo_korob ,   
 --case when ttk.q_min_ost>0 and ttk.max_ost_tt_tov>0  
 --then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.max_ost_tt_tov - tk.Kolvo_korob)) else ttk.q_min_ost end  
 from m2..tt_tov_kontr ttk with (rowlock,index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
 where ttk.Number_r=@N and  
 ttk.q_min_ost <> case when ttk.q_min_ost>0 and ttk.max_ost_tt_tov>0  
 then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.max_ost_tt_tov - tk.Kolvo_korob)) else ttk.q_min_ost end  
  
  
 update m2..rasp  
 set q_min_ost= case when ttk.q_min_ost>0 and ttk.q_max_ost>0  
 then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.q_max_ost - tk.Kolvo_korob)) else ttk.q_min_ost end  
  
 --declare @N int = 22941  
 --select ttk.q_min_ost, ttk.q_max_ost , tk.Kolvo_korob ,   
 --case when ttk.q_min_ost>0 and ttk.q_max_ost>0  
 --then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.q_max_ost - tk.Kolvo_korob)) else ttk.q_min_ost end  
 from m2..rasp ttk with (index (ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr) ) on   
 ttk.id_tov=tk.id_tov and tk.id_kontr=ttk.id_kontr and tk.Number_r=@N  
 where ttk.Number_r=@N and  
 ttk.q_min_ost <> case when ttk.q_min_ost>0 and ttk.q_max_ost>0  
 then master.dbo.maxz ( 0 , master.dbo.minz (ttk.q_min_ost,ttk.q_max_ost - tk.Kolvo_korob)) else ttk.q_min_ost end  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1090, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 ------ ______________________________________  
  
  
 ---новый алгоритм.  
 -- только для товара, который распределять не весь и у которого есть минимум две характеристики на остатках  
 -- 1. по возрастанию дат отгружаем по каждой отдельно харакетеристике.  
 -- 2. не закрытие из rasp q_nuzno закрываем другими характеристиками  
 -- 3. переносим только те характеристики, которые по сроку более, чем половина срока годности  
 -- date_ost < datediff(day,-convert(int,tk.[srok_godnosti]/2),@date_rasp)    
 -- 4. остаток не полной коробки - на большее распеределение  
  
 -- остатки по датам  
 create table #ost_date (id_tov int , id_kontr int  , date_ost date ,f_ost real , r_ost real)  
 -- распределение по точкам  
 create table #rasp_date (id_tov int , id_kontr int , id_tt int ,id_kontr_new int , q_nuzno real , q_nuzno_r real)  
 -- как раз распределение по уникальной  id_tov,id_tt с (id_kontr и date_ost), если не пусто, то на id_kontr_new   
 create table #add_k_date (id_tov int , id_tt int , id_kontr int  , id_kontr_new int , date_ost date ,q_nuzno real )  
 create  unique clustered index ind1 on #add_k_date  (id_tov , id_tt)  
  
  
 --delete from #ost_date   
 --delete from #rasp_date  
  
 --declare @N as int =33834   
 insert into #ost_date   
 select tkd.id_tov , tkd.id_kontr , tkd.date_ost , tkd.q_ost_sklad , tkd.q_ost_sklad  
 from m2..tov_kontr_date   tkd  
 inner join M2..tov_kontr   tk on tk.Number_r = @N and tk.id_tov = tkd.id_tov   
 and tk.id_kontr = tkd.id_kontr and tk.rasp_all =0  
 -- проверить, что ставлю rasp_all на все kontr  
 where tkd.Number_r = @N and tkd.q_ost_sklad>0.01  
 and tkd.id_tov in   
 (select tkd.id_tov   
 from m2..tov_kontr   tkd  
 where tkd.Number_r = @N and tkd.q_ost_sklad>0.01  
 group by tkd.id_tov  
 having COUNT(*)>1)  
  
  
 insert into #rasp_date  
 select r.id_tov , r.id_kontr , r.id_tt , null , r.q_nuzno , r.q_nuzno  
 from M2..rasp   r  
 inner join M2..tov_kontr   tk on tk.Number_r = @N and tk.id_tov = r.id_tov   
 and tk.id_kontr = r.id_kontr  and tk.rasp_all =0  
 where r.Number_r = @N  and r.q_nuzno>0.01  
 and r.id_tov in   
 (select tkd.id_tov   
 from m2..tov_kontr   tkd  
 where tkd.Number_r = @N and tkd.q_ost_sklad>0.01  
 group by tkd.id_tov  
 having COUNT(*)>1)  
  
 -- из распределения на самую свободную старую  
  
  
 declare @q1 int = 0  
 while @q1 <10000  
 begin  
  
 delete from #add_k_date  
 insert into #add_k_date  
 select a.id_tov ,  a.id_tt , a.id_kontr  ,null, a.date_ost  ,a.q_r  
 from   
 (Select rd.id_tov , rd.id_kontr , rd.id_tt , rd.q_nuzno_r q_r, od.date_ost ,  
 ROW_NUMBER() over (partition by rd.id_tov , rd.id_kontr order by od.date_ost ,   
  od.r_ost- rd.q_nuzno_r  ) rn  
 from #rasp_date rd  
  
 inner join #ost_date od on rd.id_tov = od.id_tov and rd.id_kontr = od.id_kontr  
 -- чтоб пересчет делать по коробкам  
  
 where rd.q_nuzno_r>0.01  
 -- только если остатка хватает на полное закрытие q_nuzno_r  
 and od.r_ost> rd.q_nuzno_r   -0.01  
  
 )a  
 where a.rn =1  
  
 update #rasp_date  
 set q_nuzno_r = 0   
 from #rasp_date rd  
 inner join #add_k_date ad on rd.id_tov = ad.id_tov and rd.id_kontr = ad.id_kontr and rd.id_tt = ad.id_tt  
  
 update #ost_date  
 set r_ost = od.r_ost  - ad.q_nuzno  
 from #ost_date od  
 inner join #add_k_date ad on od.id_tov = ad.id_tov and od.id_kontr = ad.id_kontr and od.date_ost = ad.date_ost  
 select @q1 = @q1+1  
  
 -- выход из цикла есть ничего больше распределять  
 if not exists (select * from #add_k_date)  
 select @q1 = 1000000  
  
 end  
  
  
 -- тут нужно сравнивать количество в коробках, чтоб не получился перебор, например  
 -- ищем в распределении, остались ли не закрытие и распределяем на самые старые свободные  
 --declare  @q1 int  
 --declare @N as int =33834   
  
 Set @q1 = 0  
 while @q1 <10000  
 begin  
  
  
 delete from #add_k_date  
 insert into #add_k_date  
  
 select a.id_tov  ,  a.id_tt , a.id_kontr , a.id_kontr_new , a.date_ost , a.q_r  
 from   
 (Select rd.id_tov , rd.id_kontr , rd.id_tt  ,od.id_kontr id_kontr_new ,   
 -- новое количество q_nuzno_r для характеристики id_kontr_new  
 ceiling(rd.q_nuzno_r/ tk2.Kolvo_korob -0.01 ) * tk2.Kolvo_korob q_r,  
 od.date_ost ,  
 ROW_NUMBER() over (partition by rd.id_tov , rd.id_kontr  order by od.date_ost ,  
 od.r_ost - ceiling(rd.q_nuzno_r/ tk2.Kolvo_korob -0.01) * tk2.Kolvo_korob  ) rn  
 from #rasp_date rd  
 inner join #ost_date od on rd.id_tov = od.id_tov   
 --and od.r_ost>0.01  
  
 inner join M2..tov_kontr   tk2 on tk2.Number_r = @N and od.id_tov = tk2.id_tov  
    and od.id_kontr=tk2.id_kontr  
            
 where rd.q_nuzno=rd.q_nuzno_r --только полные магазины переносить  
 and od.r_ost> ceiling(rd.q_nuzno_r/ tk2.Kolvo_korob -0.01 ) * tk2.Kolvo_korob - 0.01  
 )a  
 where a.rn =1  
  
 update #rasp_date  
 set q_nuzno_r = 0, id_kontr_new = ad.id_kontr_new , q_nuzno = ad.q_nuzno -- новое q_nuzno  
 from #rasp_date rd  
 inner join #add_k_date ad on rd.id_tov = ad.id_tov and rd.id_kontr = ad.id_kontr and rd.id_tt = ad.id_tt  
  
 update #ost_date  
 set r_ost = od.r_ost  - ad.q_nuzno  
 from #ost_date od  
 inner join #add_k_date ad on od.id_tov = ad.id_tov and od.id_kontr = ad.id_kontr_new and od.date_ost = ad.date_ost  
 select @q1 = @q1+1  
  
 -- выход из цикла есть ничего больше распределять  
 if not exists (select * from #add_k_date)  
 select @q1 = 1000000  
  
 end  
  
  
 -- а теперь найти дырки в датах и при разнице более на 10% от даты распределения перенести вниз, но не менее 7 дней  
 -- c id_kontr и date_ost (заполнены) на  id_kontr_new и date_ost_new  (пустые)  
 create table #ost_date_2 (id_tov int , id_kontr int  , id_kontr_new int , date_ost date   
 , date_ost_new date ,f_ost real , r_ost real)  
 create table #add_k_date_2 (id_tov int , id_tt int , id_kontr int  , id_kontr_new int   
 , date_ost date , date_ost_new date , q_nuzno real  , q_nuzno_old real)  
  
  
 --declare @q1 int  
 declare @q2 int = 0 , @q3 int = 0  
 --declare @N int = 33834    
  
 Set @q1 = 0  
 while @q1 <10000  
 begin  
  
 --declare @q2 int = 0  
  
 --declare @N int = 33834    
 delete from #ost_date_2  
 insert into #ost_date_2  
  
 --declare @N int = 33834   
 select a.id_tov , a.id_kontr , a.id_kontr_new , a.date_ost , a.date_ost_new , a.r_ost , a.r_ost  
 from   
 (  
 --declare @N int = 33834    
 Select tk2.Kolvo_korob, od.id_tov  , od1.id_kontr , od.id_kontr id_kontr_new  , od1.date_ost , od.date_ost date_ost_new  
 ,master.dbo.minz(od.r_ost,od1.f_ost - od1.r_ost)   r_ost ,  
 ROW_NUMBER() over (PARTITION by od.id_tov  , od.id_kontr ,od.date_ost  order by od.date_ost ) rn   
 from #ost_date od -- пустое  
  
  
 inner join #ost_date od1 on od.id_tov = od1.id_tov and od1.f_ost - od1.r_ost>0 -- значит что-то распределили  
  
 --and (DATEDIFF (DAY,od.date_ost,@date_rasp) > 1.1 * DATEDIFF (DAY,od1.date_ost,@date_rasp)    
  
 inner join M2..tov_kontr   tk1 on tk1.Number_r = @N and od1.id_tov = tk1.id_tov  
    and od1.id_kontr=tk1.id_kontr  
  
 inner join M2..tov_kontr   tk2 on tk2.Number_r = @N and od.id_tov = tk2.id_tov  
    and od.id_kontr=tk2.id_kontr  
  
 where master.dbo.minz(od.r_ost,od1.f_ost - od1.r_ost) > tk2.Kolvo_korob- 0.01 -- значит еще есть нераспределенный остаток  
 and od.date_ost < dateadd(day,-convert(int,tk2.[srok_godnosti]/2),@date_rasp)   
 and dateadd(DAY,tk2.srok_godnosti,od.date_ost) < DATEADD(day,tk1.srok_godnosti -2,od1.date_ost)  
   
 ) a   
 where a.rn=1  
  
 Set @q3 = 1  
  
 -- ищем в распределении, остались ли не закрытие и распределяем на самые старые свободные  
 Set @q2 = 0  
 while @q2 <10000  
 begin  
  
 --declare @N int = 33834    
  
 -- теперь тт для переноса - отдельно по каждой паре  переносов #ost_date_2  
 delete from #add_k_date_2  
 insert into #add_k_date_2  
  
  
 --declare @N int = 33834    
 select a.id_tov  ,  a.id_tt , a.id_kontr , a.id_kontr_new , a.date_ost , a.date_ost_new ,a.q_r , a.q_nuzno_old   
 from   
 (  
  
  
 Select rd.id_tov , rd.id_kontr , rd.id_tt  ,od.id_kontr_new id_kontr_new ,   
 master.dbo.minz (ceiling(rd.q_nuzno/ tk2.Kolvo_korob - 0.01) * tk2.Kolvo_korob , od.r_ost ) q_r,  
 od.date_ost , od.date_ost_new , master.dbo.minz ( rd.q_nuzno , od.r_ost ) q_nuzno_old,   
 ROW_NUMBER() over (partition by rd.id_tov , od.id_kontr_new, od.date_ost_new  order by   
 od.r_ost - ceiling(rd.q_nuzno/ tk2.Kolvo_korob -0.01) * tk2.Kolvo_korob ) rn  
 from #rasp_date rd  
 inner join #ost_date_2 od on rd.id_tov = od.id_tov and isnull(rd.id_kontr_new,rd.id_kontr) = od.id_kontr  
 --and od.r_ost>0.01  
  
 inner join M2..tov_kontr   tk2 on tk2.Number_r = @N and od.id_tov = tk2.id_tov  
    and od.id_kontr_new=tk2.id_kontr  
            
 where rd.q_nuzno_r=0 --только полностью перенесенные магазины переносить  
 and rd.q_nuzno>0.01  
 and od.r_ost > 0.01 --tk2.Kolvo_korob  
 --ceiling(rd.q_nuzno/ tk2.Kolvo_korob -0.01) * tk2.Kolvo_korob - 0.01  
 )a  
 where a.rn =1  
  
 update #rasp_date  
 set id_kontr_new = case when rd.id_kontr_new = ad.id_kontr_new then null else  ad.id_kontr_new end ,   
 q_nuzno = ad.q_nuzno  
 --select *  
 from #rasp_date rd  
 inner join #add_k_date_2 ad on rd.id_tov = ad.id_tov   
 --and isnull(rd.id_kontr_new,rd.id_kontr) = ad.id_kontr   
 and rd.id_tt = ad.id_tt  
  
 --select * from #add_k_date_2  
  
 -- новая  
 update #ost_date  
 set r_ost = od.r_ost  - ad.q_nuzno  
 --select *  
 from #ost_date od  
 inner join #add_k_date_2 ad on od.id_tov = ad.id_tov   
 and od.id_kontr = ad.id_kontr_new and od.date_ost = ad.date_ost_new  
  
 -- старая  
 update #ost_date  
 set r_ost = od.r_ost  + ad.q_nuzno_old  
 --select *  
 from #ost_date od  
 inner join #add_k_date_2 ad on od.id_tov = ad.id_tov and od.id_kontr = ad.id_kontr and od.date_ost = ad.date_ost  
  
 -- в новом переносе меняем остатки  
 update #ost_date_2  
 set r_ost = od.r_ost  - ad.q_nuzno  
 --select *  
 from #ost_date_2 od  
 inner join #add_k_date_2 ad on od.id_tov = ad.id_tov and od.id_kontr = ad.id_kontr and od.date_ost = ad.date_ost  
 and ad.id_kontr_new = od.id_kontr_new and ad.date_ost_new = od.date_ost_new  
  
  
 select @q2 = @q2+1  
  
 -- выход из цикла есть ничего больше распределять  
 if not exists (select * from #add_k_date_2)  
 begin  
  
 select @q2 = 1000000  
  
 -- если это первый вход и сразу 0 - то значит вообще выходим из вехнего  
 if @q3 = 1  
 select @q1 = 1000000  
  
 end  
  
 -- значит хоть, что-то нашлось  
 if @q1 <> 1000000  
 Set @q3 = 0  
  
 end  
  
  
 select @q1 = @q1+1  
  
 if not exists (select * from #ost_date_2)  
 select @q1 = 1000000  
  
  
  
 end  
  
  
  
 -- набрать на перенос ТТ из rasp  
  
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
    Select @N , r.id_tt , r.id_tov ,  rd.id_kontr_new ,r.id_kontr , 80  
 from M2..rasp r with (rowlock, index(ind1))  
 inner join #rasp_date rd on r.id_tov= rd.id_tov and r.id_tt=rd.id_tt  
 where r.number_r=@N  
 and rd.id_kontr_new is not null and r.id_kontr <> rd.id_kontr_new  
  
  
 update M2..rasp  
 set id_kontr = rd.id_kontr_new , q_nuzno = rd.q_nuzno  
 from M2..rasp r with (rowlock, index(ind1))  
 inner join #rasp_date rd on r.id_tov= rd.id_tov and r.id_tt=rd.id_tt  
 where r.number_r=@N  
 and rd.id_kontr_new is not null and r.id_kontr <> rd.id_kontr_new  
  
  
  
  
  
 update M2..tt_tov_kontr  
 set id_kontr = rd.id_kontr_new  
 from M2..tt_tov_kontr r with (rowlock,index (ind1))  
 inner join #rasp_date rd on r.id_tov= rd.id_tov and r.id_tt=rd.id_tt  
 where r.number_r=@N  
 and rd.id_kontr_new is not null and r.id_kontr <> rd.id_kontr_new  
  
  
  
  
  
 --__________________________________________________________________  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 90, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  -- выявляем, какие товары из складируемых не хватает товара и проставляем тип rasp_all в товарах  
  -- 0 - не весь, 1 весь остаток  
    
  update M2..tov_kontr  
  set rasp_all = case when tov.Skladir=0 or tov_kontr.rasp_all=1  
  then 1   
  else   
  case when rasp_i_t.q_ost_sklad_calc < rasp_i.q_nuzno_i * 1.1 then 1 else 0 end end ,  
    
  rasp_all_init = isnull(tov_kontr.rasp_all,  
  case when tov.Skladir=0 or tov_kontr.rasp_all=1  
  then 1   
  else   
  case when rasp_i_t.q_ost_sklad_calc < rasp_i.q_nuzno_i * 1.1 then 1 else 0 end end )  
  --declare @N int = 27247  
  --select *   
  from M2..tov_kontr with (rowlock , INDEX(IX_tov_kontr_1) )  
  inner join   
  (select rasp.id_tov , SUM (ceiling(rasp.q_nuzno / tk.Kolvo_korob ) * tk.Kolvo_korob) q_nuzno_i  
  from M2..rasp with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (rowlock , INDEX(PK_tov_kontr) ) on tk.id_tov=rasp.id_tov and tk.id_kontr=rasp.id_kontr and tk.Number_r=@N  
  where rasp.number_r=@N   
  group by rasp.id_tov ) rasp_i on tov_kontr.id_tov=rasp_i.id_tov   
    
  inner join   
  (select tk.id_tov , SUM(tk.q_ost_sklad_calc) q_ost_sklad_calc  
  from M2..tov_kontr tk with (rowlock , INDEX(IX_tov_kontr_1) )  
  where tk.number_r=@N   
  group by tk.id_tov ) rasp_i_t on tov_kontr.id_tov=rasp_i_t.id_tov   
    
  inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=tov_kontr.id_tov and tov.Number_r=@N  
  where tov_kontr.Number_r=@N   
    
    
    
  
  insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 91, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 --**/  
  
 -- если товара для распределения меньше более 20% чем, нужно по всем,   
 -- то переделать q_nuzno на N пропоциональную по продажам, вместо 2 наибольшей  
 -- , что сумму этих продаж стала больше 100% от остатка.  
  
  --declare @n int = 29169 , @id_job int = 77 , @date_rasp date = {d'2016-03-27'}  
 create table #vib_rn ( id_tov int , rn int)  
 insert into #vib_rn  
 select a.id_tov , max(w.rn) rn  
 from   
  (select rasp_i.id_tov , rasp_i_t.q_ost_sklad_calc , rasp_i.q_fo from  
  (select rasp.id_tov ,SUM(rasp.q_FO) q_fo , SUM (ceiling(rasp.q_nuzno / tk.Kolvo_korob ) * tk.Kolvo_korob) q_nuzno_i  
  from M2..tov_kontr tk with (rowlock , INDEX(IX_tov_kontr_1) )   
  inner join M2..rasp with (  INDEX (PK_rasp))   
  on tk.id_tov=rasp.id_tov and tk.id_kontr=rasp.id_kontr and rasp.number_r=@N   
  where  tk.Number_r=@N  
  group by rasp.id_tov ) rasp_i   
  inner join   
  (select tk.id_tov , SUM(tk.q_ost_sklad_calc) q_ost_sklad_calc  
  from M2..tov_kontr tk with (rowlock , INDEX(IX_tov_kontr_1) )  
  where tk.number_r=@N   
  group by tk.id_tov ) rasp_i_t on rasp_i.id_tov=rasp_i_t.id_tov   
  where q_nuzno_i > rasp_i_t.q_ost_sklad_calc * 1.2  
  ) a  
  left join   
  (Select w.id_tov , w.rn , SUM(w.Fact) fact  
  from #w_all w  
  group by w.id_tov , w.rn ) w on a.id_tov = w.id_tov  
  where w.fact * 1.2 > (a.q_ost_sklad_calc +a.q_fo )   
  group by a.id_tov  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 911, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
    
 create table #vib_malo ( id_tov int)  
 insert into #vib_malo  
  select rasp_i.id_tov from  
  (select rasp.id_tov ,SUM(rasp.q_FO) q_fo , SUM (ceiling(rasp.q_nuzno / tk.Kolvo_korob ) * tk.Kolvo_korob) q_nuzno_i  
  from M2..rasp with (  INDEX (ind1))  
  inner join M2..tov_kontr tk with (rowlock , INDEX(PK_tov_kontr) )   
  on tk.id_tov=rasp.id_tov and tk.id_kontr=rasp.id_kontr and tk.Number_r=@N  
  where rasp.number_r=@N   
  group by rasp.id_tov ) rasp_i   
  inner join   
  (select tk.id_tov , SUM(tk.q_ost_sklad_calc) q_ost_sklad_calc  
  from M2..tov_kontr tk with (rowlock , INDEX(IX_tov_kontr_1) )  
  where tk.number_r=@N   
  group by tk.id_tov ) rasp_i_t on rasp_i.id_tov=rasp_i_t.id_tov   
  where q_nuzno_i > rasp_i_t.q_ost_sklad_calc * 1.2  
    
  --update m2..rasp  
  --set q_nuzno =   
    
  --declare @n int = 29169 , @id_job int = 77 , @date_rasp date = {d'2016-03-27'}  
    
  -- произвести замену планов продаж и товара, который нужно привести   
  update m2..rasp  
  set q_nuzno =  master.dbo.maxz( master.dbo.maxz( r.q_min_ost  , w.Fact) - r.q_FO,0) , q_plan_pr = master.dbo.maxz( r.q_min_ost , w.Fact) ,  
  q_ko_ost = r.q_ko_ost + r.q_plan_pr - master.dbo.maxz( r.q_min_ost , w.Fact) --, q_min_ost = 0   
  from M2..rasp r (rowlock)  
  inner join #w_all w on r.id_tov = w.id_tov and r.id_tt = w.id_tt  
  inner join #vib_rn v on w.id_tov = v.id_tov and w.rn = v.rn  
    
  left join #rasp_for_sales_3 rs on rs.id_tov = r.id_tov and rs.id_tt = r.id_tt  
     
  --inner join m2..tt on tt.id_TT = r.id_tt and tt.tt_format<>10  
     
  where r.number_r = @N and rs.id_tov is null  
    and r.tt_format_rasp not in (10)  
     
  update m2..tt_tov_kontr  
  set q_plan_pr = w.Fact --, q_min_ost = 0 , min_ost_tt_tov = 0  
  from M2..tt_tov_kontr r (rowlock)  
  inner join #w_all w on r.id_tov = w.id_tov and r.id_tt = w.id_tt  
  inner join #vib_rn v on w.id_tov = v.id_tov and w.rn = v.rn  
    
  left join #rasp_for_sales_3 rs on rs.id_tov = r.id_tov and rs.id_tt = r.id_tt  
    
  --inner join m2..tt on tt.id_TT = r.id_tt and tt.tt_format<>10  
        
  where r.number_r = @N and rs.id_tov is null  
    and r.tt_format_rasp not in (10)  
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 912, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
    
  --/**  
 ----------------------____________________   
 -- -1 проход - закрываем 0 и меньше 0 по остаткам -- убрать товары, с нехваткой боле 20%  
       
     create table #rasp (number_r int, id_tov int, id_tt int, id_kontr int , rn_r int,  
   znach real ,  sort_r int,  prohod int, sort_ost real , sort_pr real , p1 int , znach_sum int, znach_sum_narast int , rn_gr int, type_add_kor int ,  
   tt_format_rasp int , id_group_rasp int, price_rasp int, koef_ost_pr_rasp int)  
       
       
     create clustered index ind1 on #rasp (id_tt  , id_tov)  
     create unique index ind2 on #rasp (id_tt  , id_tov , prohod)  
       
       
     /**  
     insert into #rasp  
      
  select   
  @N number_r, rasp.id_tov , rasp.id_tt , rasp.id_kontr ,-1 rn_r ,  
  rasp.Kolvo_korob znach   
  , 0 sort_r, 0 prohod , rasp.q_plan_pr, 0 sort_pr , 0 p1 , 0 , 0 , 0   
  from  
  (  
  -- declare @N int  
  select rasp.id_tov , rasp.id_tt , rasp.id_kontr , tk.Kolvo_korob , floor(tk.q_ost_sklad_calc / tk.Kolvo_korob) q_ost_sklad_kor  
  , ROW_NUMBER() over (PARTITION by rasp.id_tov order by rasp.id_tt desc ) sort_ost , rasp.q_plan_pr  
  from M2..rasp with (  INDEX (ind1))  
  inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=rasp.id_tov and tov.Number_r=@N  
  inner join M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1)) on tk.id_tov=rasp.id_tov and tk.id_kontr=rasp.id_kontr and tk.Number_r=@N  
    
  left join M2..ZC_tt_tov zc   on zc.id_tov = rasp.id_tov and zc.id_tt = rasp.id_tt  
    
  where rasp.number_r=@N and rasp.q_FO<=0.1 and rasp.q_nuzno>0  
  and isnull(ZC.Status,0)<>2  
  ) rasp  
 left join #vib_malo v on rasp.id_tov = v.id_tov   
  
 where rasp.sort_ost <= q_ost_sklad_kor and v.id_tov is null  
   
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 913, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
  
  
 insert into M2..raspr_hystory   
  ([number_r]  
  ,[id_tov]  
  ,[id_tt]  
  ,[id_kontr]  
  ,[rn_r]  
  ,[znach]  
  ,[sort_rz]  
  ,[prohod]  
  ,[sort_ost]  
  ,[sort_pr])  
 select   
 number_r,   
 id_tov ,   
 id_tt ,   
 id_kontr  ,   
 rn_r ,  
 znach  ,    
 sort_r ,    
 prohod ,   
 sort_ost  ,   
 sort_pr   
  from #rasp  
  
  
 update M2..rasp   
 set q_raspr = rasp1.znach , q_ko_ost = rasp.q_ko_ost +rasp1.znach  
 --select q_raspr , rasp1.znach , q_ko_ost, rasp.q_ko_ost +rasp1.znach  
 from M2..rasp with (rowlock, index(ind1))  
 inner join #rasp rasp1 on rasp1.id_tt=rasp.id_tt and rasp1.id_tov=rasp.id_tov  
 where rasp.number_r=@N  
  
 delete from #rasp   
  
 **/  
     
 ----------------------____________________   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 914, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
  
 -- первое распределение товара - rasp_all=0 - q_nuzno, rasp_all=1 q_ost_sklad_calc пропорционально q_nuzno - 1 коробка  
  
  select rasp.id_tov , rasp.id_kontr , SUM (ceiling( (master.dbo.maxz(0,rasp.q_nuzno - rasp.q_raspr)) / tk.Kolvo_korob ) * tk.Kolvo_korob) q_nuzno_i  
  , SUM(rasp.q_raspr) q_raspr_fact  
  into #rasp11  
  from M2..rasp with (rowlock , INDEX (ind1))  
  inner join M2..tov_kontr tk with (rowlock , INDEX(PK_tov_kontr) )on tk.id_tov=rasp.id_tov and tk.id_kontr=rasp.id_kontr and tk.Number_r=@N  
  where rasp.number_r=@N   
    
  group by rasp.id_tov , rasp.id_kontr  
  having SUM (ceiling( (master.dbo.maxz(0,rasp.q_nuzno - rasp.q_raspr)) / tk.Kolvo_korob ) * tk.Kolvo_korob) >0  
    
    
  insert into #rasp  
  select   
  @N number_r, rasp.id_tov , rasp.id_tt , rasp.id_kontr ,0 rn_r ,  
  case tk.rasp_all when 0 then ceiling((rasp.q_nuzno- rasp.q_raspr) / tk.Kolvo_korob ) * tk.Kolvo_korob  
  when 1 then   
  
  case when rasp.q_max_ost < = 0 then  
  master.dbo.minz (   
  ceiling((rasp.q_nuzno - rasp.q_raspr )/ tk.Kolvo_korob ) * tk.Kolvo_korob ,   
  master.dbo.maxz(0,floor((rasp.q_nuzno - rasp.q_raspr) / rasp_i.q_nuzno_i * (tk.q_ost_sklad_calc - q_raspr_fact ) / tk.Kolvo_korob ) -1 )* Kolvo_korob)  
  else  
  master.dbo.maxz ( 0 ,   
  master.dbo.minz ( floor((rasp.q_max_ost - rasp.q_FO)/ Kolvo_korob) * Kolvo_korob ,   
    
  master.dbo.minz (   
  ceiling((rasp.q_nuzno - rasp.q_raspr )/ tk.Kolvo_korob ) * tk.Kolvo_korob ,   
  master.dbo.maxz(0,floor((rasp.q_nuzno - rasp.q_raspr) / rasp_i.q_nuzno_i * (tk.q_ost_sklad_calc - q_raspr_fact) / tk.Kolvo_korob ) -1 )* Kolvo_korob)  
  
  ) )  
    
  end  
  
  end znach   
    
  , -- добавил записать в sort_r коэф неообходимости товара, что потом можно было понять, какие самые не нужные    
   -- case tk.rasp_all when 0 then ceiling((rasp.q_nuzno- rasp.q_raspr) / tk.Kolvo_korob ) * tk.Kolvo_korob  
                                              -- АК добавил 27 09, чтоб в raspr_hystory  записать прогнозный остаток  
    case tk.rasp_all when 0 then ceiling((rasp.q_nuzno- rasp.q_raspr) / tk.Kolvo_korob ) * tk.Kolvo_korob + rasp.q_FO - rasp.q_plan_pr  
  when 1 then   
  
  case when rasp.q_max_ost < = 0 then  
  master.dbo.minz (   
  ceiling((rasp.q_nuzno - rasp.q_raspr )/ tk.Kolvo_korob ) * tk.Kolvo_korob ,   
  master.dbo.maxz(0,floor((rasp.q_nuzno - rasp.q_raspr) / rasp_i.q_nuzno_i * (tk.q_ost_sklad_calc - q_raspr_fact ) / tk.Kolvo_korob ) -1 )* Kolvo_korob)  
  else  
  master.dbo.maxz ( 0 ,   
  master.dbo.minz ( floor((rasp.q_max_ost - rasp.q_FO)/ Kolvo_korob) * Kolvo_korob ,   
    
  master.dbo.minz (   
  ceiling((rasp.q_nuzno - rasp.q_raspr )/ tk.Kolvo_korob ) * tk.Kolvo_korob ,   
  master.dbo.maxz(0,floor((rasp.q_nuzno - rasp.q_raspr) / rasp_i.q_nuzno_i * (tk.q_ost_sklad_calc - q_raspr_fact) / tk.Kolvo_korob ) -1 )* Kolvo_korob)  
  
  ) )  
    
  end  
  
  end   
    
  -- sort_r, 0 prohod , rasp.sort_ost , rasp.sort_pr -- АК добавил 27 09, чтоб в raspr_hystory  записать план продаж  
  sort_r, 0 prohod , rasp.q_plan_pr , rasp.sort_pr, 0 p1 ,0 , 0 , 0, 0  
  ,0 , 0 , 0 , 0  
  --into #rasp  
  from M2..rasp with (rowlock , INDEX (ind1))  
  inner join #rasp11 rasp_i on rasp.id_tov=rasp_i.id_tov and rasp.id_kontr=rasp_i.id_kontr  
  inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=rasp_i.id_tov and tov.Number_r=@N  
  inner join M2..tov_kontr tk with (rowlock , INDEX(PK_tov_kontr)) on tk.id_tov=rasp_i.id_tov and tk.id_kontr=rasp_i.id_kontr and tk.Number_r=@N  
    
  where rasp.number_r=@N  
    
  and   
    
  case tk.rasp_all when 0 then ceiling((rasp.q_nuzno- rasp.q_raspr) / tk.Kolvo_korob ) * tk.Kolvo_korob  
  when 1 then   
  
  case when rasp.q_max_ost < = 0 then  
  master.dbo.minz (   
  ceiling((rasp.q_nuzno - rasp.q_raspr )/ tk.Kolvo_korob ) * tk.Kolvo_korob ,   
  master.dbo.maxz(0,floor((rasp.q_nuzno - rasp.q_raspr) / rasp_i.q_nuzno_i * (tk.q_ost_sklad_calc - q_raspr_fact ) / tk.Kolvo_korob ) -1 )* Kolvo_korob)  
  else  
  master.dbo.maxz ( 0 ,   
  master.dbo.minz ( floor((rasp.q_max_ost - rasp.q_FO)/ Kolvo_korob) * Kolvo_korob ,   
    
  master.dbo.minz (   
  ceiling((rasp.q_nuzno - rasp.q_raspr )/ tk.Kolvo_korob ) * tk.Kolvo_korob ,   
  master.dbo.maxz(0,floor((rasp.q_nuzno - rasp.q_raspr) / rasp_i.q_nuzno_i * (tk.q_ost_sklad_calc - q_raspr_fact) / tk.Kolvo_korob ) -1 )* Kolvo_korob)  
  
  ) )  
    
  end  
  
  end > 0  
    
  and ( (rasp.koef_ost_pr_rasp is null   
    
  and rasp.tt_format_rasp in (2) ) or tk.rasp_all=0 )  
  
     and @sql_raspr = 0 and @only_zakaz=0  
    
 -- drop table #rasp11  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 92, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 insert into M2..raspr_hystory   
  ([number_r]  
  ,[id_tov]  
  ,[id_tt]  
  ,[id_kontr]  
  ,[rn_r]  
  ,[znach]  
  ,[sort_rz]  
  ,[prohod]  
  ,[sort_ost]  
  ,[sort_pr])  
 select   
    number_r,   
 id_tov ,   
 id_tt ,   
 id_kontr  ,   
 rn_r ,  
 znach  ,    
 sort_r ,    
 prohod ,   
 sort_ost  , -- q_plan_pr    
 sort_pr   
  from #rasp  
  
  
 update M2..rasp   
 set q_raspr = rasp.q_raspr + rasp1.znach , q_ko_ost = rasp.q_ko_ost +rasp1.znach  
 --select q_raspr , rasp1.znach , q_ko_ost, rasp.q_ko_ost +rasp1.znach  
 from M2..rasp with (rowlock, index(ind1))  
 inner join #rasp rasp1 on rasp1.id_tt=rasp.id_tt and rasp1.id_tov=rasp.id_tov  
 where rasp.number_r=@N  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 95, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 -- добавляем товар, но не более Коэф1  
  
  
  
 delete from #rasp  
  
  
  
 create table #ost_r  
 (id_tov int , id_kontr int , ОсталосьРаспр real)  
  
 insert into #ost_r  
 Select r_u.id_tov , r_u.id_kontr ,   
 floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
 (select id_tov ,id_kontr , SUM(q_nuzno) q_nuzno, SUM(q_raspr) q_raspr  
 from m2..rasp with (  index(ind1))  
 where number_r=@N  
 group by id_tov , id_kontr ) r_u  
 inner join M2..tov_kontr with (  INDEX(PK_tov_kontr)) on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 and number_r=@N and tov_kontr.rasp_all=1  
    where floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001)>0  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 96, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
   --  вообще товар с tk.rasp_all = 0 уже весь распределился и здесь ничего не дораспределяется.  
     
     
 --declare @N int = 79777  
 Select ttk.id_tov , ttk.id_tt ,   
   
 case when ttk.tt_format_rasp=10 then 0 else  
 case when tk.rasp_all=1 then   
   
 master.dbo.maxz(0 ,   
 case when tk.srok_godnosti<4 then -0.4 when  tk.srok_godnosti<6 then -0.2 when  tk.srok_godnosti<10 then 0 else 0.2 end   
 * master.dbo.maxz( ttk.q_plan_pr , ttk.q_min_ost) / tk.Kolvo_korob )    
   
 else  
   
 master.dbo.maxz(0 ,  
    case when @id_zone in (338,4550) then 0.4 else 1 end   
 * master.dbo.maxz( ttk.q_plan_pr , ttk.q_min_ost) / tk.Kolvo_korob )   
   
 end end Koef1 ,   
   
 tk.Kolvo_korob Kolvo_korob_koef1   
 into #koef1   
 from M2..tt_tov_kontr ttk with (  index(ind1))  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr)) on tk.id_kontr=ttk.id_kontr and tk.id_tov=ttk.id_tov and tk.Number_r=@N   
 --inner join M2..tt on tt.id_TT = ttk.id_tt  
 where ttk.Number_r = @N   
  
 create clustered index ind1 on #koef1 (id_tt , id_tov)  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 97, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
   
 -- АК - смотрим, а как было в прошлом распределении   
  
    
   create table #raspr_last (id_tt int, id_tov int, q_nuzno real, q_raspr real)  
   insert into #raspr_last  
     
   --declare @N int = 71366, @date_rasp date = '2018-10-16'  
   SELECT   top 1 with ties  r.id_tt , isnull(tov.id_tov_pvz ,r.id_tov) id_tov , r.q_nuzno , r.q_raspr  
   FROM  M2..archive_Rasp   r   
   INNER JOIN M2..Raspr_zadanie  rz  ON r.Number_r = rz.Number_r  
   inner join M2..rasp tk   on tk.Number_r = @N and tk.id_tt =r.id_tt and tk.id_tov=r.id_tov  
   inner join m2..tov tov   on tov.Number_r = @n and tov.id_tov = r.id_tov  
   where rz.Date_r = DATEADD(day,-1,@date_rasp)  
   order by ROW_NUMBER() over (partition by rz.Date_r , r.id_tt , isnull(tov.id_tov_pvz ,r.id_tov) order by r.Number_r desc)   
   
   create unique clustered index ind1 on #raspr_last (id_tt , id_tov)  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9701, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
  
 --declare @N int = 78210  
 select tk.* , rasp.id_tt, rasp.q_ko_ost , rasp.q_FO, rasp.q_raspr, rasp.q_max_ost , rasp.q_nuzno ,   
 rasp.sort_ost, rl.q_nuzno q_nuzno_rl , rl.q_raspr q_raspr_rl, zc_Status, rasp.id_kontr_init  , rasp.q_plan_pr , rasp.q_zakaz  
 , rasp.tt_format_rasp  , rasp.price_rasp , rasp.koef_ost_pr_rasp  
 into #rasp9701  
 from M2..tov_kontr tk  with ( INDEX(IX_tov_kontr_1))  
 left join M2..rasp with (INDEX (ind1))    
 on tk.id_kontr=rasp.id_kontr and tk.id_tov=rasp.id_tov and rasp.number_r = @N     
    left join #raspr_last rl on rl.id_tov = rasp.id_tov and rl.id_tt = rasp.id_tt  
 where tk.Number_r=@N  and tk.rasp_all=1  
 create clustered index ind1 on #rasp9701 (id_tov , id_kontr)   
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9702, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
   
 -- новый кусок, ограничение по остатку на вечер по группам, не более 25% в сумме о Прогноза Продаж   
-- Супы 10179  
--  Вторые блюда 10223  
  
    -- sum( (fo + raspr - q_plan_pr )*price) - сумма остатка не более 1.25% q_plan_pr * price  
    --  sum(raspr)*price < =  sum((q_plan_p*2.25 - fo) * price)  
  
  
  
 --drop table #tovgr    
 create table #tovgr  (id_tov int , id_group int,  price int , id_group_init int) -- koef_ost_pr int,  
 insert into #tovgr  
 select distinct t.id_tov , t.Group_raspr  , pr.Price , t.id_group  
    from m2..Tovari t   
    inner join [M2].[dbo].[Group_koef_raspr] g on t.Group_raspr = g.id_group and g.[type_gr] = 'КоэфОст'   
    inner join Reports..Price_1C_tov pr on pr.id_tov = t.id_tov  
      
    create unique clustered index ind1 on #tovgr (id_tov)  
      
  
   
 -- менем алгорим распределения  
  -- распредляем все, не смтря на остатки, но в сортировке по тт, группа товара  
  -- потом удалеем превышение по группам 10179, 10223  
  -- потом удаляем оставшееся до остатков  
  
 --declare @N int = 93952 , @date_rasp date = '2019-02-21'    
  
  
 --declare @N int = 78210  
 Select rasp_all_init , korob, rasp.q_nuzno ,  rasp.id_tov, rasp.id_tt , rasp.id_kontr ,   
  rasp.Kolvo_korob znach, rasp.q_plan_pr sort_ost , rasp.q_ko_ost + korob * rasp.Kolvo_korob sort_pr,  
 --ROW_NUMBER() over ( partition by rasp.id_tov , rasp.id_kontr order by rasp.q_ko_ost - rasp.q_min_ost + korob * rasp.Kolvo_korob) rn  
 ROW_NUMBER() over ( partition by rasp.id_tov , rasp.id_kontr order by   
      
 case when rasp.tt_format_rasp in (10) then -1   
   
 when rasp.tt_format_rasp in (4,12,14)  then 0 else 1 end, -- приоритет с заказами покупателей  
      
    case when rasp.q_ko_ost + (korob-1) * rasp.Kolvo_korob >= koef1.Koef1 * Kolvo_korob -- значит последняя коробка  
 then 1 else 0 end  ,  
   
    case when isnull(rasp.zc_status,4)=4 then 1 when rasp.zc_status=5 then 2 else 3 end  , -- чтоб избытки сначала распределять по 4 статусу, потом 5, а потом уже 2   
   
 case when kr.id_tov is not null and rasp.id_kontr_init= rasp.id_kontr then 0 else 1 end   , -- добавить, что если первое распределение, что положить в ТТ, где эта характ  
   
  master.dbo.maxz(0,floor( rasp.q_ko_ost/rasp.Kolvo_korob + korob )) , -- колво коробок в превышение - может случится для складир товаров, но которых мало  
   
 -- новый приоритет для последней коробки - если последняя коробка, то в первую очередь тех, кто в прошлом распределении не получил последнюю коробку.  
 case when rasp.q_nuzno < korob * rasp.Kolvo_korob then -- значит превысыли потребность  
 case when q_nuzno_rl<=q_raspr_rl then 1 else 0 end -- если в прошлом распределении была лишняя коробка, то в последнюю очередь  
 else 0 end  
   
 , rasp.q_ko_ost + korob * rasp.Kolvo_korob) rn ,   
   
 case when rasp.q_nuzno < korob * rasp.Kolvo_korob then -- значит превысыли потребность  
 case when q_nuzno_rl<=q_raspr_rl or isnull(rasp.zc_Status,4)=5 then 1 else 0 end -- если в прошлом распределении была лишняя коробка, то в последнюю очередь  
 else 0 end  p1  
  ,   
  ROW_NUMBER() over ( partition by  rasp.id_tt , isnull(tg.id_group,tov.id_group) order by   
  
 case when (--isnull(rasp.q_zakaz,0) >0 and  
 rasp.q_ko_ost + (korob-1) * rasp.Kolvo_korob < koef1.Koef1 * Kolvo_korob  )  
 or rasp.tt_format_rasp in (4,10,12,14)  then 0 else 1 end, -- приоритет с заказами покупателей  
  
    case when rasp.q_ko_ost + (korob-1) * rasp.Kolvo_korob >= koef1.Koef1 * Kolvo_korob -- значит последняя коробка  
 then 1 else 0 end  ,  
    
 case when isnull(rasp.zc_status,4)=4 then 1 when rasp.zc_status=5 then 2 else 3 end  , -- чтоб избытки сначала распределять по 4 статусу, потом 5, а потом уже 2   
   
 case when kr.id_tov is not null and rasp.id_kontr_init= rasp.id_kontr then 0 else 1 end   , -- добавить, что если первое распределение, что положить в ТТ, где эта характ  
   
  master.dbo.maxz(0,floor( rasp.q_ko_ost/rasp.Kolvo_korob + korob )) , -- колво коробок в превышение - может случится для складир товаров, но которых мало  
   
 -- новый приоритет для последней коробки - если последняя коробка, то в первую очередь тех, кто в прошлом распределении не получил последнюю коробку.  
 case when rasp.q_nuzno < korob * rasp.Kolvo_korob then -- значит превысыли потребность  
 case when q_nuzno_rl<=q_raspr_rl then 0 else 1 end -- если в прошлом распределении была лишняя коробка, то в последнюю очередь  
 else 1 end  
   
 , rasp.q_ko_ost + korob * rasp.Kolvo_korob  
  ) rn_gr -- сначала убирать с большими номерами  
    
   ,rasp.tt_format_rasp  ,   
    case when rasp.koef_ost_pr_rasp is not null then tov.id_group_rasp end id_group_rasp ,   
    rasp.price_rasp,   
    rasp.koef_ost_pr_rasp  
    
 into #rasp9702  
 from #rasp9701 rasp  
   
 left join #tovgr tg on tg.id_tov = rasp.id_tov -- значит товар есть в ограничении распределения по группе - добавить по 1 коробке  
   
 left join #q_sobr_kontr kr on kr.id_tov = rasp.id_tov and kr.id_kontr = rasp.id_kontr  
   
 --inner join M2..tt on tt.id_TT = rasp.id_tt  
   
 inner join M2..tov   tov on tov.id_tov = rasp.id_tov and tov.Number_r = @N  
   
 --inner join m2..Tovari t  on rasp.id_tov = t.id_tov   
   
 inner join M2..Korob_add on 1=1  
   
 left join m2..WorkingDaysAndWeekends w on w.date_w = dateadd(day,1,@date_rasp)  -- следующий день после дня распределения  
   
 inner join #koef1 koef1 on rasp.id_tov=koef1.id_tov and rasp.id_tt=koef1.id_tt  
 and ((  
 not (rasp.tt_format_rasp in (4,14)  and w.type_pr=1  and rasp.srok_godnosti<8 )  
 and (rasp.q_ko_ost + (korob-1) * rasp.Kolvo_korob < koef1.Koef1 * Kolvo_korob   
 --+ case when tg.id_tov is not null and rasp.tt_format_rasp in (2,3) and not (tov.raspr_double=1 and tov.raspr_d_1_2=1)   
 + case when rasp.koef_ost_pr_rasp is not null and not (tov.raspr_double=1 and tov.raspr_d_1_2=1)   
 --and tov.id_group not in (65, 10174, 10176) --но ФРОВ без лишней коробки  
 then Kolvo_korob else 0 end -- добавить 1 коробку, если ограничение по группе  
 )  )  
   
 --or(rasp.rasp_all_init=0 and rasp.rasp_all=1 and (korob -1 ) * (rasp.Kolvo_korob)  < rasp.q_nuzno  ))   
   
 or ( rasp.tt_format_rasp in (4,14)  and w.type_pr=1  and rasp.srok_godnosti<8   
  and rasp.q_ko_ost + korob* rasp.Kolvo_korob <= 0.3 * rasp.Kolvo_korob  )  
    
 or  (rasp.tt_format_rasp=10 and korob * rasp.Kolvo_korob + rasp.q_raspr <=rasp.q_nuzno +0.01 ))   
    
 left join #tov_only_VV t_vv on t_vv.id_tov = rasp.id_tov  
    
    
 where -- tk.Number_r=@N  and tk.rasp_all=1 and   
 -- не более макс остатка  
 rasp.q_FO + rasp.q_raspr + korob * rasp.Kolvo_korob   
 <= case when q_max_ost>0 then q_max_ost else rasp.q_FO + rasp.q_raspr + korob * rasp.Kolvo_korob end   
  
    and @sql_raspr = 0 and @only_zakaz=0  
  
    and not ( rasp.tt_format_rasp <> 2 and  (@hour_raspr between 4 and 7 or t_vv.id_tov is not null) and tov.raspr_double=1 ) -- не распределять иные форматы, если утром или будет еще распр и скоропорт  
  
   
   
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9703, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
 --and tk.id_tov = 17148  
   
   
  
        
   
 truncate table #rasp  
  
 --declare @N int = 93643 , @date_rasp date = '2019-02-19'    
    insert into #rasp  
   
      
   
 Select @N , rasp.id_tov, rasp.id_tt , rasp.id_kontr ,1 rn_r , znach , rasp.rn sort_r , korob prohod, rasp.sort_ost , rasp.sort_pr ,   
 p1 , znach*price_rasp znach_sum, 0 znach_sum_narast , rasp.rn_gr rn_gr ,  
 ROW_NUMBER() over ( partition by rasp.id_tov, rasp.id_tt order by korob desc) type_add_kor -- просто в обратную сортировка прохода, те 1 - последняя коробка  
 , tt_format_rasp  , id_group_rasp , price_rasp, rasp.koef_ost_pr_rasp  
 from #rasp9702 rasp  
  
  
     insert into m2..rasp_temp  
     (number_r , id_tov , id_tt , id_kontr  , rn_r ,  
   znach  ,  sort_r ,  prohod , sort_ost  , sort_pr  , p1  , znach_sum , znach_sum_narast , rn_gr , type_add_kor  ,  
   tt_format_rasp , id_group_rasp , price_rasp , koef_ost_pr_rasp )  
  select *  
     from #rasp  
       
  
 -- для утконоса проверить, что нет запрещенных характеристик, если есть, то перекинуть на другие, которые есть на остатках  
 -- в [M2].[dbo].[rasp_Kontr_iskl] - type_isk = 0 , значит нельзя эту характеристику, = 1 = наоборот, только эту и можно грузить  
   
 --Declare @N int = 4998  
 --drop table #rasp_Kontr_iskl  
 create table #rasp_Kontr_iskl (id_tt int, id_tov int , id_kontr int , id_kontr_new int)  
   
 --Declare @N int = 98507  
 insert into #rasp_Kontr_iskl  
 select distinct r.id_tt , r.id_tov , r.id_kontr , 0  
 from #rasp r  
 inner join   
 (  
  --Declare @N int = 4456  
  select isnull(t.id_tov_pvz ,rk.id_tov ) id_tov, rk.id_kontr , rk.id_tt  
 from [M2].[dbo].[rasp_Kontr_iskl] rk  
 inner join M2..tov t on t.Number_r = @N and rk.id_tov = t.id_tov   
 where rk.type_isk=0  
 ) rk  
 on r.id_tov = rk.id_tov and r.id_tt = rk.id_tt and r.id_kontr = rk.id_kontr  
  
 -- добавить все остальные характеристики, если type_isk = 1  
   
 insert into #rasp_Kontr_iskl  
 --Declare @N int = 7873  
 select distinct r.id_tt , r.id_tov , tk.id_kontr , 0  
 from m2..rasp r  
   
 inner join m2..tov_kontr tk on tk.Number_r = @N and tk.id_tov = r.id_tov  
  
 inner join  (  
  select isnull(t.id_tov_pvz ,rk.id_tov ) id_tov , rk.id_tt  
 from [M2].[dbo].[rasp_Kontr_iskl] rk  
 inner join M2..tov t on t.Number_r = @N and rk.id_tov = t.id_tov   
 where rk.type_isk=1  
 ) rk2 on rk2.id_tt = r.id_tt and rk2.id_tov=r.id_tov   
   
 left join  (  
  select isnull(t.id_tov_pvz ,rk.id_tov ) id_tov, rk.id_kontr , rk.id_tt  
 from [M2].[dbo].[rasp_Kontr_iskl] rk  
 inner join M2..tov t on t.Number_r = @N and rk.id_tov = t.id_tov   
 where rk.type_isk=1  
 ) rk0 on rk0.id_tt = r.id_tt and rk0.id_tov=r.id_tov and rk0.id_kontr = tk.id_kontr  
   
 left join #rasp_Kontr_iskl rki on rki.id_tov = r.id_tov and rki.id_tt = r.id_tt and rki.id_kontr<>tk.id_kontr  
   
 where r.number_r=@N  and rk0.id_kontr is null  
 and rki.id_tov is null  
  
 -- добавить 12628 Сторонняя розница 2313МП_Озон  
 insert into #rasp_Kontr_iskl  
 select distinct 12628 , 0 , 0 , 0  
   
  -- добавить 12743 Сторонняя розница 2438МП_ДКДЕЙЛИ  
 insert into #rasp_Kontr_iskl  
 select distinct 12743 , 0 , 0 , 0  
    
 -- добавить 12666 Сторонняя розница 2350МП_Комус  
 insert into #rasp_Kontr_iskl  
 select distinct 12666 , 0 , 0 , 0  
   
 -- добавить 12705 Сторонняя розница 2400МП_ВАЙЛДБЕРРИЗ  
 insert into #rasp_Kontr_iskl  
 select distinct 12705 , 0 , 0 , 0  
  
   
   
 -- а теперь добавить сюда все полные аналоги к товарам, которые выбраны у контрагента   
  --Declare @N int = 4456  
 insert into #rasp_Kontr_iskl  
 --Declare @N int = 4456  
 select distinct r.id_tt , r.id_tov , tk.id_kontr , 0  
 from m2..rasp r  
 inner join   
 (select distinct rk.id_tt from #rasp_Kontr_iskl rk ) rk on rk.id_tt=r.id_tt  
   
 inner join m2..tov_kontr tk0 on tk0.Number_r = @N and tk0.id_tov = r.id_tov and tk0.id_kontr = r.id_kontr_init -- первоначальная характеристика  
   
 inner join m2..tov_kontr tk on tk.Number_r = @N and tk.id_tov = r.id_tov and tk0.id_tov<> tk.id_tov -- не равен тому, что был первоначально - те уброать полные аналоги  
   
 left join #rasp_Kontr_iskl rki on rki.id_tov = r.id_tov and rki.id_tt = r.id_tt  
   
 where r.number_r=@N and rki.id_tov is null -- and ttk.id_tov<>t.id_tov -- и чтоб полный аналог просто не был равен товар из первичного  
 --  для Озона добавить также все, у которых id_kontr не равен первоначальному  
 and (r.id_tt not in ( 12628) or (r.id_tt in ( 12628) and  tk.id_kontr <> r.id_kontr_init) )  
   
   
 --select *  
 --Declare @N int = 4456  
 update #rasp_Kontr_iskl  
 set id_kontr_new = tk.id_kontr_new  
 from #rasp_Kontr_iskl ri  
 inner join  
 (  
  --Declare @N int = 4456  
 select top 1 with ties   
 tk.id_tt , tk.id_tov , tk.id_kontr  ,  tk0.id_kontr id_kontr_new   
 from   
 -- заказы Утконосу  
 (  
 select distinct r.id_tt , r.id_tov , r.id_kontr   
 from m2..rasp r  
 inner join   
 (select distinct rk.id_tt from [M2].[dbo].[rasp_Kontr_iskl] rk  ) rk on rk.id_tt=r.id_tt   
 where r.number_r=@N   
 ) tk  
  -- совпали с исключениями  
 inner join #rasp_Kontr_iskl ri on ri.id_tt = tk.id_tt and ri.id_tov = tk.id_tov and ri.id_kontr = tk.id_kontr  
   
 inner join M2..tov_kontr tk0 on tk0.Number_r = @N and tk0.id_tov = tk.id_tov  
 -- и не взять исключений  
 left join #rasp_Kontr_iskl rk on rk.id_tt = tk.id_tt and rk.id_tov = tk0.id_tov and rk.id_kontr = tk0.id_kontr  
   
 where rk.id_tov is null and tk0.q_ost_sklad>0  
   
 order by  ROW_NUMBER() over (partition by tk.id_tt , tk.id_tov order by tk0.q_ost_sklad desc)  
 ) tk on ri.id_tov = tk.id_tov and ri.id_kontr = tk.id_kontr and ri.id_tt = tk.id_tt  
   
    
   
 --select * from #rasp_Kontr_iskl  
   
 update #rasp  
 set id_kontr = ri.id_kontr_new , znach = tk.Kolvo_korob  
 from #rasp r  
 inner join #rasp_Kontr_iskl ri on r.id_tt = ri.id_tt and r.id_tov = ri.id_tov and r.id_kontr = ri.id_kontr  
 inner join m2..tov_kontr tk on tk.Number_r = @N and tk.id_tov= r.id_tov and tk.id_kontr = ri.id_kontr_new  
 where isnull(ri.id_kontr_new,0) > 0  
   
  
 delete #rasp  
 from #rasp r  
 inner join #rasp_Kontr_iskl ri on r.id_tt = ri.id_tt and r.id_tov = ri.id_tov and r.id_kontr = ri.id_kontr  
 where isnull(ri.id_kontr_new,0) = 0  
  
  --Declare @N int = 98507  
   insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
    Select @N , id_tt , id_tov ,  id_kontr_new ,id_kontr , 888   
    from #rasp_Kontr_iskl ri  
      
    
  
--- поправить утконос , если отгрузка более на 10% больше заказа  
create table #rki_del (id_tt int , id_tov int)  
declare @rki_del int =1  
  
--declare @N int = 2428  
  
while @rki_del=1  
begin  
  
truncate table #rki_del  
  
insert into #rki_del  
select r.id_tt , r.id_tov   
from  
(select r.id_tt , r.id_tov ,  SUM(r.znach) znach  
from #rasp r  
inner join m2..tov_kontr tk on tk.Number_r = @N and tk.id_tov= r.id_tov and tk.id_kontr = r.id_kontr  
  
inner join (select distinct rk.id_tt from [M2].[dbo].[rasp_Kontr_iskl] rk ) rk on rk.id_tt=r.id_tt  
group by r.id_tt , r.id_tov   
having count(*)>1  
) r  
inner join m2..tt_tov_kontr_init ttk on ttk.Number_r = @N and ttk.id_tov= r.id_tov and ttk.id_tt = r.id_tt  
where r.znach>ttk.q_min_ost*1.1  
  
delete #rasp  
from #rasp r  
inner join  
(select top 1 with ties r.id_tt ,r.id_tov , r.prohod  
from #rasp r  
inner join #rki_del rki on rki.id_tt = r.id_tt and rki.id_tov = r.id_tov  
order by ROW_NUMBER() over (partition by r.id_tt ,r.id_tov order by r.prohod desc)  
) a on a.id_tt = r.id_tt and a.id_tov = r.id_tov and r.prohod = a.prohod  
  
if not exists (select * from #rki_del)  
select @rki_del=0  
  
end  
  
  
  
  
  
  
  
  
  
      
  
    --inner join Reports..Price_1C_tov pr on pr.id_tov = rasp.id_tov  
  
  
/**  
update M2..tt_tov_kontr_init  
set tt_format_rasp = tt.tt_format , price_rasp = pr.Price , koef_ost_pr_rasp = gr.koef_ost_pr  
from M2..tt_tov_kontr_init ttk  
inner join M2..tt on tt.id_TT = ttk.id_tt  
inner join M2..Tovari t on t.id_tov = ttk.id_tov  
inner join Reports..Price_1C_tov pr on pr.id_tov = ttk.id_tov  
left join M2..Group_koef_raspr gr on gr.id_group = t.Group_raspr and gr.type_gr = 'КоэфОст'  
where ttk.Number_r=97288  
  
--select *  
update M2..tov_init  
set id_group_rasp = gr.id_group  
from M2..tov_init ttk  
inner join M2..Tovari t on t.id_tov = ttk.id_tov  
inner join M2..Group_koef_raspr gr on gr.id_group = t.Group_raspr and gr.type_gr = 'КоэфОст'  
where ttk.Number_r=97288  
  
**/  
  
    
--1. Распределяем на ТТ всех товаров по лишней 1 коробке.  
-- получилаось таблица #rasp  
  
--2.  
 -- считаем по каждому формату и группе, какой Запас и сравнение с нормативным, но остатков не больше, чем на складе   
  
 --id_group = 4000003  
/**   
2. Считаем нормативные Запас, убрав Товары, которых нет на остатках,   
по каждой Подгруппе по каждому Формату и получаем % этого остатка к ПлануПродаж.  
Разница с Нормативным % - как раз дает сумму Избытка или Нехватки целиком по Подгруппе.  
  
Пример. Прогнозный Остаток на утро - 300 тыс.   ПланПродаж - 1 млн руб.  
План%запас - 50%, Значит идеально распределение по группе 1.200 млн  
Но остатки на складе к распределению (но не более 1 лишней коробки) - 1.250 руб.  
По факту получилось 55% 550 тыс руб, те 50 тыс руб лишние. Распределение будет 1.250 руб.  
  
При этом с остатков снято 400 тыс руб  
Итого Распределение без снятия с остатков - 1650 (1.250 + 400)  
  
                   Запас    ПланПР    Расп       q_FO        ПроцЗапас  
2 4000003      1 285 324  4 284 415  971 160   4 409 248 0,255809103216522  
  
И всего В реестре на распр  4 919 940    -убрать 4 919 940 - 971 160 = 3 948 780  
  
  
**/  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777010, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
create table #rasp_gr_ubr (tt_format_rasp int, id_group_rasp int, Запас int, ПланПР int, Распр int, q_FO int   
, ПроцЗапас real,   
Распр_всего int, Убрать real)  
  
-- таблица с товарами, которые нужно удалить из распределения  
-- drop table #rasp_del_tov  
create table #rasp_del_tov (tt_format_rasp int, id_tov int, id_kontr int, Распр real , Распр_сняли  real , id_group_rasp int) -- znach real ,  ,znach_сняли real  
create unique clustered index ind1 on #rasp_del_tov (tt_format_rasp , id_tov , id_kontr)  
  
-- данные по каждому ТТ - понадобятся, чтоб быстро находить тек коэф запаса  
create table #rasp_gr_tt_ubr (id_tt int, id_group_rasp int, Запас real, ПланПР real, q_FO real)  
create unique clustered index ind1 on #rasp_gr_tt_ubr (id_tt , id_group_rasp)  
  
--drop table #rasp_gr_reestr   
create table #rasp_gr_reestr (tt_format_rasp int , id_group_rasp int , id_tt int, id_tt_old int,    
id_tov int , id_kontr int , znach real  , Распр real ,  Сняли real , ПроцЗапас real , price_rasp int , pri int ,  
prohod int, type_add_kor int  , pri_alg int)  
  
create clustered index ind1 on  #rasp_gr_reestr (id_tt ,id_group_rasp )  
create index ind3 on  #rasp_gr_reestr (tt_format_rasp ,id_tov , id_kontr )  
  
create table #rasp_gr_reestr_gr (id_tt int, id_group_rasp int, tt_format_rasp int, НормЗапас real,  Распр real )  
create clustered index ind1 on  #rasp_gr_reestr_gr (id_tt ,id_group_rasp )  
        
  
-- таблица с макс количеством коробок по товару на ТТ  
create table #rasp_tt_tov_max_kor (id_tt int, id_tov int, Макс_кол_кор int)  
create unique clustered index ind1 on #rasp_tt_tov_max_kor (id_tt,id_tov)  
  
-- таблица с количеством, сколько нужно еще грузить  
create table #rasp_tt_tov_dob_kor (id_tt int, id_tov int, МожноДоб int)  
create unique clustered index ind1 on #rasp_tt_tov_dob_kor (id_tt,id_tov)  
  
--drop table #rasp_gr_format  
create table #rasp_gr_format (tt_format_rasp int , id_group_rasp int ,  ПроцЗапас_групп real , колво_тт int  ,СнятьЕще bigint , ИтогоРаспр bigint ,ПроцИсправ real )  
create unique clustered index ind1 on #rasp_gr_format (tt_format_rasp , id_group_rasp)  
  
--drop table #rasp_gr_tt  
create table #rasp_gr_tt (id_tt int , id_group_rasp int , Запас real, ПланПР real, Распр real , q_FO real ,  
ПроцЗапас real , НормЗапас real , Снять_еще real  )  
create unique clustered index ind1 on #rasp_gr_tt (id_tt , id_group_rasp)  
  
  
-- найти по 1 строке с каждого магазина товаров, которые нужно снять, при этом Запас не будет более 50% коробки ниже норматива  
--drop table #rasp_tt_tov  
create table #rasp_tt_tov (id_tt int , tt_format_rasp int, id_group_rasp int , id_tov int , id_kontr int ,prohod int,Распр real  ,  
Распр_нараст_tt real ,  Распр_нараст_tov real,  rn_tt int ,  rn_tov int)  
create unique clustered index ind1 on #rasp_tt_tov (id_tt  , id_group_rasp , rn_tt desc)  
create unique index ind2 on #rasp_tt_tov (id_tov  , id_kontr ,tt_format_rasp , rn_tov desc)  
  
-- ТТ и товары, по которым больше норм запаса  
-- реестр 1  
--drop table #rasp_tt_tov_r1  
create table #rasp_tt_tov_r1 (id_tt int , tt_format_rasp int, id_group_rasp int , id_tov int , id_kontr int , prohod int,Распр real  ,  
--Распр_нараст_tt int ,  Распр_нараст_tov int ,    
rn_tov int , znach real ) --,  rn_tov int)  
--create unique clustered index ind1 on #rasp_tt_tov (id_tt  , id_group_rasp , rn_tt desc)  
create unique  clustered index ind2 on #rasp_tt_tov_r1 (id_tov  , id_kontr , tt_format_rasp , rn_tov desc)  
  
  
create table #rasp_tt_tov_r2 (id_tt int , tt_format_rasp int, id_group_rasp int , id_tov int , id_kontr int, prohod int,Распр real  ,  
--Распр_нараст_tt int ,  Распр_нараст_tov int ,    
rn_tov int , znach real  ) --,  rn_tov int)  
--create unique clustered index ind1 on #rasp_tt_tov (id_tt  , id_group_rasp , rn_tt desc)  
create unique  clustered index ind2 on #rasp_tt_tov_r2 (id_tov  , id_kontr , tt_format_rasp , rn_tov desc)  
  
-- вот реальная замена tt у тов. товар переносится из r1 в r2.  
--drop table #rasp_tt_tov_zam  
create table #rasp_tt_tov_zam (id_tov_old int , id_tov_new int , id_kontr_old int , id_kontr_new int ,id_tt_old int, id_tt_new int , prohod_old int, prohod_new int   
, znach_old real , znach_new real)  
  
  
create table #rasp_type_rr_gr (id_tt int , id_group_rasp int ,type_tt_gr int  ,распр int ,ПроцЗапас real , НормЗапас real , СнятьЕще real)  
create unique clustered index ind1 on  #rasp_type_rr_gr (id_tt , id_group_rasp)  
  
  
--drop table  #rasp_zam_last  
create table #rasp_zam_last (id_group_rasp int , tt_format_rasp int , id_tov int, id_tt_old int,  id_tt_new int ,  id_kontr int ,Распр real, znach real , prohod_old int)  
create unique clustered index ind1 on #rasp_zam_last (id_group_rasp , tt_format_rasp  , id_tov)  
  
create table #rasp_g (id_group_rasp int , tt_format_rasp int,  id_tt int, id_tov int, id_kontr int, Распр real , Снять_еще real,  znach real,КолвоКор int , prohod int)  
create  clustered index ind1 on #rasp_g ( tt_format_rasp  , id_tov , prohod)  
  
--drop table  #rasp_f  
create table #rasp_f (id_group_rasp int , tt_format_rasp int,  id_tt int, id_tov int, id_kontr int, Распр real , Снять_еще real, znach real, КолвоКор int, МожноДоб int , prohod int)  
create  clustered index ind1 on #rasp_f ( tt_format_rasp  , id_tov , prohod )  
  
  
     --declare @N int = 97288 , @date_rasp date = '2019-03-14'       
 create table #raspr_group (id_tt int, id_group int , распр int)  
      
  
  
  
--   тут теперь 2 цикла, если одновременно тт с форматом 2, + что-то из 4, 12, 14 по одним и те же товарам с rasp.koef_ost_pr_rasp is not null  
  
-- в первом цикле - распределить, что что попало в #rasp по 4, 12, 14  
-- потом убрать превышение по групам, и добавить в распределение rasp   
-- пересчитать остатки и запустить второй цикл на формат 2, чтоб уже все остатки распределить.  
  
  
  
-- перемещаем все из #rasp в #rasp_0  
Select *  
into #rasp_0  
from #rasp  
  
  
declare @ff int =1  
  
  
if not exists(  
select r.id_tov   
from #rasp r   
where r.koef_ost_pr_rasp is not null  
and  r.tt_format_rasp in (4, 12, 14)  
group by r.id_tov)   
  
Select @ff = 2 -- если нет чего-то из 4, 12, 14, то запустить сразу 2 цикл - просто ВкусВилл  
  
  
if @only_zakaz=1  
select @ff=3 -- если только заказы покупателей, то в этот цикл даже не заходим  
  
  
  
While @ff <=2  
begin  
  
truncate table #rasp  
  
if @ff =1 -- цикл для форматов  - убрать превышения по группам  
  
insert into #rasp  
select *  
from #rasp_0 r  
where r.tt_format_rasp in (4, 12, 14) and r.koef_ost_pr_rasp is not null  
  
else -- обычный цикл для ВВ   
  
insert into #rasp  
select *  
from #rasp_0 r  
where not ( r.tt_format_rasp in (4, 12, 14) and r.koef_ost_pr_rasp is not null )  
  
  
  
  
 --declare @N int = 97288  
   
  
truncate table #rasp_gr_ubr  
insert into #rasp_gr_ubr  
  
select a.tt_format_rasp , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , b.q_FO   
, (a.Распр + b.q_FO  - b.ПланПР ) /  b.ПланПР ПроцЗапас ,   
c.Распр_всего , c.Распр_всего - a.Распр Убрать  
  
from   
    (  select rasp.tt_format_rasp , rasp.id_group_rasp  ,   
  
      sum( rasp.znach*rasp.price_rasp  ) Распр    
  
      from #rasp rasp   
        
      --inner join m2..rasp r   on r.id_tt = r.id_tt and r.id_tov = r.id_tov and r.number_r = 97291  
       
    left join -- но не более, чем осталось распределить  
  
 #ost_r ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
   
 where rasp.sort_r<=isnull(ost_r.ОсталосьРаспр,0)  
 and rasp.id_group_rasp is not null  
 --and rasp.tt_format_rasp in (2,3)  
 and rasp.koef_ost_pr_rasp is not null  
 group by rasp.tt_format_rasp , rasp.id_group_rasp  
 ) a  
 inner join  
 (  
      select rasp.tt_format_rasp ,  tov.id_group_rasp  ,   
      SUM(rasp.q_FO *rasp.price_rasp) q_FO,  
      SUM(0.01 * rasp.q_plan_pr*rasp.koef_ost_pr_rasp*rasp.price_rasp )Запас,   
      sum( rasp.q_plan_pr *rasp.price_rasp) ПланПР      
      from  m2..rasp rasp      
      inner join M2..tov   on tov.Number_r = rasp.number_r and tov.id_tov = rasp.id_tov  
      where rasp.number_r=  @N and rasp.koef_ost_pr_rasp is not null  
 group by rasp.tt_format_rasp , tov.id_group_rasp  
 having sum( rasp.q_plan_pr *rasp.price_rasp)>1  
 ) b on a.id_group_rasp = b.id_group_rasp and a.tt_format_rasp = b.tt_format_rasp  
inner join  
   
(select rasp.tt_format_rasp , rasp.id_group_rasp  ,   
      sum( rasp.znach*rasp.price_rasp  ) Распр_всего   
      from #rasp rasp   
 where rasp.id_group_rasp is not null  
 --and rasp.tt_format_rasp in (2,3)  
 and rasp.koef_ost_pr_rasp is not null  
 group by rasp.tt_format_rasp , rasp.id_group_rasp  
 ) c on a.id_group_rasp = c.id_group_rasp and a.tt_format_rasp = c.tt_format_rasp  
  
  
  
  
   
--Declare @N int = 97253  
insert into m2..rasp_gr_ubr  
Select @N Number_r , * , GETDATE() date_add   
from #rasp_gr_ubr  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777020, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
  
  
  
truncate table #rasp_del_tov  
insert into #rasp_del_tov  
  
select rasp.tt_format_rasp , rasp.id_tov  , rasp.id_kontr ,  
  
     --  sum( rasp.znach) znach ,  
      sum( rasp.znach*rasp.price_rasp  ) Распр  ,  
     --  0 znach_сняли ,  
      0 Распр_сняли     , rasp.id_group_rasp        
  
      from #rasp rasp   
        
      --inner join m2..rasp r   on r.id_tt = r.id_tt and r.id_tov = r.id_tov and r.number_r = 97291  
       
    left join -- но не более, чем осталось распределить  
  
 #ost_r ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
   
 where rasp.sort_r>isnull(ost_r.ОсталосьРаспр,0)  
 --and rasp.tt_format_rasp in (2,3)  
 and rasp.koef_ost_pr_rasp is not null  
 group by rasp.tt_format_rasp , rasp.id_tov, rasp.id_kontr, rasp.id_group_rasp  
  
  
/**  
insert into m2..rasp_del_tov  
Select @N Number_r , * , GETDATE() date_add  , 1 type_add  
from #rasp_del_tov  
**/  
   
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777030, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
 --declare @N int = 97288    
  
  
truncate table #rasp_gr_tt_ubr  
insert into #rasp_gr_tt_ubr  
  
      select rasp.id_tt ,tov.id_group_rasp  ,   
      SUM(0.01 * rasp.q_plan_pr*rasp.koef_ost_pr_rasp*rasp.price_rasp )Запас,   
      sum( rasp.q_plan_pr *rasp.price_rasp) ПланПР,    
       SUM(rasp.q_FO *rasp.price_rasp) q_FO    
      from  m2..rasp rasp      
      inner join M2..tov   on tov.Number_r = rasp.number_r and tov.id_tov = rasp.id_tov  
      where rasp.number_r= @N and rasp.koef_ost_pr_rasp is not null   
 group by rasp.id_tt , tov.id_group_rasp  
 having sum( rasp.q_plan_pr *rasp.price_rasp)>1  
   
--Declare @N int = 97253  
insert into m2..rasp_gr_tt_ubr  
Select @N Number_r , * , GETDATE() date_add   
--into m2..rasp_gr_tt_ubr  
from #rasp_gr_tt_ubr  
   
  
   
    
  
/**  
  
Делаем Расчетный Реестр всего распределения  
  
ТТ и товары, Сумма на 1.650 тыс руб  
Самое основное - Разбить Этот реестр на 3 ПриоритетаРаспределения  
1  ПриоритетРаспределения   , первыми товарами идет те, которые нужно убирать в первую очередь, все лишние коробки.   
2  ПриоритетРаспределения   . Далее идут уже остальные коробки, кроме последней коробки   
3  ПриоритетРаспределения   , только последняя коробка каждого товара в последнюю очередь.  
Последние коробки по каждому тт - уже сортируются, так чтоб сначала шли те тт, в которых этот товар был недавно (по дате последнего распределения)  
  
Задача алгоритма - из этого реестра выбрать товаров на 400 тыс руб, так чтоб при этом Нормативный Запас был 55% по каждой ТТ.  
При этом, алгоритм, как может метить товар из реестра на "вывод", так и менять "местами" товары на разных ТТ.  
Менять местами нужно в случае, если по Подгруппе уже достигнуто 55%, а в распределении остался товар, который нужно снять с Остатка и Этот товар еще не полностью снят.  
Тогда алгоритм перекидывает Другой товар, делая Избыток в ПодГруппе, чтоб можно было снять Товар, которого нет на остатках.  
  
По таблице всегда легко рассчитать, сколько уже сняли товара, чтоб сравнить, сколько нужно еще снять.  
По таблице легко по каждому магазину рассчитать ПланЗапас и превышение над Нормативным.   
  
-- убрать возможность распределения коробок сверх 1 лишней  
-- учесть, что может быть вообще ни одна коробка может не пойти  
-- уметь из #rasp_gr_reestr получать текущее распределение и считать, сколько еще осталось  
  
   
  
**/  
  
  
  
  
  
  
  
  
truncate table #rasp_gr_reestr  
  
insert into #rasp_gr_reestr  
   
select r.tt_format_rasp , r.id_group_rasp ,  
  r.id_tt ,  
  0 id_tt_old ,-- откуде переброшен  
  r.id_tov ,  
  r.id_kontr ,  
  r.znach  ,  
  r.znach * r.price_rasp Распр  ,   
  0  Сняли, -- сюда пишем, когда сняли      
  
  rgu.ПроцЗапас ,  
  r.price_rasp ,  
 case when r.type_add_kor=1 then 1 when  r.prohod>1 then 2 else 3 end pri ,  
 r.prohod ,  
 type_add_kor,  
 0  
    
from #rasp r  
 inner join #rasp_gr_ubr rgu on r.tt_format_rasp  = rgu.tt_format_rasp and  r.id_group_rasp  = rgu.id_group_rasp  
where r.id_group_rasp is not null  
--and r.tt_format_rasp in (2,3)  
and r.koef_ost_pr_rasp is not null  
  
  
  
   
   
   
   
--Declare @N int = 97253  
insert into m2..rasp_gr_reestr  
Select @N Number_r , * , GETDATE() date_add  , 1 type_add  
--into m2..rasp_gr_reestr  
from #rasp_gr_reestr  
  
  
truncate table #rasp_tt_tov_max_kor  
insert into #rasp_tt_tov_max_kor  
select r.id_tt , r.id_tov , MAX(prohod) Макс_кол_кор  
from #rasp r  
group by r.id_tt , r.id_tov  
  
  
  
  
  
/**         
   
  
-- запрос - сколько распределено и сняли  
select tt_format_rasp , id_group_rasp , SUM(Распр) Распр , SUM(Сняли) Сняли  
from #rasp_gr_reestr  
--where id_group_rasp=10141  
group by tt_format_rasp , id_group_rasp  
 order by id_group_rasp  
   
select tt_format_rasp , id_group_rasp ,  Pri_alg , pri ,  
SUM(Распр) Распр , SUM(Сняли) Сняли  
from #rasp_gr_reestr  
where id_group_rasp=10141  
group by tt_format_rasp , id_group_rasp, Pri , pri_alg  
order by  pri_alg , pri  
  
  
       
 select *  
 from #rasp_gr_reestr  
 where id_group_rasp=4000003  
   
   
  
  
  
 select *  
 from #rasp_gr_tt  
 --left join   
 where id_group_rasp=10141  
 order by снять_еще  
   
 select id_group_rasp, sum(распр) распр, SUM(снять_еще)снять_еще  
 from #rasp_gr_tt  
 --where id_group_rasp=10141  
 group by id_group_rasp  
 order by id_group_rasp  
  
   
  
 select *  
 from  #rasp_tt_tov  
 where id_group_rasp=4000003  
   
 select sum(распр)  
 from  #rasp_tt_tov  
 where id_group_rasp=4000003  
    
  
 select *  
 from  #rasp_del_tov r  
 inner join M2..Tov t on r.id_tov = t.id_tov and t.Number_r = 98619  
 where  t.id_group_rasp=10141  
  
   
 select t.id_group_rasp, sum(распр) еще_снять , sum(распр_сняли) сняли  
 from  #rasp_del_tov r  
 inner join M2..Tov t on r.id_tov = t.id_tov and t.Number_r = 98619  
 where -- t.id_group_rasp=10141  
 t.id_group_rasp is not null  
 group by t.id_group_rasp  
 order by t.id_group_rasp  
  
   
 **/   
         
-- найти тек коэф ТТ по группе товара - запрос текущий  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777040, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
  
truncate table #rasp_gr_format  
  
insert into #rasp_gr_format  
   
select a.tt_format_rasp , a.id_group_rasp    , a.НормЗапас , COUNT(distinct a.id_tt)   
   
 , (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  -  sum(b.ПланПР) * a.НормЗапас СнятьЕще  
   
 , sum(a.Распр ) - ((sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  -  sum(b.ПланПР) * a.НормЗапас ) ИтогоРаспр  
 , a.НормЗапас  
   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
   
   
 group by a.tt_format_rasp, a.id_group_rasp, НормЗапас  
 having COUNT(distinct a.id_tt) >0  
  
   
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777041, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
  
  
  
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * a.НормЗапас Снять_еще   
  
--a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцЗапас_групп Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
--left join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
--order by a.id_group_rasp   
 --where a.id_group_rasp=4000003  
  
--Declare @N int = 97253  
/**  
insert into m2..rasp_gr_tt  
Select @N Number_r , * , GETDATE() date_add  , 1 type_add  
from #rasp_gr_tt  
**/  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777050, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
  
  
  
  
--------------------------------------------------------------------  
-- товары, которые в группах, то их нет на остатках  
  
truncate table #rasp_tt_tov  
  
insert into #rasp_tt_tov  
  
  
select r.id_tt , r.tt_format_rasp , r.id_group_rasp , r.id_tov , r.id_kontr , r.prohod , r.Распр , 0 Распр_нараст_tt, 0 Распр_нараст_tov,   
ROW_NUMBER () over (partition by r.id_tt ,  r.id_group_rasp order by  r.prohod desc) rn_tt ,  
ROW_NUMBER () over (partition by r.id_tov , r.id_kontr , r.tt_format_rasp order by  r.prohod desc) rn_tov   
from #rasp_gr_reestr r  
  
inner join   
(select r.tt_format_rasp , r.id_tov , r.id_kontr , SUM(r.Распр) Распр  
from #rasp_gr_reestr r  
group by r.tt_format_rasp , r.id_tov , r.id_kontr  
) rgr on r.id_tov = rgr.id_tov and r.id_kontr = rgr.id_kontr and r.tt_format_rasp = rgr.tt_format_rasp  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr  
where abs(rgr.Распр-rdl.Распр)<0.1  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777051, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
update #rasp_del_tov  
set Распр = rdl.Распр  - r.Распр , Распр_сняли = rdl.Распр_сняли + r.Распр  
from #rasp_del_tov rdl  
inner join   
(select tt_format_rasp,  r.id_tov , r.id_kontr , SUM(r.Распр) Распр  
from #rasp_tt_tov r  
group by tt_format_rasp,  r.id_tov , r.id_kontr) r  
on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777052, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
update #rasp_gr_reestr  
set znach = 0 , Распр =0 , Сняли = rgl.Распр , pri_alg = -1  
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov r  
on r.id_tov = rgl.id_tov and r.id_kontr = rgl.id_kontr and r.id_tt = rgl.id_tt and  r.prohod = rgl.prohod  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777053, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
-- пересчитать #rasp_gr_tt  
  
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР *a.НормЗапас Снять_еще  
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
--left join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
   
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777054, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
--------------------------------------------------------------------  
  
--select * from #rasp_gr_reestr rgr where rgr.type_add_kor>=2000  
  
  
-- часть ВВ ушли в минуса. Проверить, нет ли списанных товаров, у которых есть харакетирстики в магазинах, где плюс  
  
declare @id_tov_v int=1 , @id_tt_v int , @id_tt_v_old int , @znach_v real, @Распр_v real , @id_kontr_v_old int  
  
while @id_tov_v is not null  
begin  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770549, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
--select @id_tov_v  
if OBJECT_ID('tempdb..#r777054') is not null drop table #r777054  
select r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще , sum(rgr.Сняли) Сняли , r.ПланПР  
into #r777054  
from #rasp_gr_tt r  
inner join #rasp_gr_reestr rgr on r.id_tt = rgr.id_tt and r.id_group_rasp = rgr.id_group_rasp  
where r.Снять_еще<=-100 and rgr.Распр=0 and rgr.type_add_kor<2000  
group by r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще, r.ПланПР  
  
if OBJECT_ID('tempdb..#r777055') is not null drop table #r777055  
select r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще , sum(rgr.Распр) Распр  
into #r777055  
from #rasp_gr_tt r  
inner join #rasp_gr_reestr rgr on r.id_tt = rgr.id_tt and r.id_group_rasp = rgr.id_group_rasp  
where  rgr.Распр> 0  
group by r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще  
having r.Снять_еще>0.5*sum(rgr.Распр)  
  
  
select @id_tov_v = Null  
select @id_tov_v = v.id_tov , @id_tt_v = v.id_tt -- тов и тт, которые нужно подменить  
from   
(select top 1 a.id_tt , a.id_tov  ,  
ROW_NUMBER() over (order by a.Снять_еще / a.ПланПР) rn  
from  
#r777054  a  
inner join   
#r777055  b on a.id_tov = b.id_tov   
) v  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777055, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
--находим с кем подменить  
select @id_tt_v_old = v.id_tt , @znach_v = v.znach , @Распр_v = v.Распр , @id_kontr_v_old = v.id_kontr  
from  
(select top 1 b.id_tt , b.id_kontr , b.Распр , b.znach ,  
ROW_NUMBER() over (partition by a.id_tt , a.id_tov order by b.Снять_еще/ a.ПланПР desc) rn  
from  
(select r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще , sum(rgr.Сняли) Сняли , r.ПланПР   
from #rasp_gr_tt r  
inner join #rasp_gr_reestr rgr on r.id_tt = rgr.id_tt and r.id_group_rasp = rgr.id_group_rasp  
where r.Снять_еще<=-100 and rgr.Распр=0  
and r.id_tt = @id_tt_v and rgr.id_tov = @id_tov_v  
group by r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще, r.ПланПР  
)  a  
inner join   
(select r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще  , MAX(rgr.znach) znach, MAX(rgr.Распр) Распр  
from #rasp_gr_tt r  
inner join #rasp_gr_reestr rgr on r.id_tt = rgr.id_tt and r.id_group_rasp = rgr.id_group_rasp  
where  rgr.Распр> 0  
group by r.id_tt , rgr.id_tov , rgr.id_kontr , r.Снять_еще  
having r.Снять_еще>0.5*sum(rgr.Распр)  
)  b on a.id_tov = b.id_tov   
) v  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777056, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
--select * from #rasp_gr_reestr  
  
update #rasp_gr_reestr  
set id_kontr = r2.id_kontr  , Распр = 0  , Сняли = r1.Распр ,id_tt_old = @id_tt_v , type_add_kor = 2000  from #rasp_gr_reestr r1  
inner join   
(  
select r1.id_kontr , r1.znach , r1.Распр  
from #rasp_gr_reestr r1  
where r1.id_tov = @id_tov_v and r1.id_tt = @id_tt_v  
) r2 on 1=1  
where r1.id_tov = @id_tov_v and r1.id_tt = @id_tt_v_old  
  
update #rasp_gr_reestr  
set id_kontr = @id_kontr_v_old , Распр = @Распр_v , znach = @znach_v , Сняли = 0 ,id_tt_old = @id_tt_v_old , type_add_kor = 2000  
from #rasp_gr_reestr r1  
where r1.id_tov = @id_tov_v and r1.id_tt = @id_tt_v  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777057, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * a.НормЗапас Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
--left join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
end  
  
  
------------------------------------------------------------------------------------------------  
  
-- теперь получилось, что есть Магазины, которые уже нельзя собрать, те нет товаров, кторые можно добавить  
-- оставшиеся магазины теперь пересчитать норматив наполнения группы, чтоб между ними было равномерно   
--   
  
 -- выявляем магазины, в которые по нормативу ничего не добавить, тк все товары уже с доп коробкой - тип 3000  
 -- выявляем магазтины, в которых уже все сняли, те распр=0 , тип 2000  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777058, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
truncate table #rasp_type_rr_gr  
   
-- ничего не добавить -- только первый раз  
insert into #rasp_type_rr_gr  
select a.id_tt , a.id_group_rasp , 3000 , a.Распр   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , f.ПроцИсправ ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a   
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt   
   
    inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
      
      
 where a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ <0     
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777059, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
 -------------------------------------------------------------------------------------------------------------------------  
   
 -- все уже сняли - это повторяем после каждого цикла  
insert into #rasp_type_rr_gr  
select a.id_tt , a.id_group_rasp , 2000 , a.Распр   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a   
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt   
   
   inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
     
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
     
where a.Распр=0 and a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцЗапас_групп >100  
and r2.id_tt is null  
   
 ------------------------------------------------------------------------------------------------  
-- поставить, чтоб те, что не набираются, больше не участовали в перереаспределении  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770591, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
update #rasp_gr_reestr  
set type_add_kor = r2.type_tt_gr + r.type_add_kor  
--select *  
from #rasp_gr_reestr r  
 inner join #rasp_type_rr_gr r2 on r2.id_tt = r.id_tt and r2.id_group_rasp = r.id_group_rasp  
where  r.type_add_kor < r2.type_tt_gr  
    
   
  insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770592, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
-- пересчитать #rasp_gr_format     
  
  
truncate table #rasp_gr_format  
  
insert into #rasp_gr_format  
  
select a.tt_format_rasp , a.id_group_rasp    , a.НормЗапас , COUNT(distinct a.id_tt) ,  
    
  f.СнятьЕще ,  
 sum(a.Распр) ,  
 ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) / sum(b.ПланПР ) ПроцИспр  
 --,  ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) ,  
 -- sum(a.Распр) ,  sum(isnull(b.q_FO,0))  , sum (b.ПланПР)   
   
   
    
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
   
      
   inner join  
   ( select gr.tt_format_rasp tt_format_rasp , gr.id_group_rasp , SUM(gr.Распр) Снятьеще  
 from #rasp_del_tov gr  
 --inner join m2..tovari t on t.id_tov = gr.id_tov  
 group by gr.tt_format_rasp , gr.id_group_rasp )  f on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp   
      
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
  
   where r2.id_tt is null -- убрать магазины, которые уже не участуют в распределении  
  
 group by a.tt_format_rasp, a.id_group_rasp  , f.СнятьЕще, a.НормЗапас  
 having COUNT(distinct a.id_tt) >0  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770593, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
  
case when r2.id_tt is null then a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ  else 0 end Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
 inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
 left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
   
 -------------------------------------------------------------------------------------------------------------------------  
   
   
 --where  a.id_group_rasp = 10141  
   
     
--where a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцЗапас_групп > -1  
  
   
  
/**  
  
select *  
from #rasp_gr_tt rgt  
--inner join #rasp_gr_reestr r on r.id_tt = rgt.id_tt and r.id_group_rasp = rgt.id_group_rasp and rgt.Снять_еще<-1  
where rgt.Снять_еще>1 and rgt.Распр=0  
**/  
  
  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777060, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
------------------------------------------------------------------------------------------------  
  
  
Declare @pri int = 1  
-- Первый цикл - удаляем товары, которые нужно и с остатка убрать и в Запас не полезут.  
Declare @r_i int = 1 ,  @r_j int = 1  , @i_is_made int = 1 , @count int  -- счетчик выхода  
-- @r_i int  - цикл по удалению товаров,  @r_j -- цикл внутри pri, включает и удаление товаров и их перенос  
-- Показатель @i_is_made, что нужно пробовать переносить - @r_j это успешно сделано удаление. Если же не выполнено удаление, то и из Переноса выходим  
  
  
While @pri<=3  
begin  
  
  
Select @r_j = 1  
  
  
While @r_j=1  
begin  
  
  
Select @i_is_made=0 , @count =1 , @r_i =1  
  
While @r_i=1  
begin  
  
  
   
-- insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
-- select @id_job , 777060, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
-- select @getdate = getdate()   
/**  
select *  
from #rasp_tt_tov r  
where r.id_tt=11444 and r.id_group_rasp=10141  
order by rn_tt  
  
select *  
from #rasp_gr_reestr r  
where r.id_tt=11444 and r.id_group_rasp=10141  
  
select *  
from #rasp_gr_tt r  
where r.id_tt=11444 and r.id_group_rasp=10141   
  
select rdl.*  
from #rasp_del_tov rdl  
inner join M2..Tovari t on t.id_tov = rdl.id_tov  
where t.Group_raspr = 10141  
order by rdl.Распр desc  
  
select *  
from #rasp_tt_tov r  
where r.id_tov=489 and r.id_kontr= 14686   
order by rn_tov  
  
select *  
from #rasp_gr_reestr r  
where r.id_tov=489 and r.id_kontr= 14686   
  
select *  
from   
(select r.tt_format_rasp , r.id_tov , r.id_kontr , SUM(r.Распр) Распр  
from #rasp_gr_reestr r  
group by r.tt_format_rasp , r.id_tov , r.id_kontr  
) r  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr  
where abs(r.Распр-rdl.Распр)<0.1  
  
  
select rdl.*  
from #rasp_del_tov rdl  
inner join M2..Tovari t on t.id_tov = rdl.id_tov  
where t.Group_raspr = 10141  
order by rdl.Распр desc  
  
**/  
  
   
-- найти по 1 строке с каждого магазина товаров, которые нужно снять, при этом Запас не будет более 50% коробки ниже норматива  
-- снимать в первую очередь товары (наименьшие rn_tt), которых нужно больше снять  
--Declare @pri int =1   
truncate table #rasp_tt_tov  
  
insert into #rasp_tt_tov  
  
select r.id_tt , r.tt_format_rasp , r.id_group_rasp , r.id_tov , r.id_kontr , r.prohod , r.Распр , 0 Распр_нараст_tt, 0 Распр_нараст_tov,    
ROW_NUMBER () over (partition by r.id_tt ,  r.id_group_rasp order by  rdl.Распр desc) rn_tt ,  
ROW_NUMBER () over (partition by r.id_tov , r.id_kontr , r.tt_format_rasp order by  rgt.Снять_еще/rgt.ПланПР desc) rn_tov   
from #rasp_gr_reestr r  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr and rdl.Распр>0  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = r.id_group_rasp  
where r.pri <= @pri and r.Распр>0  -- @pri  
  
-- проставить нарастающую сумму распределения  
update #rasp_tt_tov  
set Распр_нараст_tt = rtt2.Распр  
from #rasp_tt_tov rtt  
inner join   
(  
select rtt.id_tt , rtt.id_group_rasp , rtt.rn_tt , SUM(rtt2.Распр) Распр  
from #rasp_tt_tov rtt  
inner join #rasp_tt_tov rtt2 on rtt.id_tt = rtt2.id_tt and rtt.id_group_rasp = rtt2.id_group_rasp and rtt2.rn_tt<=rtt.rn_tt  
group by rtt.id_tt , rtt.id_group_rasp, rtt.rn_tt  
) rtt2 on rtt.id_tt = rtt2.id_tt and rtt.id_group_rasp = rtt2.id_group_rasp and rtt2.rn_tt=rtt.rn_tt  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par2,par3)   
 select @id_job , 777070, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri ,@r_i , @r_j  
 select @getdate = getdate()   
  
-- удалить все товары , от которых ТТ уйдет в минус.  
--select *, rgt.Снять_еще - r.Распр_нараст_tt , - 0.5 * r.Распр  
delete from #rasp_tt_tov  
from #rasp_tt_tov r  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = r.id_group_rasp  
where  rgt.Снять_еще - r.Распр_нараст_tt < - 0.5 * r.Распр     
--order by r.id_tt , r.id_group_rasp , r.rn_tt  
  
/**  
select *, rgt.Снять_еще - r.Распр_нараст_tt , - 0.5 * r.Распр  
from #rasp_tt_tov r  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = r.id_group_rasp  
where  rgt.Снять_еще - r.Распр_нараст_tt < - 0.5 * r.Распр     
order by r.id_tt , r.id_group_rasp , r.rn_tt  
**/  
  
-- проставит нарастающую сумму по товару, чтоб его можно было убрать  
  
update #rasp_tt_tov  
set Распр_нараст_tov = rtt2.Распр  
from #rasp_tt_tov rtt  
inner join   
(  
select rtt.id_tov , rtt.id_kontr ,  rtt.tt_format_rasp , rtt.rn_tov , SUM(rtt2.Распр) Распр  
from #rasp_tt_tov rtt  
inner join #rasp_tt_tov rtt2 on rtt.id_tov = rtt2.id_tov and rtt.id_kontr = rtt2.id_kontr and rtt.tt_format_rasp = rtt2.tt_format_rasp and rtt2.rn_tov<=rtt.rn_tov  
group by rtt.id_tov , rtt.id_kontr , rtt.tt_format_rasp , rtt.rn_tov  
) rtt2 on rtt.id_tov = rtt2.id_tov and rtt.id_kontr = rtt2.id_kontr and rtt.tt_format_rasp = rtt2.tt_format_rasp and rtt2.rn_tov=rtt.rn_tov  
  
-- вот товары, что можно удалять из распределения. они и не снизят Запас и нужно удалить  
-- удалить товары, что уйдут в минус после уделения  
--select *, rdl.znach   
delete from #rasp_tt_tov  
from #rasp_tt_tov r  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr and rdl.Распр>0  
where  rdl.распр  - r.Распр_нараст_tov < -0.1   
--order by r.id_tov , r.tt_format_rasp , r.rn_tov  
  
/**  
select *  
from #rasp_tt_tov r  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and rdl.Распр>0  
where  rdl.распр  - r.Распр_нараст_tov < -0.1   
order by r.id_tov , r.tt_format_rasp , r.rn_tov  
**/  
  
  
  
-- теперь в #rasp_tt_tov как раз товары, что нужно удалить из распределения  
-- результаты записать в #rasp_del_tov (znach_сняли, Распр_сняли ) и в #rasp_gr_reestr (Распр int ,  Сняли)  
  
-- insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
-- select @id_job , 777070, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
-- select @getdate = getdate()   
   
--Declare @pri int =1   
update #rasp_del_tov  
set Распр = rdl.Распр  - r.Распр , Распр_сняли = rdl.Распр_сняли + r.Распр  
from #rasp_del_tov rdl  
inner join   
(select tt_format_rasp,  r.id_tov , r.id_kontr , SUM(r.Распр) Распр  
from #rasp_tt_tov r  
group by tt_format_rasp,  r.id_tov , r.id_kontr) r  
on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr  
  
update #rasp_gr_reestr  
set znach = 0 , Распр =0 , Сняли = rgl.Распр , pri_alg = @pri  
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov r  
on r.id_tov = rgl.id_tov and r.id_kontr = rgl.id_kontr and r.id_tt = rgl.id_tt and  r.prohod = rgl.prohod  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par2,par3)   
 select @id_job , 777071, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri ,@r_i , @r_j  
 select @getdate = getdate()   
   
  
  
  
  
-------------------------------------------------------------------------------------------------------------------------  
   
 -- все уже сняли - это повторяем после каждого цикла  
insert into #rasp_type_rr_gr  
select a.id_tt , a.id_group_rasp , 2000 , a.Распр   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a   
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt   
   
   inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp       
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
     
where a.Распр=0 and a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцЗапас_групп >100  
and r2.id_tt is null  
   
  insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par2,par3)   
 select @id_job , 777072, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri ,@r_i , @r_j  
 select @getdate = getdate()   
   
 ------------------------------------------------------------------------------------------------  
-- поставить, чтоб те, что не набираются, больше не участовали в перереаспределении  
  
update #rasp_gr_reestr  
set type_add_kor = r2.type_tt_gr + r.type_add_kor  
--select *  
from #rasp_gr_reestr r  
 inner join #rasp_type_rr_gr r2 on r2.id_tt = r.id_tt and r2.id_group_rasp = r.id_group_rasp  
where  r.type_add_kor < r2.type_tt_gr  
    
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par2,par3)   
 select @id_job , 777073, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri ,@r_i , @r_j  
 select @getdate = getdate()    
   
-- пересчитать #rasp_gr_format     
  
truncate table #rasp_gr_reestr_gr  
  
insert into #rasp_gr_reestr_gr  
select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
      sum( rasp.распр  ) Распр    
      from #rasp_gr_reestr rasp   
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
   
--select * from #rasp_gr_format  
  
truncate table #rasp_gr_format  
  
insert into #rasp_gr_format  
  
select a.tt_format_rasp , a.id_group_rasp    , a.НормЗапас , COUNT(distinct a.id_tt) ,  
    
  f.СнятьЕще ,  
 sum(a.Распр) ,  
 ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) / sum(b.ПланПР ) ПроцИспр  
 --,  ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) ,  
 -- sum(a.Распр) ,  sum(isnull(b.q_FO,0))  , sum (b.ПланПР)   
   
   
    
from   
    #rasp_gr_reestr_gr a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
   
      
   inner join  
   ( select gr.tt_format_rasp tt_format_rasp , id_group_rasp , SUM(gr.Распр) Снятьеще  
 from #rasp_del_tov gr  
 --inner join m2..tovari t on t.id_tov = gr.id_tov  
 group by gr.tt_format_rasp , gr.id_group_rasp )  f on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp   
      
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
  
   where r2.id_tt is null -- убрать магазины, которые уже не участуют в распределении  
  
 group by a.tt_format_rasp, a.id_group_rasp  , f.СнятьЕще, a.НормЗапас  
 having COUNT(distinct a.id_tt) >0  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par2,par3)   
 select @id_job , 777074, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri ,@r_i , @r_j  
 select @getdate = getdate()   
   
   
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
  
case when r2.id_tt is null then a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ  else 0 end Снять_еще   
from   
    #rasp_gr_reestr_gr a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
 inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
 left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
   
 -------------------------------------------------------------------------------------------------------------------------  
   
   
  
   
-- Цикл успешно завершен по удалению товаров, что не упали ниже Запаса  
-- повторяем, пока не будет 0  
if not exists (select *  
from  #rasp_tt_tov  
where @count<20)  
Select @r_i=0  
else  
Select @i_is_made=1  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1,par2,par3)   
 select @id_job , 777080, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri ,@r_i , @r_j  
 select @getdate = getdate()   
   
end  
  
-- второй шаг - пытаемся пернести так товар между тт, чтоб можно было удалить товар  
/**  
2. После 1 шага остаются Товары, которые нужно снять с Распределения, то нет ТТ с которых это можно сделать, чтоб не уйти ниже по Норм Коэф   
Есть список ТТ, у которых он есть в Распределении. Делаем из них Реестр 1.  
Для каждой из этих ТТ нужно найти другую ТТ с другим товаров, чтоб переместить на эту ТТ, чтоб можно было убрать Товар.   
Все товары,  с ТТ с избытками выстраиваем в Реестр2  
И начинаем "переносить" но одинаковым номерам строки из Реестра1 и Реестра2, только чтоб, не совпал Товар и не совпала ТТ.  
Если совпал ТТ или Товар, то просто сдвигаем на 1 строку вниз в Реестре 1 и пробуем "перенести еще раз".  
**/  
  
  
-- ТТ и товары, по которым больше норм запаса  
-- реестр 1  
--drop table #rasp_tt_tov_r1  
--create table #rasp_tt_tov_r1 (id_tt int , tt_format_rasp int, id_group_rasp int , id_tov int , prohod int,Распр int  ,  
--Распр_нараст_tt int ,  Распр_нараст_tov int ,    
--rn_tov int ) --,  rn_tov int)  
--create unique clustered index ind1 on #rasp_tt_tov (id_tt  , id_group_rasp , rn_tt desc)  
--create unique  clustered index ind2 on #rasp_tt_tov_r1 (id_tov  , tt_format_rasp , rn_tov desc)  
  
/**  
  
select *  
from #rasp_tt_tov_r1  
  
  
select *  
from #rasp_tt_tov r  
where  r.id_group_rasp=10141  
order by rn_tt  
  
select r.id_tov , r.id_kontr , SUM(Сняли)  
from #rasp_gr_reestr r  
where r.id_group_rasp=10141 and r.id_tt=11209 and Распр = 0  
group by r.id_tov , r.id_kontr  
order by r.id_tov , r.id_kontr  
  
  
  
select SUM(Сняли)  
from #rasp_gr_reestr r  
where r.id_tov = 488 and id_kontr = 14722  
  
select *  
from #rasp_gr_reestr r  
where r.id_group_rasp=10141 and r.id_tt=11209  
  
select rdl.*  
from #rasp_del_tov rdl  
inner join M2..Tovari t on t.id_tov = rdl.id_tov  
where t.Group_raspr = 10141  
and Распр>0.1  
--order by rdl.Распр desc  
order by rdl.id_tov , rdl.id_kontr  
  
  
select *  
from #rasp_gr_tt r  
where r.id_group_rasp=10141   
order by r.Снять_еще  
  
  
  
select *  
from #rasp_tt_tov r  
where r.id_tov=489 and r.id_kontr= 14686   
order by rn_tov  
  
select *  
from #rasp_gr_reestr r  
where r.id_tt=11209 and r.id_group_rasp=10141   
  
select *  
from #rasp r  
where r.id_tt=11209 and r.id_group_rasp=10141   
  
select *  
from   
(select r.tt_format_rasp , r.id_tov , r.id_kontr , SUM(r.Распр) Распр  
from #rasp_gr_reestr r  
group by r.tt_format_rasp , r.id_tov , r.id_kontr  
) r  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr  
where abs(r.Распр-rdl.Распр)<0.1  
  
  
select rdl.*  
from #rasp_del_tov rdl  
inner join M2..Tovari t on t.id_tov = rdl.id_tov  
where t.Group_raspr = 10141  
order by rdl.Распр desc  
  
**/  
  
  
-- считаем тт и тов, куда можно добавлять лишние коробки  
truncate table #rasp_tt_tov_dob_kor  
insert into #rasp_tt_tov_dob_kor  
select r1.id_tt , r1.id_tov , r.Макс_кол_кор - r1.Кол_кор_распр МожноДоб  
from  
(select r.id_tt , r.id_tov , MAX(prohod) Кол_кор_распр  
from #rasp_gr_reestr r  
where r.распр>0   
group by r.id_tt , r.id_tov  
) r1 inner join #rasp_tt_tov_max_kor r on r1.id_tt = r.id_tt and r1.id_tov = r.id_tov  
where r.Макс_кол_кор  - r1.Кол_кор_распр   >0  
  
  
--select * from #rasp_gr_tt  
-- select * from #rasp_gr_format  
  
   
   
  
--inner join #rasp_del_tov rd on rd.tt_format_rasp = rf.tt_format_rasp  
--inner join M2..Tovari t on t.id_tov = rd.id_tov and t.Group_raspr = rf.id_group_rasp  
--group by rf.tt_format_rasp , rf.id_group_rasp , rf.колво_тт  
  
  
--Declare @pri int =3  
truncate table #rasp_tt_tov_r1  
insert into #rasp_tt_tov_r1  
select r.id_tt , r.tt_format_rasp , r.id_group_rasp , r.id_tov , r.id_kontr , r.prohod , r.Распр , --0 Распр_нараст_tt,   
--0 Распр_нараст_tov,    
--ROW_NUMBER () over (partition by r.id_tt ,  r.id_group_rasp order by  rgt.Снять_еще desc) rn_tt ,  
ROW_NUMBER () over (partition by r.tt_format_rasp, r.id_group_rasp order by  колвоТТ desc) rn_tov   
--, rgt.Снять_еще  
, r.znach  
from #rasp_gr_reestr r  
inner join #rasp_gr_format rf on rf.tt_format_rasp = r.tt_format_rasp and rf.id_group_rasp = r.id_group_rasp  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr and rdl.Распр >100  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = r.id_group_rasp  and  rgt.Снять_еще >  rf.ИтогоРаспр / rf.колво_тт  
  
inner join   
( select r.id_tov , COUNT(r.id_tt) колвоТТ  
from  #rasp_tt_tov_dob_kor r   
group by r.id_tov  
) r3 on r.id_tov = r3.id_tov  
  
where r.pri < = @pri and r.Распр>0 and r.prohod<1000 --@pri  
--order by r.id_tov , r.tt_format_rasp ,  rn_tov  
  
  
--create table #rasp_tt_tov_r2 (id_tt int , tt_format_rasp int, id_group_rasp int , id_tov int , prohod int,Распр int  ,  
--Распр_нараст_tt int ,  Распр_нараст_tov int ,    
--rn_tov int ) --,  rn_tov int)  
--create unique clustered index ind1 on #rasp_tt_tov (id_tt  , id_group_rasp , rn_tt desc)  
--create unique  clustered index ind2 on #rasp_tt_tov_r2 (id_tov  , tt_format_rasp , rn_tov desc)  
  
--Declare @pri int =3  
truncate table #rasp_tt_tov_r2  
insert into #rasp_tt_tov_r2  
  
  
select r.id_tt , r.tt_format_rasp , r.id_group_rasp , r.id_tov , r.id_kontr , r.prohod , r.Распр , --0 Распр_нараст_tt,   
--0 Распр_нараст_tov,    
--ROW_NUMBER () over (partition by r.id_tt ,  r.id_group_rasp order by  rgt.Снять_еще desc) rn_tt ,  
ROW_NUMBER () over (partition by r.tt_format_rasp, r.id_group_rasp  order by  rgt.Снять_еще/rgt.ПланПР desc) rn_tov   
--, rgt.Снять_еще  
, r.znach  
from #rasp_gr_reestr r  
inner join #rasp_gr_format rf on rf.tt_format_rasp = r.tt_format_rasp and rf.id_group_rasp = r.id_group_rasp  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr and rdl.Распр < 100  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = r.id_group_rasp and  rgt.Снять_еще  <= rf.ИтогоРаспр / rf.колво_тт  
where r.pri <= @pri and r.Распр>0 and r.prohod<1000 --and r1.id_tov is null  
  
-- select * from #rasp_tt_tov_r2  
  
--order by r.id_tov , r.tt_format_rasp ,  rn_tov  
  
/**  
select *  
from #rasp_tt_tov_r1 r1  
where r1.id_group_rasp=4000003  
  
  
select *  
from #rasp_tt_tov_r2 r1  
where r1.id_group_rasp=4000003  
**/  
  
  
-- вот реальная замена tt у тов. товар переносится из r1 в r2.  
--create table #rasp_tt_tov_zam (id_tov int , id_tt_old int, id_tt_new int , prohod_old int, prohod_new int)  
  
 -- добавить, что в r2.id_tt и r1.id_tov не попало более коробок, чтоб можно добавить   
  
truncate table  #rasp_tt_tov_zam  
insert into #rasp_tt_tov_zam  
  
select a.id_tov , a.r2tov, a.id_kontr , a.r2k ,a.id_tt , a.r2tt, a.prohod , a.r2pro, a.znach , a.r2znach  
from   
(select r1.id_tov , r2.id_tov r2tov, r1.id_kontr , r2.id_kontr r2k ,r1.id_tt id_tt , r2.id_tt r2tt, r1.prohod , r2.prohod r2pro, r1.znach , r2.znach r2znach  
,ROW_NUMBER() over (partition by r2.id_tt , r1.id_tov order by r1.prohod ) kor_add_r2ttr1tov  
from #rasp_tt_tov_r1 r1  
inner join #rasp_tt_tov_r2 r2 on  r1.id_group_rasp = r2.id_group_rasp and  r1.tt_format_rasp = r2.tt_format_rasp and r1.rn_tov = r2.rn_tov  
-- учесть, что старый товар на новой тт имеет ту же id_kontr  
-- и что новый товар на старой тт тоже имеет ту же id_kontr  
left join   
(select distinct r.id_tt , r.id_tov , r.id_kontr  
from #rasp_gr_reestr r  
where r.znach>0 ) a_new on r1.id_tov = a_new.id_tov and r2.id_tt = a_new.id_tt  
left join  
(select distinct r.id_tt , r.id_tov , r.id_kontr  
from #rasp_gr_reestr r  
where r.znach>0 ) a_old on r2.id_tov = a_old.id_tov and r1.id_tt = a_old.id_tt  
where isnull(a_new.id_kontr,r1.id_kontr) = r1.id_kontr and isnull(a_old.id_kontr,r2.id_kontr) = r2.id_kontr  
) a  
inner join #rasp_tt_tov_dob_kor r on r.id_tt = a.r2tt and r.id_tov = a.id_tov and a.kor_add_r2ttr1tov<=r.МожноДоб  
  
  
  
-- + куда переносим - добавляем запись в #rasp_gr_reestr  
  
 -- добавляем запись строки #rasp_gr_reestr rgl с данными _old, только меняем   
 -- id_tt_new id_tov_new  ПроцЗапас  pri=1 prohod - счетчик новый от Макс_new+1  type_add_kor =1000  
  
--Declare @pri int =3  
insert into #rasp_gr_reestr  
   
select   
  rgl.tt_format_rasp , rgl.id_group_rasp ,  
  r.id_tt_new ,  
  r.id_tt_old ,-- откуде переброшен  
  r.id_tov_old ,  
  r.id_kontr_old,  
  r.znach_old  ,  
  rgl.Распр  ,   
  0  Сняли, -- сюда пишем, когда сняли      
  
  r2.ПроцЗапас ,  
  rgl.price_rasp ,  
 1 pri ,  
 isnull(r_m.max_prohod,1000)+  row_number() over (partition by r.id_tt_new,r.id_tov_old order by  r.prohod_old) prohod ,  
 1000 type_add_kor , --, *  
  @pri pri_alg   
   
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov_zam r  
on r.id_tov_old = rgl.id_tov and  r.id_kontr_old = rgl.id_kontr and r.id_tt_old = rgl.id_tt and  r.prohod_old = rgl.prohod  
inner join    
(  
select distinct rgl.id_group_rasp , rgl.id_tt , rgl.ПроцЗапас  
from #rasp_gr_reestr rgl   
) r2 on r2.id_tt = r.id_tt_new and r2.id_group_rasp = rgl.id_group_rasp  
   
left join   
(  
select r.id_tt , r.id_tov , MAX(r.prohod) max_prohod  
from #rasp_gr_reestr r  
where r.prohod>1000 and r.prohod<2000  
group by r.id_tt , r.id_tov  
) r_m on r_m.id_tt = r.id_tt_new and r_m.id_tov= r.id_tov_old  
  
  
   
   
--Declare @pri int =3  
insert into #rasp_gr_reestr  
   
select   
  rgl.tt_format_rasp , rgl.id_group_rasp ,  
  r.id_tt_old ,  
  r.id_tt_new ,-- откуде переброшен  
  r.id_tov_new ,  
  r.id_kontr_new,   
  r.znach_new  ,  
  rgl.Распр  ,   
  0  Сняли, -- сюда пишем, когда сняли      
  
  r2.ПроцЗапас ,  
  rgl.price_rasp ,  
 1 pri ,  
  isnull(r_m.max_prohod,1000)+ row_number() over (partition by r.id_tt_old,r.id_tov_new order by  r.prohod_new) prohod ,  
 1000 type_add_kor ,  
   @pri pri_alg   
   
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov_zam r  
on r.id_tov_new = rgl.id_tov and r.id_kontr_new = rgl.id_kontr and  r.id_tt_new = rgl.id_tt and  r.prohod_new = rgl.prohod  
inner join    
(  
select distinct rgl.id_group_rasp , rgl.id_tt , rgl.ПроцЗапас  
from #rasp_gr_reestr rgl   
) r2 on r2.id_tt = r.id_tt_old and r2.id_group_rasp = rgl.id_group_rasp  
  
  
left join   
(  
select r.id_tt , r.id_tov , MAX(r.prohod) max_prohod  
from #rasp_gr_reestr r  
where r.prohod>1000 and r.prohod<2000  
group by r.id_tt , r.id_tov  
) r_m on r_m.id_tt = r.id_tt_old and r_m.id_tov= r.id_tov_new  
  
  
   
  
-- делаем перенос между тт  
-- откуда переносим - пишем 0 и куда переносим тоже 0  
  
--Declare @pri int =3  
update #rasp_gr_reestr  
set Распр = 0 , Сняли = 0 ,  pri_alg = @pri   
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov_zam r  
on r.id_tov_old = rgl.id_tov and r.id_kontr_old = rgl.id_kontr and  r.id_tt_old = rgl.id_tt and  r.prohod_old = rgl.prohod  
  
update #rasp_gr_reestr  
set Распр = 0 , Сняли = 0 , pri_alg = @pri   
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov_zam r  
on r.id_tov_new = rgl.id_tov and r.id_kontr_new = rgl.id_kontr and r.id_tt_new = rgl.id_tt and  r.prohod_new = rgl.prohod  
  
  
  
-- пересчитать #rasp_gr_tt  
  
  
-------------------------------------------------------------------------------------------------------------------------  
   
 -- все уже сняли - это повторяем после каждого цикла  
insert into #rasp_type_rr_gr  
select a.id_tt , a.id_group_rasp , 2000 , a.Распр   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a   
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt   
   
   inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
     
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
     
where a.Распр=0 and a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцЗапас_групп >100  
and r2.id_tt is null  
   
 ------------------------------------------------------------------------------------------------  
-- поставить, чтоб те, что не набираются, больше не участовали в перереаспределении  
  
update #rasp_gr_reestr  
set type_add_kor = r2.type_tt_gr + r.type_add_kor  
--select *  
from #rasp_gr_reestr r  
 inner join #rasp_type_rr_gr r2 on r2.id_tt = r.id_tt and r2.id_group_rasp = r.id_group_rasp  
where  r.type_add_kor < r2.type_tt_gr  
    
   
   
-- пересчитать #rasp_gr_format     
  
  
--select * from #rasp_gr_format  
  
truncate table #rasp_gr_format  
  
insert into #rasp_gr_format  
  
select a.tt_format_rasp , a.id_group_rasp    , a.НормЗапас , COUNT(distinct a.id_tt) ,  
    
  f.СнятьЕще ,  
 sum(a.Распр) ,  
 ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) / sum(b.ПланПР ) ПроцИспр  
 --,  ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) ,  
 -- sum(a.Распр) ,  sum(isnull(b.q_FO,0))  , sum (b.ПланПР)   
   
   
    
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
   
      
   inner join  
   ( select gr.tt_format_rasp tt_format_rasp , id_group_rasp , SUM(gr.Распр) Снятьеще  
 from #rasp_del_tov gr  
 --inner join m2..tovari t on t.id_tov = gr.id_tov  
 group by gr.tt_format_rasp , gr.id_group_rasp )  f on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp   
      
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
  
   where r2.id_tt is null -- убрать магазины, которые уже не участуют в распределении  
  
 group by a.tt_format_rasp, a.id_group_rasp  , f.СнятьЕще, a.НормЗапас  
 having COUNT(distinct a.id_tt) >0  
  
  
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
  
case when r2.id_tt is null then a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ  else 0 end Снять_еще   
from   
    (  select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
  
      sum( rasp.распр  ) Распр    
  
      from #rasp_gr_reestr rasp   
  
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
 ) a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
 inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
 left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
   
 -------------------------------------------------------------------------------------------------------------------------  
   
  
   
/**  
select *  
from #rasp_gr_format f   
  
select id_group_rasp , SUM(Снять_еще)  
from #rasp_gr_tt r  
group by r.id_group_rasp  
  
select *  
from #rasp_del_tov  
**/  
  
  
if @i_is_made=0   
select @r_j=0  
  
  
  
end -- цикл @r_j  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
 select @id_job , 777090, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@pri  
 select @getdate = getdate()   
   
   
select @pri = @pri+1  
end -- цикл @pri  
  
  
  
  
  
/**  
insert into m2..rasp_del_tov  
Select @N Number_r , * , GETDATE() date_add  , 2  + @ff *10 type_add  
from #rasp_del_tov  
**/  
  
  
--Declare @N int = 97253  
insert into m2..rasp_gr_reestr  
Select @N Number_r , * , GETDATE() date_add  , 2  + @ff *10 type_add  
--into m2..rasp_gr_reestr  
from #rasp_gr_reestr  
  
insert into m2..rasp_gr_tt  
Select @N Number_r , * , GETDATE() date_add  , 2  + @ff *10 type_add  
from #rasp_gr_tt  
  
  
  
-- удалить  все товары, что остались в #rasp_del_tov, сортируя по Снять_еще по одному товару, начиная с самых больших  
declare @id_tov_r int, @id_kontr_r int, @znach_r int , @tt_format_rasp_r int  
  
DECLARE crs CURSOR LOCAL FOR  
  
select r.tt_format_rasp , r.id_tov , r.id_kontr   
from #rasp_del_tov r    
--inner join M2..Tovari t on t.id_tov = r.id_tov  
where r.Распр>1  
order by r.Распр desc  
  
OPEN crs  
   
FETCH crs INTO @tt_format_rasp_r, @id_tov_r , @id_kontr_r   
  
WHILE NOT @@fetch_status = -1   
  BEGIN  
  
  
--declare @tt_format_rasp_r int = 2, @id_tov_r int = 21158, @id_kontr_r int = 13006, @znach_r int = 8820  
  
truncate table #rasp_tt_tov  
  
insert into #rasp_tt_tov  
  
--select r.id_tt , r.tt_format_rasp , r.id_group_rasp , r.id_tov , r.id_kontr , r.prohod , r.Распр , 0 Распр_нараст_tt, 0 Распр_нараст_tov,    
--ROW_NUMBER () over (partition by r.id_tt ,  r.id_group_rasp order by  rgt.Снять_еще desc) rn_tt ,  
--ROW_NUMBER () over (partition by r.id_tov , r.tt_format_rasp order by  rgt.Снять_еще desc) rn_tov   
  
select r.id_tt , r.tt_format_rasp , r.id_group_rasp , r.id_tov , r.id_kontr , r.prohod , r.Распр , 0 Распр_нараст_tt, 0 Распр_нараст_tov,  
ROW_NUMBER () over (partition by r.id_tt ,  r.id_group_rasp order by c.Снять_еще/c.ПланПР desc) rn_tt ,  
ROW_NUMBER() over (partition by r.id_tov order by c.Снять_еще/c.ПланПР desc  ) rn_tov   
from #rasp_gr_reestr r   
inner join  
(select r.id_tt , r.id_group_rasp , r.Снять_еще, r.ПланПР  
from #rasp_gr_tt r    
) c on r.id_tt = c.id_tt and r.id_group_rasp = c.id_group_rasp  
where r.id_tov = @id_tov_r and r.id_kontr = @id_kontr_r and r.tt_format_rasp =@tt_format_rasp_r  
and r.Распр>0  
--order by r.id_tt  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770911, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
update #rasp_tt_tov  
set Распр_нараст_tov = rtt2.Распр  
from #rasp_tt_tov rtt  
inner join   
(  
select rtt.id_tov , rtt.id_kontr ,  rtt.tt_format_rasp , rtt.rn_tov , SUM(rtt2.Распр) Распр  
from #rasp_tt_tov rtt  
inner join #rasp_tt_tov rtt2 on rtt.id_tov = rtt2.id_tov and rtt.id_kontr = rtt2.id_kontr and rtt.tt_format_rasp = rtt2.tt_format_rasp and rtt2.rn_tov<=rtt.rn_tov  
group by rtt.id_tov , rtt.id_kontr , rtt.tt_format_rasp , rtt.rn_tov  
) rtt2 on rtt.id_tov = rtt2.id_tov and rtt.id_kontr = rtt2.id_kontr and rtt.tt_format_rasp = rtt2.tt_format_rasp and rtt2.rn_tov=rtt.rn_tov  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770912, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
delete #rasp_tt_tov  
from #rasp_tt_tov r  
inner join #rasp_del_tov rdl on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr and rdl.Распр>0  
where r.Распр_нараст_tov > rdl.Распр  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770913, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
update #rasp_del_tov  
set Распр = rdl.Распр  - r.Распр , Распр_сняли = rdl.Распр_сняли + r.Распр  
from #rasp_del_tov rdl  
inner join   
(select tt_format_rasp,  r.id_tov , r.id_kontr , SUM(r.Распр) Распр  
from #rasp_tt_tov r  
group by tt_format_rasp,  r.id_tov , r.id_kontr) r  
on r.tt_format_rasp = rdl.tt_format_rasp and  r.id_tov = rdl.id_tov and r.id_kontr = rdl.id_kontr  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770914, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
update #rasp_gr_reestr  
set znach = 0 , Распр =0 , Сняли = rgl.Распр , pri_alg = 10  
from #rasp_gr_reestr rgl  
inner join  #rasp_tt_tov r  
on r.id_tov = rgl.id_tov and r.id_kontr = rgl.id_kontr and r.id_tt = rgl.id_tt and  r.prohod = rgl.prohod  
  
-- пересчитать #rasp_gr_tt  
truncate table #rasp_gr_reestr_gr  
  
insert into #rasp_gr_reestr_gr  
select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
      sum( rasp.распр  ) Распр    
      from #rasp_gr_reestr rasp   
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
   
  
truncate table #rasp_gr_format  
  
insert into #rasp_gr_format  
  
select a.tt_format_rasp , a.id_group_rasp    , 0 , COUNT(distinct a.id_tt) ,  
    
  f.СнятьЕще ,  
 sum(a.Распр) ,  
 ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) / sum(b.ПланПР ) ПроцИспр  
 --,  ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) ,  
 -- sum(a.Распр) ,  sum(isnull(b.q_FO,0))  , sum (b.ПланПР)   
   
   
    
from   
    #rasp_gr_reestr_gr a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
   
      
   inner join  
   ( select gr.tt_format_rasp tt_format_rasp , id_group_rasp , SUM(gr.Распр) Снятьеще  
 from #rasp_del_tov gr  
 --inner join m2..tovari t on t.id_tov = gr.id_tov  
 group by gr.tt_format_rasp , gr.id_group_rasp )  f on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp   
      
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
  
   where r2.id_tt is null -- убрать магазины, которые уже не участуют в распределении  
  
 group by a.tt_format_rasp, a.id_group_rasp  , f.СнятьЕще  
 having COUNT(distinct a.id_tt) >0  
  
  
   
  
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
  
case when r2.id_tt is null then a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ  else 0 end Снять_еще   
from #rasp_gr_reestr_gr a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
 inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
 left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
   
 -------------------------------------------------------------------------------------------------------------------------  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770915, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
FETCH NEXT FROM crs INTO @tt_format_rasp_r, @id_tov_r , @id_kontr_r  
END   
CLOSE crs;    
DEALLOCATE crs  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777091, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
  
/**  
select r.* , r2.*  
from #rasp_gr_tt r   
 left join #rasp_type_rr_gr r2 on r2.id_tt = r.id_tt and r2.id_group_rasp = r.id_group_rasp  
where r.id_group_rasp=10195  
order by  r.id_group_rasp , Снять_еще  
--12069 10141  
  
select *  
from #rasp_gr_reestr gr  
where gr.id_tt = 12069 and gr.id_group_rasp=  10141  
**/  
  
  
-- поискать в тт, где избыток, если ли товары, которые нужны тт,  которых нехватка  
  
  
  
  
  
declare @rasp_i int =1  
  
while @rasp_i =1  
begin   
  
-- считаем тт и тов, куда можно добавлять лишние коробки  
truncate table #rasp_tt_tov_dob_kor  
insert into #rasp_tt_tov_dob_kor  
select r1.id_tt , r1.id_tov , r.Макс_кол_кор - r1.Кол_кор_распр МожноДоб  
from  
(select r.id_tt , r.id_tov , MAX(prohod) Кол_кор_распр  
from #rasp_gr_reestr r  
where r.распр>0   
group by r.id_tt , r.id_tov  
) r1 inner join #rasp_tt_tov_max_kor r on r1.id_tt = r.id_tt and r1.id_tov = r.id_tov  
where r.Макс_кол_кор  - r1.Кол_кор_распр   >0  
  
truncate table #rasp_g  
insert into #rasp_g  
select rg.id_group_rasp  , rg.tt_format_rasp ,  rg.id_tt , rg.id_tov , rg.id_kontr , rg.Распр Распр , a.Снять_еще , rg.znach znach  
,1 КолвоКор , rg.prohod  
from   
(select r.*  
from #rasp_gr_tt r   
where r.Снять_еще>100  
) a  
inner join #rasp_gr_reestr rg on a.id_tt = rg.id_tt and a.id_group_rasp =rg.id_group_rasp and rg.Распр>0  
where rg.type_add_kor<4000  
--and a.id_tt=11485  
--and rg.id_group_rasp=10141  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777092, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
/**  
select r.*  
from #rasp_gr_tt r   
where id_tt in (11485,12132) and id_group_rasp=10141  
**/  
  
   
truncate table #rasp_f  
insert into #rasp_f  
select rg.id_group_rasp  , rg.tt_format_rasp ,  rg.id_tt , rg.id_tov , rg.id_kontr , rg.Распр Распр , a.Снять_еще , rg.znach  
, 1 КолвоКор , rk.МожноДоб , rg.prohod  
from   
(select r.*  
from #rasp_gr_tt r   
where r.Снять_еще<-100  
) a  
inner join #rasp_gr_reestr rg on a.id_tt = rg.id_tt and a.id_group_rasp =rg.id_group_rasp   
inner join #rasp_tt_tov_dob_kor rk on rk.id_tt = rg.id_tt and rk.id_tov = rg.id_tov  
where rg.type_add_kor<4000  
--and a.id_tt=11485  
  
  
  
  
   
  
truncate table #rasp_zam_last  
insert into #rasp_zam_last  
select top 1 with ties   
g.id_group_rasp  , g.tt_format_rasp , g.id_tov , g.id_tt id_tt_old, f.id_tt id_tt_new,  g.id_kontr  , g.Распр  , g.znach , g.prohod  
--, f.Снять_еще, g.Снять_еще  
--  , g.id_group_rasp , g.id_kontr , f.id_kontr , g.Распр , f.Распр  , g.КолвоКор , f.КолвоКор , f.МожноДоб  
from #rasp_g g  
inner join #rasp_f f on g.id_tov = f.id_tov and g.tt_format_rasp = f.tt_format_rasp  
where g.id_kontr = f.id_kontr  
--and g.Снять_еще >= g.Распр and abs(f.Снять_еще) >= g.Распр --and f.Снять_еще + g.Распр  < g.Снять_еще  
--and abs (g.Снять_еще -  g.Распр ) + abs (  f.Снять_еще +  g.Распр ) < g.Снять_еще + abs(f.Снять_еще)  
and g.Снять_еще >= g.Распр * 0.5 and abs(f.Снять_еще) >= g.Распр * 0.5  
  
order by ROW_NUMBER() over (partition by g.id_group_rasp, g.tt_format_rasp order by   
master.dbo.maxz(abs(f.Снять_еще), g.Снять_еще ) desc,  
 (g.Снять_еще + abs(f.Снять_еще)) -  (abs (g.Снять_еще -  g.Распр ) + abs (  f.Снять_еще +  g.Распр ))  desc , f.Снять_еще , g.Снять_еще desc, g.prohod desc )   
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777093, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
if not exists (select * from #rasp_zam_last)  
select @rasp_i =0  
  
  
  
insert into #rasp_gr_reestr  
   
select   
  rgl.tt_format_rasp , rgl.id_group_rasp ,  
  r.id_tt_new ,  
  r.id_tt_old ,-- откуде переброшен  
  r.id_tov,  
  r.id_kontr,  
  rgl.znach  ,  
  rgl.Распр  ,   
  0  Сняли,       
  r2.ПроцЗапас ПроцЗапас ,  
  rgl.price_rasp ,  
 1 pri ,  
 isnull(r_m.max_prohod,4000)+ row_number() over (partition by r.id_tt_new,r.id_tov order by  rgl.prohod) prohod ,  
 4000 type_add_kor , --, *  
 6 pri_alg   
   
from #rasp_gr_reestr rgl  
inner join  #rasp_zam_last r  
on r.id_tov = rgl.id_tov and r.id_tt_old = rgl.id_tt and r.prohod_old = rgl.prohod   
inner join    
(  
select distinct rgl.id_group_rasp , rgl.id_tt , rgl.ПроцЗапас  
from #rasp_gr_reestr rgl   
) r2 on r2.id_tt = r.id_tt_new and r2.id_group_rasp = rgl.id_group_rasp  
  
left join   
(  
select r.id_tt , r.id_tov , MAX(r.prohod) max_prohod  
from #rasp_gr_reestr r  
where r.prohod>4000 and r.prohod<5000  
group by r.id_tt , r.id_tov  
) r_m on r_m.id_tt = r.id_tt_new and r_m.id_tov= r.id_tov  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777095, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
--Declare @pri int =3  
  
update  #rasp_gr_reestr  
set Распр = 0 ,id_tt_old = r.id_tt_new , Сняли = rgl.Распр , type_add_kor = 4000  
--select *  
from #rasp_gr_reestr rgl  
inner join  #rasp_zam_last r  
on r.id_tov = rgl.id_tov  and  r.id_tt_old = rgl.id_tt and r.prohod_old = rgl.prohod  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 7770951, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
-- пересчитать #rasp_gr_tt  
truncate table #rasp_gr_reestr_gr  
  
insert into #rasp_gr_reestr_gr  
select rasp.id_tt , rasp.id_group_rasp , rasp.tt_format_rasp , max(rasp.ПроцЗапас) НормЗапас,  
      sum( rasp.распр  ) Распр    
      from #rasp_gr_reestr rasp   
 group by rasp.id_tt , rasp.id_group_rasp, rasp.tt_format_rasp  
   
   
truncate table #rasp_gr_format  
  
insert into #rasp_gr_format  
  
select a.tt_format_rasp , a.id_group_rasp    , 0 , COUNT(distinct a.id_tt) ,  
    
  f.СнятьЕще ,  
 sum(a.Распр) ,  
 ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) / sum(b.ПланПР ) ПроцИспр  
 --,  ( (sum(a.Распр + isnull(b.q_FO,0)  - b.ПланПР) )  - f.СнятьЕще ) ,  
 -- sum(a.Распр) ,  sum(isnull(b.q_FO,0))  , sum (b.ПланПР)   
   
   
    
from   
    #rasp_gr_reestr_gr a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
      
   inner join  
   ( select gr.tt_format_rasp tt_format_rasp , id_group_rasp , SUM(gr.Распр) Снятьеще  
 from #rasp_del_tov gr  
 --inner join m2..tovari t on t.id_tov = gr.id_tov  
 group by gr.tt_format_rasp , gr.id_group_rasp )  f on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp   
      
   left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
  
   where r2.id_tt is null -- убрать магазины, которые уже не участуют в распределении  
  
 group by a.tt_format_rasp, a.id_group_rasp  , f.СнятьЕще  
 having COUNT(distinct a.id_tt) >0  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777096, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
  
truncate table #rasp_gr_tt  
  
insert into #rasp_gr_tt  
  
select a.id_tt , a.id_group_rasp , b.Запас , b.ПланПР , a.Распр , isnull(b.q_FO,0)   
, 1.0* (a.Распр + isnull(b.q_FO,0)  - b.ПланПР ) /  b.ПланПР ПроцЗапас  , a.НормЗапас ,  
  
case when r2.id_tt is null then a.Распр + isnull(b.q_FO,0)  - b.ПланПР -  b.ПланПР * f.ПроцИсправ  else 0 end Снять_еще   
from   
    #rasp_gr_reestr_gr a  
 inner join #rasp_gr_tt_ubr b on a.id_group_rasp = b.id_group_rasp and a.id_tt = b.id_tt  
  
 inner join #rasp_gr_format f  on f.tt_format_rasp=a.tt_format_rasp and f.id_group_rasp=a.id_group_rasp  
  
 left join #rasp_type_rr_gr r2 on r2.id_tt = a.id_tt and r2.id_group_rasp = a.id_group_rasp  
   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777098, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
 end  
   
   
 -------------------------------------------------------------------------------------------------------------------------  
   
   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777100, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
/**   
insert into m2..rasp_del_tov  
Select @N Number_r , * , GETDATE() date_add  , 3  + @ff *10 type_add  
from #rasp_del_tov  
**/  
  
--Declare @N int = 97253  
insert into m2..rasp_gr_reestr  
Select @N Number_r , * , GETDATE() date_add  , 3  + @ff *10 type_add  
--into m2..rasp_gr_reestr  
from #rasp_gr_reestr  
  
insert into m2..rasp_gr_tt  
Select @N Number_r , * , GETDATE() date_add  , 3  + @ff *10 type_add  
from #rasp_gr_tt  
  
  
  
  
/**  
select *  
from   
(select r.*  
from #rasp_gr_reestr r    
where  r.id_group_rasp = 10223  
--and r.id_tt=12011  
  
  
  
) a  
inner join  
(  
select r.*  
from #rasp_del_tov r    
inner join M2..Tovari t on t.id_tov = r.id_tov  
where  t.Group_raspr = 10223  
and r.Распр>0  
) b on a.id_tov = b.id_tov and a.Распр > 0  
  
inner join  
  
(select r.id_tt , r.Снять_еще  
from #rasp_gr_tt r    
where  r.id_group_rasp = 10223  
--order by r.Снять_еще desc  
) c on a.id_tt = c.id_tt  
order by a.id_tov ,c.Снять_еще desc  
  
  
**/  
  
  
  
  
-- теперь удаляем из #rasp , те что в #rasp_gr_reestr Распр=0  
  
delete from #rasp  
from #rasp_gr_reestr r1  
inner join #rasp r on r1.id_tt = r.id_tt and r1.id_tov=r.id_tov and r1.prohod = r.prohod  
where r1.Распр=0  
  
/**  
select *  
from #rasp_gr_reestr r1  
inner join #rasp r on r1.id_tt = r.id_tt and r1.id_tov=r.id_tov and r1.prohod = r.prohod  
where r1.Распр=0  
**/  
  
--select * from #rasp  
  
 -- и добавляем новые, если были  type_add_kor =1000  
-- declare @N int = 98619  
insert into #rasp ( number_r ,id_tov , id_tt , id_kontr , rn_r , znach , sort_r ,  prohod , sort_ost  , sort_pr  )   
select @N , r1.id_tov ,r1.id_tt , r1.id_kontr, 1 , r1.znach , 0 , r1.prohod, r1.pri , r1.pri_alg  
from #rasp_gr_reestr r1  
--inner join M2..rasp r on  r.number_r =@N and r1.id_tov = r.id_tov and r1.id_tt = r.id_tt -- найти строку в распр  
--inner join M2..tov_kontr tk on tk.Number_r = @N and r.id_tov = tk.id_tov and r.id_kontr = tk.id_kontr -- найти колво в короб  
where r1.prohod>1000 and r1.Распр>0   
  
  
-- и может быть смена id_kontr -  сравнить #rasp и m2..rasp tt_tov_kontr  
  
/**  
select *  
from #rasp_gr_reestr r1  
where r1.id_tt = 11955 and id_tov = 16061  
(11955, 16061, 1001).  
**/  
  
  
--declare @N int = 98619  
insert into [M2].[dbo].[rasp_smena_kontr]  
       ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_init]  
      ,[type_smena])  
 Select @N , r2.id_tt , r2.id_tov ,  r2.id_kontr , r.id_kontr ,777     
from #rasp r2  
inner join m2..rasp r on r.number_r = @N and r2.id_tt = r.id_tt and r2.id_tov = r.id_tov   
where r2.id_kontr <> r.id_kontr  
  
update  m2..rasp  
set id_kontr = r2.id_kontr  
from #rasp r2  
inner join m2..rasp r on r.number_r = @N and r2.id_tt = r.id_tt and r2.id_tov = r.id_tov   
where r2.id_kontr <> r.id_kontr  
  
  
update  m2..tt_tov_kontr  
set id_kontr = r2.id_kontr  
from #rasp r2  
inner join m2..tt_tov_kontr r on r.number_r = @N and r2.id_tt = r.id_tt and r2.id_tov = r.id_tov   
where r2.id_kontr <> r.id_kontr  
  
  
  
  
  
  
/**  
select *  
from #rasp_gr_reestr r1  
where r1.znach=0 and r1.prohod>1000  
  
tt_format_rasp id_group_rasp id_tt id_tt_old id_tov znach Распр Сняли ПроцЗапас price_rasp pri prohod type_add_kor pri_alg  
2 1010189 11571 11851 20631 0 0 408 0,2263995 51 1 1001 1000 3  
2 10185 11571 11935 23551 0 0 204 0,4603826 51 1 1001 1000 3  
2 10185 11571 11645 23551 0 0 204 0,4603826 51 1 1001 1000 3  
  
select *  
from #rasp_gr_reestr r1  
where r1.id_tt = 11571 and r1.id_tov = 20631  
select *  
from #rasp_gr_reestr r1  
where r1.id_tt = 11851 and r1.id_tov = 20631  
**/  
  
/**  
select *  
from #rasp r  
  
select *  
from #rasp_gr_reestr r1  
left join #rasp r on r1.id_tt = r.id_tt and r1.id_tov=r.id_tov   
where r1.prohod>1000  
**/  
  
  
/**  
select *  
from  #rasp r  
left join #rasp_gr_reestr r1 on r1.id_tt = r.id_tt and r1.id_tov=r.id_tov and r1.prohod = r.prohod  
inner join #rasp_gr_ubr rgu on r.tt_format_rasp  = rgu.tt_format_rasp and  r.id_group_rasp  = rgu.id_group_rasp  
where r.tt_format_rasp=2  
and r1.prohod is null  
**/  
  
  
  
-- пересортировать rasp.sort_r  
  
  
      update r  
      set sort_r = a.rn  
      --select *  
      from   
      (Select  id_tov , id_kontr , sort_r ,  
      ROW_NUMBER() over (partition by id_tov , id_kontr order by  sort_r ) rn  
      from #rasp rasp  
     
      ) a  
      inner join #rasp r on r.id_tov = a.id_tov and r.id_kontr = a.id_kontr and r.sort_r = a.sort_r  
      where a.sort_r <> a.rn  
            
        
        
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 777100, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
  
  
  
--    where rasp.id_tov = 17077 and rasp.id_tt = 12313  
  
      
    -- распределяем сколько нужно  
    /**  
 inner join -- но не более, чем осталось распределить  
  
 #ost_r ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
 and rasp.rn<=ost_r.ОсталосьРаспр  
    **/  
  
  -- посчитать сколько в итоге уже распределилось   
  
    /**  
    --declare @N int = 83096 , @date_rasp date = '2018-12-10'  
    Select  t.id_group , t.Name_tov , SUM(r.q_fact_pickup*pr.Price) распр , MIN(rz.Date_end), Max(rz.Date_end)  
    from m2..archive_rasp r    
    inner join M2..Raspr_zadanie rz   on rz.Number_r = r.number_r  
    inner join m2..Tovari t  on r.id_tov = t.id_tov and t.id_group in (10179,10223)  
    inner join Reports..Price_1C_tov pr on pr.id_tov = r.id_tov  
    where rz.number_r < @N and rz.Date_r = @date_rasp  
    group by t.id_group , t.Name_tov  
    **/  
    
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9704, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
      -- и,  удалить все лишние коробки от остатков  
   
       --сохранить в отдельной таблице  
      insert into m2..prev_Group_raspr        
      (  
      [number_r]  
      ,[id_tov]  
      ,[id_tt]  
      ,[id_kontr]  
      ,[rn_r]  
      ,[znach]  
      ,[sort_r]  
      ,[prohod]  
      ,[sort_ost]  
      ,[sort_pr]  
      ,[p1]  
      ,[znach_sum]  
      ,[znach_sum_narast]  
      ,[rn_gr]  
      ,type_add_kor  
      ,[type_prev]        
      )  
      select   
       [number_r]  
      ,rasp.[id_tov]  
      ,[id_tt]  
      ,rasp.[id_kontr]  
      ,[rn_r]  
      ,[znach]  
      ,[sort_r]  
      ,[prohod]  
      ,[sort_ost]  
      ,[sort_pr]  
      ,[p1]  
      ,[znach_sum]  
      ,[znach_sum_narast]  
      ,[rn_gr]  
      ,type_add_kor  
        
       , 2 type_prev --, isnull(ost_r.ОсталосьРаспр,0)      
      from #rasp rasp   
       
     left join -- но не более, чем осталось распределить  
  
 #ost_r ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
   
 where rasp.sort_r>isnull(ost_r.ОсталосьРаспр,0)  
 and rasp.type_add_kor>1  
        
 --  
            
     delete from rasp  
     from #rasp rasp   
       
     left join -- но не более, чем осталось распределить  
  
 #ost_r ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
   
 where rasp.sort_r>isnull(ost_r.ОсталосьРаспр,0)  
  
--/**  
  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9707, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
       
      -- пересортировать rn_gr - больше номера = убирать в первую очередь  
        
      update r  
      set rn_gr = a.rn  
      --select *  
      from   
      (Select rasp.* , isnull(tg.id_group,t.id_group)  id_group,  
      ROW_NUMBER() over (partition by id_tt , isnull(tg.id_group,t.id_group) order by   
      case when type_add_kor=1 then 1 else 0 end , rn_gr ) rn  
      from #rasp rasp  
      inner join m2..Tovari t  on rasp.id_tov = t.id_tov   
  
    left join #tovgr tg on tg.id_tov = rasp.id_tov -- значит товар есть в ограничении распределения по группе - добавить по 1 коробке  
  
     
      ) a  
      inner join #rasp r on r.id_tt = a.id_tt and r.rn_gr = a.rn_gr  
      inner join m2..Tovari t  on r.id_tov  = t.id_tov and t.id_group = a.id_group  
  
      where a.rn_gr <> a.rn  
  
 -----------------------------------------------     
 --не нужно считать, что уже распр, тк учтено в q_fo  
   /**  
    if OBJECT_ID('tempdb..#b_group') is not null drop table #b_group  
     
     --declare @N int = 97288 , @date_rasp date = '2019-03-14'   
    Select r.id_tt , t.id_group ,  SUM(r.q_fact_pickup*t.Price) распр  
    into #b_group  
    from m2..archive_rasp r    
    inner join M2..Raspr_zadanie rz   on rz.Number_r = r.number_r  
    inner join #tovgr t  on r.id_tov = t.id_tov   
    --inner join Reports..Price_1C_tov pr on pr.id_tov = r.id_tov  
    where rz.number_r < @N and rz.Date_r = @date_rasp  
    and r.koef_ost_pr_rasp is not null  
    group by r.id_tt , t.id_group  
  
 **/   
   
      
    truncate table  #raspr_group  
      
    insert into #raspr_group  
    select a.id_tt , a.id_group , a.сум_расп -- + ISNULL( b.распр ,0)  
    from   
    (  
    select  r.id_tt , r.id_group , SUM(сум_расп) сум_расп  
    from   
    (  
      
    Select r.id_tt , t.id_group , (r.q_raspr*t.Price) сум_расп  
    from m2..rasp r    
    inner join #tovgr t  on r.id_tov = t.id_tov   
    --inner join Reports..Price_1C_tov pr on pr.id_tov = r.id_tov  
    where r.number_r = @N  
    and r.koef_ost_pr_rasp is not null  
    ) r  
    group by r.id_tt , r.id_group  
    ) a  
   --left join #b_group  
   --  b  on a.id_tt = b.id_tt and a.id_group = b.id_group  
    
    
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9704, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
    if OBJECT_ID('tempdb..#b_9704') is not null drop table #b_9704  
  
 --select * from #b_9704   
     --drop table #b_9704   
     --declare @N int = 97288 , @date_rasp date = '2019-03-14'   
     --select r.id_tt , t.id_group  ,  sum((r.q_plan_pr*(1.0 + 0.01 * r.koef_ost_pr_rasp ) -r.q_FO)*t.Price) - ISNULL(r2.распр,0) ОсталосьРаспр  
     select r.id_tt , r.id_group  ,  r.Pl_gr - ISNULL(r2.распр,0) ОсталосьРаспр  
     into #b_9704     
     from  
     (select r.id_tt , t.id_group , sum((r.q_plan_pr*(1.0 + 0.01 * r.koef_ost_pr_rasp ) -r.q_FO)*t.Price) Pl_gr  
     from m2..rasp r         
     inner join #tovgr t  on r.id_tov = t.id_tov     
     where  r.number_r = @N   
     and r.koef_ost_pr_rasp is not null  
     group by r.id_tt , t.id_group) r  
       
     left join #raspr_group r2 on r2.id_tt = r.id_tt and r2.id_group = r.id_group  
       
     /**    
     from   
       
     #rasp r1  
     inner join m2..rasp r   on r.id_tt = r1.id_tt and r.id_tov = r1.id_tov and r.number_r = @N  
       
     inner join #tovgr t  on r.id_tov = t.id_tov   
     left join #raspr_group r2 on r1.id_tt = r2.id_tt and r2.id_group = t.id_group  
     where r1.koef_ost_pr_rasp is not null  
     --inner join Reports..Price_1C_tov pr on pr.id_tov = r1.id_tov  
     group by r1.id_tt , t.id_group, r2.распр  
     --having sum(((r.q_plan_pr*2.25 -r.q_FO)*pr.Price - ISNULL(r2.распр,0))) > 1000  
     
    **/  
      
   /**  
    declare @N int = 96543  
     select r1.id_tt , t.id_group  , -- sum(((r.q_plan_pr*(1.0 + 0.01 * t.koef_ost_pr ) -r.q_FO)*t.Price - ISNULL(r2.распр,0))) ОсталосьРаспр     
     r.q_plan_pr*t.Price ,  t.koef_ost_pr , r.q_FO*t.Price ,  ISNULL(r2.распр,0)  
     from #rasp r1  
     inner join m2..rasp r   on r.id_tt = r1.id_tt and r.id_tov = r1.id_tov and r.number_r = @N  
     inner join #tovgr t  on r.id_tov = t.id_tov   
     left join #raspr_group r2 on r1.id_tt = r2.id_tt and r2.id_group = t.id_group  
     --inner join Reports..Price_1C_tov pr on pr.id_tov = r1.id_tov  
     --group by r1.id_tt , t.id_group  
     inner join M2..Tovari tov on tov.id_tov = r1.id_tov  
     where t.id_group=10141 and r1.id_tt = 11870  
    **/   
     
  --select *  
  --from #b_9704  b  
  --inner join M2..[Group tovari] gr on gr.id_group = b.id_group  
  --where id_tt=12385  
    
       
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9705, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()      
    
    if OBJECT_ID('tempdb..#tt_gr_rn') is not null drop table #tt_gr_rn  
        
  -- drop table #tt_gr_rn        
     --declare @N int = 96543 , @date_rasp date = '2019-02-19'      
    Select a.id_tt , a.id_group , min(a.rn_gr) rn_gr  
    into #tt_gr_rn  
      
    --select *   
    from   
      
     (select r1.id_tt , r1.rn_gr , t.id_group , t.id_group_init , SUM(r2.znach_sum) СуммаРасп  
     from #rasp r1  
     inner join #tovgr t  on r1.id_tov = t.id_tov   
     inner join #rasp r2 on r1.id_tt = r2.id_tt and r2.rn_gr<=r1.rn_gr  
     inner join #tovgr t2  on r2.id_tov = t2.id_tov  and t.id_group = t2.id_group  
       
      where r1.koef_ost_pr_rasp is not null     
     --where r1.id_tt = 11870 and t.id_group  = 10229  
     group by r1.id_tt , r1.rn_gr , t.id_group, t.id_group_init  
     --order by r1.id_tt , t.id_group, r1.rn_gr   
     ) a  
     left join   
     -- осталось распределить - не более  
     --declare @N int = 83096 , @date_rasp date = '2018-12-10'     
      #b_9704 b on a.id_tt = b.id_tt and a.id_group = b.id_group   
      where a.СуммаРасп- isnull(b. ОсталосьРаспр ,0) >= 0 and isnull(b. ОсталосьРаспр ,0)>0  
      and ( a.id_group_init in (65, 10174, 10176) or @dont_use_wait_sklad=1  or @ff=1)  -- только ФРОВ или Ра  
      group by a.id_tt , a.id_group  
    
    
    insert into #tt_gr_rn  
     select a.id_tt , a.id_group , a.rn_gr  
    from   
     (  
     select r1.id_tt , r1.id_group , id_group_init,  MAX(СуммаРасп) СуммаРасп , MAX(rn_gr) rn_gr  
     from   
     (select r1.id_tt , r1.rn_gr , t.id_group , t.id_group_init ,SUM(r2.znach_sum) СуммаРасп  
     from #rasp r1  
     inner join #tovgr t  on r1.id_tov = t.id_tov   
     inner join #rasp r2 on r1.id_tt = r2.id_tt and r2.rn_gr<=r1.rn_gr  
     inner join #tovgr t2  on r2.id_tov = t2.id_tov  and t.id_group = t2.id_group  
       
     where r1.koef_ost_pr_rasp is not null         
     --where r1.id_tt = 11870 and t.id_group  = 10229  
     group by r1.id_tt , r1.rn_gr , t.id_group, t.id_group_init  
     --order by r1.id_tt , t.id_group, r1.rn_gr   
     ) r1  
     group by r1.id_tt , r1.id_group, id_group_init  
     ) a  
     inner join   
     -- осталось распределить - не более  
     --declare @N int = 83096 , @date_rasp date = '2018-12-10'     
      #b_9704 b on a.id_tt = b.id_tt and a.id_group = b.id_group   
      where a.СуммаРасп < isnull(b. ОсталосьРаспр ,0) and isnull(b. ОсталосьРаспр ,0)>0  
      and ( a.id_group_init in (65, 10174, 10176) or @dont_use_wait_sklad=1 or @ff=1)  -- только ФРОВ  
        
        
    
  /**  
  select *  
  from #tt_gr_rn b  
  inner join M2..[Group tovari] gr on gr.id_group = b.id_group  
  where id_tt=12385  
  **/  
    
    
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9706, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
       
      -- удалить все превышения суммы по группе  
        
      --сохранить в отдельной таблице  
      insert into m2..prev_Group_raspr        
      (  
      [number_r]  
      ,[id_tov]  
      ,[id_tt]  
      ,[id_kontr]  
      ,[rn_r]  
      ,[znach]  
      ,[sort_r]  
      ,[prohod]  
      ,[sort_ost]  
      ,[sort_pr]  
      ,[p1]  
      ,[znach_sum]  
      ,[znach_sum_narast]  
      ,[rn_gr]  
      ,type_add_kor  
       ,[type_prev]       
      )  
      select   
       [number_r]  
      ,r.[id_tov]  
      ,r.[id_tt]  
      ,[id_kontr]  
      ,[rn_r]  
      ,[znach]  
      ,[sort_r]  
      ,[prohod]  
      ,[sort_ost]  
      ,[sort_pr]  
      ,[p1]  
      ,[znach_sum]  
      ,[znach_sum_narast]  
      ,r.[rn_gr]  
      ,type_add_kor  
       , 1 type_prev --, tgr.*   
      from #rasp r   
      inner join #tovgr t  on r.id_tov = t.id_tov   
      left join #tt_gr_rn tgr on tgr.id_tt = r.id_tt and tgr.id_group = t.id_group  
        
      --inner join m2..tt on tt.id_TT = r.id_tt and tt.tt_format in (2,12)  
        
      --left join  #b_9704 b on r.id_tt = b.id_tt and t.id_group = b.id_group   
             
      where  ( tgr.rn_gr is null or r.rn_gr>tgr.rn_gr ) --and isnull(b. ОсталосьРаспр ,0)>0  
      and ( t.id_group_init in (65, 10174, 10176) or @dont_use_wait_sklad=1 or @ff=1)  -- только ФРОВ  
      --and r.tt_format_rasp in (2,3,12)          
      and r.koef_ost_pr_rasp is not null  
            
        
      delete from r  
      -- select *  
      from #rasp r   
      inner join #tovgr t  on r.id_tov = t.id_tov   
      left join #tt_gr_rn tgr on tgr.id_tt = r.id_tt and tgr.id_group = t.id_group  
        
      --inner join m2..tt on tt.id_TT = r.id_tt and tt.tt_format in (2,12)  
        
      --left join  #b_9704 b on r.id_tt = b.id_tt and t.id_group = b.id_group   
              
      where (tgr.rn_gr is null or r.rn_gr>tgr.rn_gr) --and isnull(b. ОсталосьРаспр ,0)>0  
      and ( t.id_group_init in (65, 10174, 10176) or @dont_use_wait_sklad=1 or @ff=1)  -- только ФРОВ  
      --and r.tt_format_rasp in (2,3,12)    
      and r.koef_ost_pr_rasp is not null       
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970601, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()         
        
   
--**/  
  
  
   -- добавить заказ покупателя  
/**  
  
--таблица с заказами покупаталей.  
SELECT TOP 1000 [Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[BonusCard] -- по каждому по отдельно  
      ,[q] - не общее количество в заказе, а не более штук в коробке  
      ,[ThisManufacturer] -- не менять характеристику  
      ,[q_raspr] - факт отгрузки  
      ,[id_kontr_new] - поменяли на нового  
      ,[type_add] - тип - важнейший показатель  
      ,[kolvo_korob] - штук в коробке  
      ,[N_korob] - порядковый номер коробки  
FROM [M2].[dbo].[rasp_zakaz_pok]  
  
1. сначала находим из предыдущих распределений, что уже успели распределить и записываем в [q_raspr]  
учитываем, что могло быть отгружено по [id_kontr_new], если он не пустой  
  
  
2 Далее по каждой строке  проходим   
[rasp_zakaz_pok] и [q_raspr]=0   
  
В 2 цикла  
Сначала по [ThisManufacturer]=1  
потом по остальным  
  
type_add  
Тип 1 - уже есть в распр избыток, включая распределение заказов. совпала id_kontr  
Тип 2 - уже есть в распределении избыток, по другой характеристике ( не обязат хар)  
+ В первую очередь поставить ту характеритистику, что на этот тт, а не в заказе  
При смене id_kontr пересчитать в коробки все строки, где нет отгрузки q.  
  
  
Тип 3 - со склада забрал коробку, ровно характ  
Тип 4 - со склада забрал коробку  ( не обязат хар)  
+ В первую очередь поставить ту характеритистику, что на этот тт, а не в заказе  
При смене id_kontr пересчитать в коробки все строки, где нет отгрузки q.   
   
Не хватило на складе - забираем с других тт (кроме тех, где по товару есть заказ)    
  
Тип 5 - с другого магазина забрал коробку, ровно характ (кроме магазинов, где есть по этому товару заказ)  
Тип 6 - со склада забрал коробку  ( не обязат хар) (кроме магазинов, где есть по этому товару заказ)  
 + В первую очередь поставить ту характеритистику, что на этот тт, а не в заказе  
При смене id_kontr пересчитать в коробки все строки, где нет отгрузки q.   
  
    
  
**/  
  
-- только на 2 круге  
if @ff =2  
begin  
  
  
if exists(  
Select *  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N and @sql_raspr=0 and r.q_raspr=0)  
begin  
  
-- почистить, если осталось с предыдещего этого  распределения  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
/**  
update M2..rasp_zakaz_pok  
set id_kontr_new = null , q_raspr = 0 , q_raspr_cur=0 , type_add=0  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N --and  r.q_raspr=0  
**/  
  
/**  
delete   
--select *  
from #rasp   
where sort_pr>1000  
**/  
  
-- если есть заказы покупателей  
  
--declare @N int= 3926, @date_rasp date = '2019-04-19'  
/**  
--drop table #zakaz_tt_tov_kontr_1  
-- выполненные реально распределения   
Select distinct r.number_r ,r.id_tt , r.id_tov  , r.id_kontr  
into #zakaz_tt_tov_kontr_1  
from M2..archive_rasp r  
inner join m2..Raspr_zadanie rz on rz.Number_r = r.number_r  
where r.number_r<@N and r.number_r> 0 and rz.Date_r = @date_rasp and rz.canceled is null and rz.test_raspr=0 and rz.ErrorMes is null and rz.sql_raspr is null  
and  r.q_raspr >0  
  
--declare @N int= 3926, @date_rasp date = '2019-04-19'  
-- проставить заказы, что уже отгружены  
--Select r.id_tt , r.id_tov , r.id_kontr ,r.q , r.q_raspr ,r.ThisManufacturer   
update M2..rasp_zakaz_pok  
set q_raspr = ztt.q_raspr , id_kontr_new  = ztt.id_kontr_new   
--declare @N int= 3926, @date_rasp date = '2019-04-19'  
--select *  
from M2..rasp_zakaz_pok  r  
inner join -- найти последнее распределение и сколько в нем было распределено  
(  
--declare @N int= 3926, @date_rasp date = '2019-04-19'  
Select top 1 with ties r.id_tt , r.id_tov , r.id_kontr, r.id_kontr_new  , r.q_raspr , r.N_korob  
from M2..rasp_zakaz_pok r  
inner join m2..Raspr_zadanie rz on rz.Number_r = r.number_r  
-- проверить, что реально была отгрузка, не отменено распределение  
inner join #zakaz_tt_tov_kontr_1 ztt on r.Number_r = ztt.Number_r and r.id_tt = ztt.id_tt and r.id_tov = ztt.id_tov and isnull(r.id_kontr_new,r.id_kontr) = ztt.id_kontr  
where r.number_r<@N and rz.Date_r = @date_rasp and rz.canceled is null and rz.test_raspr=0 and rz.ErrorMes is null  
and r.q_raspr >0  
order by ROW_NUMBER () over (PARTITION by r.id_tt , r.id_tov , r.id_kontr, r.N_korob order by r.Number_r desc)  
) ztt on ztt.id_tt = r.id_tt and ztt.id_tov = r.id_tov and ztt.id_kontr = r.id_kontr and ztt.N_korob = r.N_korob  
where r.number_r=@N  
  
  
/**  
declare @N int= 3522, @date_rasp date = '2019-04-17'  
Select *  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N --and  r.q_raspr=0  
and type_add>0  
order by ThisManufacturer desc  
**/  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970602, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
**/  
   
  
create table #zakaz_rasp_1 (id_tt int, id_tov int , id_kontr int , q_raspr_cur real)  
--drop table #zakaz_tt_zabrat  
create table #zakaz_tt_zabrat (id_z int, id_tt int, id_tov int, id_kontr int, id_tt_new int, prohod int, id_kontr_new int, q_raspr real)  
  
create table #ost_r_1 (id_tov int ,id_kontr int , q_raspr real)  
  
  
--declare @N int= 6797, @date_rasp date = '2019-04-17'  
Declare @id_z int =0 ,  @QZ real  
  
-- цикл по rasp_zakaz_pok, сначала ThisManufacturer=1  
  
/**  
 DECLARE crs CURSOR LOCAL FOR  
  
--declare @N int= 4052, @date_rasp date = '2019-04-20'  
Select r.id_z  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N and  r.q_raspr=0  
order by ThisManufacturer desc  
  
  OPEN crs  
  FETCH crs INTO @id_z  
    
  WHILE NOT @@fetch_status = -1   
**/  
  
-- новый цикл по всем заказам покупаталей, что нужно отгрузить   
Select @id_z = a.id_z  
from   
(Select top 1 r.id_z  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N and  r.q_raspr=0  
and r.id_z>@id_z  
order by  id_z   
)a  
   
while @@ROWCOUNT>0  
  BEGIN  
  
--Declare @id_z int = 190,  @N int= 3522, @date_rasp date = '2019-04-17'  
  
-- что уже включили в распределение из текущего заказа  
truncate table #zakaz_rasp_1  
insert into #zakaz_rasp_1  
Select r.id_tt , r.id_tov_pvz , isnull(r.id_kontr_new,r.id_kontr) id_kontr , sum(r.q_raspr_cur) q_raspr_cur  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N and  r.q_raspr_cur>0  
group by r.id_tt , r.id_tov_pvz , isnull(r.id_kontr_new,r.id_kontr)  
  
-- Тип 1 и Тип 2 - ищем, нет ли ли избытка уже в распределении  
  
-- Тип1  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
--Select *  
  
update M2..rasp_zakaz_pok  
set q_raspr=rzp.q , q_raspr_cur = rzp.q, type_add = 1  
  
from M2..rasp  ra with (index(ind1))  
left join   
(select r.id_tt , r.id_tov , r.id_kontr , sum(r.znach) znach  
from #rasp r   
group by r.id_tt , r.id_tov , r.id_kontr  
) r on   
r.id_tt = ra.id_tt and r.id_tov = ra.id_tov and r.id_kontr = ra.id_kontr  
  
inner join M2..rasp_zakaz_pok  rzp on  -- одна строка, что в цикле  
rzp.id_z=@id_z and rzp.q_raspr=0 and ra.id_tt = rzp.id_tt and ra.id_tov = rzp.id_tov_pvz and ra.id_kontr = rzp.id_kontr  
  
left join #zakaz_rasp_1 zr1 on zr1.id_tt = ra.id_tt and zr1.id_tov = ra.id_tov and zr1.id_kontr = ra.id_kontr  
  
where ra.number_r=@N   
and rzp.ThisManufacturer=1  
and (ra.q_raspr + ISNULL(r.znach,0))> ra.q_nuzno + isnull(zr1.q_raspr_cur,0) + rzp.q  -- значит есть избыток   
  
  
  
  
if @@ROWCOUNT=0  
begin  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970603, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
-- Тип2 -- только для @koef_ost_pr_rasp=0 -- это со сменой контрагента  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
  
update M2..rasp_zakaz_pok  
set q_raspr=rzp.q , q_raspr_cur = rzp.q , id_kontr_new = case when rzp.id_kontr <>ra.id_kontr_new then ra.id_kontr_new else Null end, type_add = 2  
from M2..rasp_zakaz_pok  rzp  
inner join  
(Select top 1 with ties rzp.id_z , rzp.id_tt, rzp.id_tov , rzp.id_kontr , ra.id_kontr id_kontr_new  
  
--update M2..rasp_zakaz_pok  
--set q_raspr=rzp.q , q_raspr_cur = rzp.q  
from M2..rasp  ra with (index(ind1))  
left join   
(select r.id_tt , r.id_tov , r.id_kontr , sum(r.znach) znach  
from #rasp r   
group by r.id_tt , r.id_tov , r.id_kontr  
) r on   
r.id_tt = ra.id_tt and r.id_tov = ra.id_tov and r.id_kontr = ra.id_kontr  
  
inner join M2..rasp_zakaz_pok  rzp on  -- одна строка, что в цикле  
rzp.id_z=@id_z and rzp.q_raspr=0 and ra.id_tt = rzp.id_tt and ra.id_tov = rzp.id_tov_pvz --and ra.id_kontr = rzp.id_kontr  
  
-- убрать полные аналоги  
inner join m2..tov_kontr tk on tk.Number_r=@N and tk.id_tov = ra.id_tov and tk.id_kontr = ra.id_kontr and tk.id_tov = tk.id_tov_init  
  
left join #zakaz_rasp_1 zr1 on zr1.id_tt = ra.id_tt and zr1.id_tov = ra.id_tov and zr1.id_kontr = ra.id_kontr  
  
where ra.number_r=@N   
and (ra.q_raspr + ISNULL(r.znach,0))> ra.q_nuzno + isnull(zr1.q_raspr_cur,0) + rzp.q   -- значит есть избыток   
and rzp.ThisManufacturer=0   
order by row_number() over (partition by rzp.id_z order by (ra.q_raspr + ISNULL(r.znach,0))- (ra.q_nuzno + isnull(zr1.q_raspr_cur,0) + rzp.q ) desc )  
) ra on ra.id_z = rzp.id_z --ra.id_tt = rzp.id_tt and ra.id_tov = rzp.id_tov and ra.id_kontr = rzp.id_kontr  
  
  
  
  
if @@ROWCOUNT=0  
begin  
  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970604, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
-------------------------------------------------------  
  
-- Тип 3 и Тип 4 - смотрим на складе  
  
  
  
 -- расчитать остатки на складе  
    truncate table #ost_r_1  
 insert into #ost_r_1    
    select id_tov ,id_kontr ,  SUM(q_raspr) q_raspr  
  from   
  (select rasp.id_tov , rasp.id_kontr , rasp.q_raspr  
  from m2..rasp with (  index(ind1))  
 where number_r=@N  
 union all  
 select r.id_tov , r.id_kontr , r.znach  
 from #rasp r    
  )  r  
 group by id_tov , id_kontr  
   
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
    truncate table #ost_r  
 insert into #ost_r      
 Select tov_kontr.id_tov , tov_kontr.id_kontr ,   
 floor((tov_kontr.q_ost_sklad - tov_kontr.q_wait_sklad - isnull(r_u.q_raspr,0)) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
  M2..tov_kontr with (  INDEX(PK_tov_kontr))   
 left join  #ost_r_1 r_u on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 --and tov_kontr.rasp_all=1  
 where number_r=@N   
 and floor((tov_kontr.q_ost_sklad  - tov_kontr.q_wait_sklad  - isnull(r_u.q_raspr,0)) / Kolvo_korob +0.001)>0  
  
  
--Тип 3 -- ровно контрагент  
   
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
-- добавить в #rasp товары, есть на складе по 1 коробке  
  
  
update M2..rasp_zakaz_pok  
set q_raspr = r.kolvo_korob , q_raspr_cur = r.kolvo_korob , type_add = 3  
from M2..rasp_zakaz_pok  r  -- одна строка, что в цикле  
inner join #ost_r ost_r on ost_r.id_tov=r.id_tov_pvz and ost_r.id_kontr=r.id_kontr  
where r.id_z=@id_z and  r.q_raspr=0  
and r.ThisManufacturer=1  
  
  
insert into #rasp ( number_r ,id_tov , id_tt , id_kontr , rn_r , znach , sort_r ,  prohod , sort_ost  , sort_pr  )   
select @N , r.id_tov_pvz ,r.id_tt , r.id_kontr, 1 , r.q_raspr , 0 , isnull(ra.prohod,0)+1 , 0 , 77887  
from M2..rasp_zakaz_pok  r  -- одна строка, что в цикле  
left join (  
select r.id_tt , r.id_tov , max(prohod)  prohod  
from #rasp r   
group by r.id_tt , r.id_tov  
) ra on r.id_tt = ra.id_tt and r.id_tov_pvz = ra.id_tov  
where r.id_z=@id_z and r.q_raspr>0  
  
  
  
  
if @@ROWCOUNT=0  
begin  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970605, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
--Тип 4 -- замена контрагента  
    truncate table #ost_r_1  
 insert into #ost_r_1    
    select id_tov ,id_kontr ,  SUM(q_raspr) q_raspr  
  from   
  (select rasp.id_tov , rasp.id_kontr , rasp.q_raspr  
  from m2..rasp with (  index(ind1))  
 where number_r=@N  
 union all  
 select r.id_tov , r.id_kontr , r.znach  
 from #rasp r    
  )  r  
 group by id_tov , id_kontr  
  
  
    truncate table #ost_r  
 insert into #ost_r      
 Select tov_kontr.id_tov , tov_kontr.id_kontr ,   
 floor((tov_kontr.q_ost_sklad - tov_kontr.q_wait_sklad - isnull(r_u.q_raspr,0)) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
  M2..tov_kontr with (  INDEX(PK_tov_kontr))   
 left join  #ost_r_1 r_u on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 --and tov_kontr.rasp_all=1  
 where number_r=@N   
 and floor((tov_kontr.q_ost_sklad  - tov_kontr.q_wait_sklad  - isnull(r_u.q_raspr,0)) / Kolvo_korob +0.001)>0  
  
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
-- добавить в #rasp товары, есть на складе по 1 коробке  
  
  
update M2..rasp_zakaz_pok  
set q_raspr=ra.kolvo_korob , q_raspr_cur = ra.kolvo_korob , id_kontr_new  = case when rzp.id_kontr <>ra.id_kontr_new then ra.id_kontr_new else Null end, type_add = 4  
from M2..rasp_zakaz_pok  rzp  
inner join  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
(select  top 1 with ties r.id_z , r.id_tt, r.id_tov , r.id_kontr , ost_r.id_kontr id_kontr_new , tk.Kolvo_korob  
from M2..rasp_zakaz_pok  r  -- одна строка, что в цикле  
inner join #ost_r ost_r on ost_r.id_tov=r.id_tov_pvz --and ost_r.id_kontr=r.id_kontr  
  
-- убрать полные аналоги  
inner join m2..tov_kontr tk on tk.Number_r=@N and tk.id_tov = ost_r.id_tov and tk.id_kontr = ost_r.id_kontr and r.id_tov = tk.id_tov_init  
  
left join #rasp r2 on r2.id_tt = r.id_tt and r2.id_tov = r.id_tov_pvz and r2.id_kontr = ost_r.id_kontr and r2.sort_pr<1000  
  
where r.id_z=@id_z and  r.q_raspr=0  
and r.ThisManufacturer=0  
order by row_number() over (partition by r.id_z order by  case when r2.id_tt is null then 1 else 0 end , ost_r.ОсталосьРаспр  desc )  
) ra on rzp.id_z = ra.id_z   
  
  
insert into #rasp ( number_r ,id_tov , id_tt , id_kontr , rn_r , znach , sort_r ,  prohod , sort_ost  , sort_pr  )   
select @N , r.id_tov_pvz ,r.id_tt , isnull(r.id_kontr_new,r.id_kontr), 1 , r.q_raspr , 0 , isnull(ra.prohod,0)+1 , 0 , 77888  
from M2..rasp_zakaz_pok  r  -- одна строка, что в цикле  
left join (  
select r.id_tt , r.id_tov , max(prohod)  prohod  
from #rasp r   
group by r.id_tt , r.id_tov  
) ra on r.id_tt = ra.id_tt and r.id_tov_pvz = ra.id_tov  
where r.id_z=@id_z  
and r.q_raspr>0  
  
  
  
  
if @@ROWCOUNT=0  
begin  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970606, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
----------------------------------------------------  
-- теперь придется забирать у других магазинов  
  
  
-- Тип 5 - совпалл контрагент  
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
-- добавить в #rasp товары, есть на складе по 1 коробке  
  
--update M2..rasp_zakaz_pok  
--set q_raspr = r.kolvo_korob , q_raspr_cur = r.kolvo_korob  
  
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
truncate table #zakaz_tt_zabrat  
insert into #zakaz_tt_zabrat  
select top 1 with ties r.id_z, r.id_tt ,  r.id_tov_pvz , r.id_kontr , ra.id_tt , ra.prohod , ra.id_kontr   , ra.znach  
from M2..rasp_zakaz_pok  r  -- одна строка, что в цикле  
  
inner join #rasp ra on ra.id_tov=r.id_tov_pvz and ra.id_kontr=r.id_kontr --and ra.prohod <>1 -- не трогать пепрвую коробку  
--and ra.tt_format_rasp=2  
  
-- не трогать с ТТ, где есть заказы   
left join  
(  
select distinct r.id_tt , r.id_tov_pvz  id_tov   
from M2..rasp_zakaz_pok  r   
where r.number_r=@N   
)  rz on ra.id_tt = rz.id_tt and ra.id_tov = rz.id_tov  
  
inner join m2..tov tg on tg.id_tov = r.id_tov_pvz and tg.Number_r = @N  
  
left join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = tg.id_group_rasp  
  
where   
rz.id_tt is null and  
r.id_z=@id_z   
and r.ThisManufacturer=1  
and  r.q_raspr=0  
order by row_number() over (partition by r.id_z order by case when ra.tt_format_rasp =10 then 1 else 0 end,  isnull(rgt.Снять_еще , 0 ) desc, ra.prohod  desc )  
  
-- проствить, что добавили  
update M2..rasp_zakaz_pok   
set q_raspr = rzp.kolvo_korob , q_raspr_cur = rzp.kolvo_korob , type_add = 5 , id_tt_old = r.id_tt_new  
from M2..rasp_zakaz_pok  rzp  
inner join #zakaz_tt_zabrat r on r.id_z = rzp.id_z  
  
-- добавить в  магазин с заказом  
insert into #rasp ( number_r ,id_tov , id_tt , id_kontr , rn_r , znach , sort_r ,  prohod , sort_ost  , sort_pr  )   
select @N , r.id_tov ,r.id_tt , r.id_kontr, 1 , r.q_raspr , 0 , isnull(ra.prohod,0)+1 , 0 , 77889  
from #zakaz_tt_zabrat  r  -- одна строка, что в цикле  
left join (  
select r.id_tt , r.id_tov , max(prohod)  prohod  
from #rasp r   
group by r.id_tt , r.id_tov  
) ra on r.id_tt = ra.id_tt and r.id_tov = ra.id_tov  
  
-- удалить с того магазина, откуда удалили  
delete #rasp  
from #rasp r  
inner join #zakaz_tt_zabrat rtz on r.id_tov = rtz.id_tov and r.id_tt = rtz.id_tt_new and r.prohod = rtz.prohod  
  
-- уменьшить снятьЕще в #rasp_gr_tt, тк участвует в выборе магазина  
update #rasp_gr_tt  
set Снять_еще = rgt.Снять_еще - r.q_raspr * tg.price  
from #zakaz_tt_zabrat r  
inner join #tovgr tg on tg.id_tov = r.id_tov  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt_new and rgt.id_group_rasp = tg.id_group  
  
  
  
  
if @@ROWCOUNT=0  
begin  
  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970607, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
/**  
select *  
from #rasp_gr_tt  
**/  
  
-- Тип 6 - любой контрагент  
  
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
-- добавить в #rasp товары, есть на складе по 1 коробке  
  
--update M2..rasp_zakaz_pok  
--set q_raspr = r.kolvo_korob , q_raspr_cur = r.kolvo_korob  
  
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
truncate table #zakaz_tt_zabrat  
insert into #zakaz_tt_zabrat  
select top 1 with ties r.id_z, r.id_tt ,  r.id_tov_pvz , r.id_kontr , ra.id_tt , ra.prohod , ra.id_kontr   , ra.znach  
from M2..rasp_zakaz_pok  r  -- одна строка, что в цикле  
  
inner join #rasp ra on ra.id_tov=r.id_tov_pvz --and ra.id_kontr=r.id_kontr --and ra.prohod <>1 -- не трогать пепрвую коробку  
--and ra.tt_format_rasp=2  
  
-- убрать полные аналоги  
inner join m2..tov_kontr tk on tk.Number_r=@N and tk.id_tov = ra.id_tov and tk.id_kontr = ra.id_kontr and tk.id_tov = r.id_tov  
  
-- не трогать с ТТ, где есть заказы   
left join  
(  
select distinct r.id_tt , r.id_tov_pvz id_tov   
from M2..rasp_zakaz_pok  r   
where r.number_r=@N   
)  rz on ra.id_tt = rz.id_tt and ra.id_tov = rz.id_tov  
  
inner join m2..tov tg on tg.id_tov = r.id_tov_pvz and tg.Number_r = @N  
  
left join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = tg.id_group_rasp  
  
left join #rasp r2 on r2.id_tt = r.id_tt and r2.id_tov = r.id_tov_pvz and r2.id_kontr = ra.id_kontr and r2.sort_pr<1000  
  
where   
rz.id_tt is null and  
r.id_z=@id_z  
and r.q_raspr=0  
and r.ThisManufacturer=0  
order by row_number() over (partition by r.id_z order by case when ra.tt_format_rasp =10 then 1 else 0 end , case when r2.id_tt is null then 1 else 0 end ,  
isnull(rgt.Снять_еще , 0 ) desc, ra.prohod  desc )  
  
-- проставить, что добавили  
update M2..rasp_zakaz_pok   
set q_raspr = r.q_raspr , q_raspr_cur = r.q_raspr , type_add = 6 , id_kontr_new  = case when rzp.id_kontr <>r.id_kontr_new then r.id_kontr_new else Null end   
, id_tt_old = r.id_tt_new  
from M2..rasp_zakaz_pok  rzp  
inner join #zakaz_tt_zabrat r on r.id_z = rzp.id_z  
  
-- добавить в  магазин с заказом  
insert into #rasp ( number_r ,id_tov , id_tt , id_kontr , rn_r , znach , sort_r ,  prohod , sort_ost  , sort_pr  )   
select @N , r.id_tov ,r.id_tt , isnull(r.id_kontr_new, r.id_kontr), 1 , r.q_raspr , 0 , isnull(ra.prohod,0)+1 , 0 , 77890  
from #zakaz_tt_zabrat  r  -- одна строка, что в цикле  
left join (  
select r.id_tt , r.id_tov , max(prohod)  prohod  
from #rasp r   
group by r.id_tt , r.id_tov  
) ra on r.id_tt = ra.id_tt and r.id_tov = ra.id_tov  
  
-- удалить с того магазина, откуда удалили  
delete #rasp  
from #rasp r  
inner join #zakaz_tt_zabrat rtz on r.id_tov = rtz.id_tov and r.id_tt = rtz.id_tt_new and r.prohod = rtz.prohod  
  
-- уменьшить снятьЕще в #rasp_gr_tt, тк участвует в выборе магазина  
update #rasp_gr_tt  
set Снять_еще = rgt.Снять_еще - r.q_raspr * tg.price  
from #zakaz_tt_zabrat r  
inner join #tovgr tg on tg.id_tov = r.id_tov  
inner join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt_new and rgt.id_group_rasp = tg.id_group  
  
end  
end  
end  
end  
end  
  
-- если сменился id_kontr - пересчитат все строки по неотгруженным заказам, если колво в коробке иное  
  
  
if exists(  
select rzp.Number_r ,rzp.id_tt , rzp.BonusCard , rzp.id_tov , rzp.id_kontr   
from M2..rasp_zakaz_pok  rzp  
inner join m2..tov_kontr tk on tk.Number_r=rzp.Number_r and tk.id_tov = rzp.id_tov and tk.id_kontr = rzp.id_kontr_new  
where rzp.id_kontr_new is not null and rzp.id_z = @id_z  
and rzp.kolvo_korob <> tk.Kolvo_korob )  
begin  
  
--declare @id_z int = 22024 , @QZ real  
  
select @QZ= master.dbo.maxz(0,sum(rzp.q) - SUM(rzp.q_raspr))  -- Заказ Покупателя, осталось собрать  
from M2..rasp_zakaz_pok  rzp  
inner join  
(select rzp.Number_r ,rzp.id_tt , rzp.BonusCard , rzp.id_tov , rzp.id_kontr   
from M2..rasp_zakaz_pok  rzp  
inner join m2..tov_kontr tk on tk.Number_r=rzp.Number_r and tk.id_tov = rzp.id_tov and tk.id_kontr = rzp.id_kontr_new  
where rzp.id_kontr_new is not null and rzp.id_z = @id_z  
) a on a.Number_r = rzp.Number_r and a.id_tt=rzp.id_tt and a.BonusCard= rzp.BonusCard and a.id_tov = rzp.id_tov and a.id_kontr= rzp.id_kontr  
  
if @QZ> 0  
begin  
  
update M2..rasp_zakaz_pok  
set q = rzp.q_raspr  
from M2..rasp_zakaz_pok  rzp  
inner join  
(select rzp.Number_r ,rzp.id_tt , rzp.BonusCard , rzp.id_tov , rzp.id_kontr   
from M2..rasp_zakaz_pok  rzp  
inner join m2..tov_kontr tk on tk.Number_r=rzp.Number_r and tk.id_tov = rzp.id_tov and tk.id_kontr = rzp.id_kontr_new  
where rzp.id_kontr_new is not null and rzp.id_z = @id_z  
) a on a.Number_r = rzp.Number_r and a.id_tt=rzp.id_tt and a.BonusCard= rzp.BonusCard and a.id_tov = rzp.id_tov and a.id_kontr= rzp.id_kontr  
where rzp.q_raspr>0  
  
  
delete M2..rasp_zakaz_pok  
from M2..rasp_zakaz_pok  rzp  
inner join  
(select rzp.Number_r ,rzp.id_tt , rzp.BonusCard , rzp.id_tov , rzp.id_kontr   
from M2..rasp_zakaz_pok  rzp  
inner join m2..tov_kontr tk on tk.Number_r=rzp.Number_r and tk.id_tov = rzp.id_tov and tk.id_kontr = rzp.id_kontr_new  
where rzp.id_kontr_new is not null and rzp.id_z = @id_z  
) a on a.Number_r = rzp.Number_r and a.id_tt=rzp.id_tt and a.BonusCard= rzp.BonusCard and a.id_tov = rzp.id_tov and a.id_kontr= rzp.id_kontr  
where rzp.q_raspr=0  
  
  
insert into m2..rasp_zakaz_pok  
      ([Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[BonusCard]  
      ,[q]  
      ,[ThisManufacturer]  
      ,[q_raspr]  
      ,[id_kontr_new]  
      ,[type_add]  
      ,[kolvo_korob]  
      ,[N_korob]  
      ,[id_tov_pvz])  
  
select td.Number_r , td.id_TT , td.id_tov , td.id_kontr , td.BonusCard ,    
case when tk.Kolvo_korob is not null then master.dbo.minz(td.q - (k.Korob-1)*tk.Kolvo_korob,tk.Kolvo_korob) else td.q end q,   
td.ThisManufacturer, 0 q_raspr , Null id_kontr_new, 0,   
case when tk.Kolvo_korob is not null then tk.Kolvo_korob else td.q end Kolvo_korob,   
td.N_korob  + k.Korob , ISNULL(tov.id_tov_pvz , tov.id_tov)  
from (select rzp.Number_r ,rzp.id_tt , rzp.BonusCard , rzp.id_tov , rzp.id_kontr , rzp.ThisManufacturer , @QZ q , rzp.N_korob  
from M2..rasp_zakaz_pok  rzp  
where rzp.id_z = @id_z) td  
left join M2..tov_kontr_init tk on tk.Number_r = td.Number_r and tk.id_tov = td.id_tov and tk.id_kontr=td.id_kontr   
inner join M2..tov_init tov on tov.Number_r =td.Number_r and tov.id_tov = td.id_tov   
inner join M2..Korob_add k on 1=1  
where --(k.Korob-1)*tk.Kolvo_korob < td.q +0.1 and (td.q - (k.Korob-1)*tk.Kolvo_korob)>tk.Kolvo_korob*0.1  
(  
((k.Korob-1)*tk.Kolvo_korob < td.q +0.1 and  ((td.q - (k.Korob-1)*tk.Kolvo_korob)>tk.Kolvo_korob*0.1 or k.Korob=1) and tk.Kolvo_korob is not null)   
or (k.Korob=1 and tk.Kolvo_korob is null )   
)  
  
end  
  
  
end  
  
  
  
  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 970608, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
/**  
  
FETCH NEXT FROM crs INTO @id_z  
END   
CLOSE crs;    
DEALLOCATE crs  
**/  
    
Select @id_z = a.id_z  
from   
(Select top 1 r.id_z  
from M2..rasp_zakaz_pok  r  
where r.number_r=@N and  r.q_raspr=0  
and r.id_z>@id_z  
order by  id_z  
)a  
  
end  
  
  
  
  
  
  
  
--select r.id_tt , r.id_tov , r.prohod  
--from #rasp r  
--group by r.id_tt , r.id_tov , r.prohod  
--having count(*)>1  
  
  
  
    
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 98, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
/**  
Number_r id_tt id_tov id_kontr BonusCard q ThisManufacturer q_raspr id_kontr_new  
3522 476 992 16646 7055999 2,2 0 2,649 183  
**/  
  
  
  
-- добавить в m2..rasp r  - если появились новые пары с контрагентами   
  
--declare @N int= 3522, @date_rasp date = '2019-04-17'  
  
-- сначала в tt_tov_kontr  
  
  
 insert into [M2].[dbo].[tt_tov_kontr]   
     ( [Number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_kontr]  
      ,[id_kontr_v]  
      ,[q_plan_pr]  
      ,[q_min_ost]  
      ,[q_FO]  
      ,[cena_pr]  
      ,[tt_format_rasp]  
      ,[price_rasp]  
      ,[koef_ost_pr_rasp]  
      ,id_tov_pvz_rasp   
       )  
        
select distinct @N , r.id_tt , r.id_tov , r.id_kontr ,r.id_kontr,  isnull(ttk.q_plan_pr,0) , isnull(ttk.q_min_ost,0), isnull(ttk.q_FO,0) , r.price_rasp  
, r.tt_format_rasp , r.price_rasp , r.koef_ost_pr_rasp , ISNULL(t.id_tov_pvz,t.id_tov)  
from #rasp r  
left join m2..rasp r2 on r2.number_r = @N and r2.id_tt = r.id_tt and r2.id_tov = r.id_tov and r2.id_kontr = r.id_kontr  
left join #ttk_rasp ttk on ttk.number_r = @N and ttk.id_tt = r.id_tt and ttk.id_tov = r.id_tov   
inner join M2..tov  t on t.Number_r = @N and r.id_tov = t.id_tov  
where r2.number_r is null  
  
 insert into M2..rasp   
  ( [number_r]  
   ,[id_tt]  
  ,[id_tov]  
  ,[id_kontr]  
  ,[q_FO]  
  ,[q_plan_pr]  
  ,[q_min_ost]  
  ,[q_max_ost]  
  ,[q_raspr]  
      ,[tt_format_rasp]  
      ,[price_rasp]  
      ,[koef_ost_pr_rasp]  
      ,id_tov_pvz_rasp   
  )  
--declare @N int= 6745, @date_rasp date = '2019-04-17'    
select distinct @N , r.id_tt , r.id_tov , r.id_kontr , isnull(ttk.q_FO,0) , isnull(ttk.q_plan_pr,0) , isnull(ttk.q_min_ost,0), isnull(ttk.max_ost_tt_tov,0) , 0   
 , r.tt_format_rasp , r.price_rasp , r.koef_ost_pr_rasp  , ISNULL(t.id_tov_pvz,t.id_tov)  
from #rasp r  
left join m2..rasp r2 on r2.number_r = @N and r2.id_tt = r.id_tt and r2.id_tov = r.id_tov and r2.id_kontr = r.id_kontr  
left join #ttk_rasp ttk on ttk.number_r = @N and ttk.id_tt = r.id_tt and ttk.id_tov = r.id_tov   
inner join M2..tov t on t.Number_r = @N and r.id_tov = t.id_tov  
where r2.number_r is null  
  
end   
end  
  
  
     insert into m2..rasp_temp  
     (number_r , id_tov , id_tt , id_kontr  , rn_r ,  
   znach  ,  sort_r ,  prohod , sort_ost  , sort_pr  , p1  , znach_sum , znach_sum_narast , rn_gr , type_add_kor  ,  
   tt_format_rasp , id_group_rasp , price_rasp , koef_ost_pr_rasp )  
  select *  
     from #rasp  
       
  
  
 insert into M2..raspr_hystory   
  ([number_r]  
  ,[id_tov]  
  ,[id_tt]  
  ,[id_kontr]  
  ,[rn_r]  
  ,[znach]  
  ,[sort_rz]  
  ,[prohod]  
  ,[sort_ost]  
  ,[sort_pr])  
 select   
 number_r,   
 id_tov ,   
 id_tt ,   
 id_kontr  ,   
 rn_r ,  
 znach  ,    
 sort_r ,    
 prohod ,   
 sort_ost  ,   
 sort_pr   
  from #rasp  
  
 update M2..rasp   
 set q_raspr = rasp1.znach + rasp.q_raspr , q_ko_ost = rasp.q_ko_ost +rasp1.znach , p1 = rasp1.p1  
 from M2..rasp with (rowlock, index(ind1))  
 inner join   
 (select id_tt , id_tov , id_kontr  ,SUM(znach) znach , max(p1) p1  
 from #rasp   
 group by id_tt , id_tov  , id_kontr ) rasp1 on rasp1.id_tt=rasp.id_tt and rasp1.id_tov=rasp.id_tov and rasp1.id_kontr = rasp.id_kontr  
 where rasp.number_r=@N  
  
 update M2..rasp   
 set  p2 = case when rl.q_nuzno<=rl.q_raspr then 1 else 0 end  
 from M2..rasp with (rowlock, index(ind1))  
 inner join #raspr_last rl on rasp.id_tt = rl.id_tt and rasp.id_tov = rl.id_tov    
 where rasp.number_r=@N  
 -- _______________________________________________________________________________  
 -- добавляем взаимозаменяемый товар, но не более Коэф2  
  
  
 -- исправить поля sort_ost и sort_pr, которые будут как раз использоваться в расчете  
  
 --declare @N as int =2738 , @potok as int =3  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 100, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 --Declare @N int = 34841    
 update M2..rasp   
 set id_tov_vz_r = tov.id_tov_vz  
 from M2..rasp with (rowlock, index(ind1))  
 inner join m2..tov with (  index (IX_tov_1))   
    on tov.id_tov=rasp.id_tov and tov.Number_r=@N and isnull(tov.id_tov_vz,0)<>0  
 where rasp.number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 110010, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 if OBJECT_ID('tempdb..#vz_2760') is not null drop table #vz_2760  
   
 select rasp.id_tt , rasp.id_tov_vz_r id_tov_vz , sum(rasp.q_ko_ost * tov.ves) sort_ost,   
 sum(rasp.q_plan_pr * tov.ves) sort_pr  
 into #vz_2760  
 from M2..rasp with ( index (ind1))  
 inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=rasp.id_tov and tov.Number_r=@N and isnull(tov.id_tov_vz,0)<>0  
 where rasp.number_r=@N  
 group by rasp.id_tt , rasp.id_tov_vz_r   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 110011, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..rasp   
 set sort_ost = vz.sort_ost/tov.ves , sort_pr=vz.sort_pr/tov.ves  
 --select *  
 from M2..rasp with (rowlock, index(ind1))  
 inner join m2..tov with (  index (IX_tov_1)) on tov.id_tov=rasp.id_tov and tov.Number_r=@N and isnull(tov.id_tov_vz,0)<>0  
 inner join #vz_2760 vz   
 on vz.id_tov_vz=rasp.id_tov_vz_r and vz.id_tt=rasp.id_tt  
 where rasp.number_r = @N and tov.ves>0  
  
-----------------------------------------------------------------------------------------------------  
  
-- новый кусок - если коробки остались и это не утреннее распределение, то распределить по Статус 2  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 96011, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()  
   
 --create table #ost_r  
 --(id_tov int , id_kontr int , ОсталосьРаспр real)  
 --declare @N int = 74683  
    truncate table #ost_r  
 insert into #ost_r  
 Select r_u.id_tov , r_u.id_kontr ,   
 floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
 (select id_tov ,id_kontr , SUM(q_nuzno) q_nuzno, SUM(q_raspr) q_raspr  
 from m2..rasp with (  index(ind1))  
 where number_r=@N  
 group by id_tov , id_kontr ) r_u  
 inner join M2..tov_kontr with (  INDEX(PK_tov_kontr)) on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 and number_r=@N and tov_kontr.rasp_all=1  
 where floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001)>0  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9601, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
Select @ff = @ff+1  -- если @ff, то будет еще один цикл  
  
end -- @ff  
--  
   
   
/**  
    if exists (   
    --declare @N int = 74683  
    select *   
    from #ost_r os  
    inner join m2..rasp   r on os.id_tov = r.id_tov and os.id_kontr = r.id_kontr and r.number_r=@N and r.zc_status=2  
 ) -- если есть товары в статусе 2  
  
  
 begin  
   
      
    ---------------------------------------------------------------------  
    -- просто красиво, вместо q_nuzno берем q_nuzno_init для товароов со статусом 2  
      
      
    delete from #rasp  
  
  
    --declare @N int = 74683  
 insert into #rasp  
   
 --declare @N int = 74683  
 Select @N , rasp.id_tov, rasp.id_tt , rasp.id_kontr ,5 , znach , rasp.rn , korob prohod, rasp.sort_ost , rasp.sort_pr , p1  
 from   
 (   
   
 --declare @N int = 74683  
 Select korob, rasp.q_nuzno_init ,  rasp.id_tov, rasp.id_tt , rasp.id_kontr ,   
  tk.Kolvo_korob znach, rasp.sort_ost , rasp.q_ko_ost + korob * tk.Kolvo_korob sort_pr,  
 --ROW_NUMBER() over ( partition by rasp.id_tov , rasp.id_kontr order by rasp.q_ko_ost - rasp.q_min_ost + korob * tk.Kolvo_korob) rn  
 ROW_NUMBER() over ( partition by rasp.id_tov , rasp.id_kontr order by   
   
  master.dbo.maxz(0,floor( korob * tk.Kolvo_korob- rasp.q_nuzno_init)) , -- колво коробок в превышение - может случится для складир товаров, но которых мало  
   
 -- новый приоритет для последней коробки - если последняя коробка, то в первую очередь тех, кто в прошлом распределении не получил последнюю коробку.  
 case when rasp.q_nuzno_init < korob * tk.Kolvo_korob then -- значит превысыли потребность  
 case when rl.q_nuzno<=rl.q_raspr then 1 else 0 end -- если в прошлом распределении была лишняя коробка, то в последнюю очередь  
 else 0 end  
   
 , rasp.q_ko_ost + korob * tk.Kolvo_korob) rn ,   
   
 case when rasp.q_nuzno_init < korob * tk.Kolvo_korob then -- значит превысыли потребность  
 case when rl.q_nuzno<=rl.q_raspr or isnull(rasp.zc_status,4)=5 then 1 else 0 end -- если в прошлом распределении была лишняя коробка, то в последнюю очередь  
 else 0 end  p1  
   
 from M2..tov_kontr tk  with ( INDEX(IX_tov_kontr_1))  
 inner join M2..Korob_add on 1=1  
 inner join M2..rasp with (INDEX (PK_rasp))    
 on tk.id_kontr=rasp.id_kontr and tk.id_tov=rasp.id_tov and rasp.number_r = @N and rasp.zc_status=2   
  
      
    left join #raspr_last rl on rl.id_tov = rasp.id_tov and rl.id_tt = rasp.id_tt  
      
 inner join #koef1 koef1 on rasp.id_tov=koef1.id_tov and rasp.id_tt=koef1.id_tt  
 and ((rasp.q_ko_ost + (korob-1) * tk.Kolvo_korob <= koef1.Koef1 * Kolvo_korob) or tk.rasp_all_init=0 )  
 where tk.Number_r=@N  and tk.rasp_all=1 and   
 -- не более макс остатка  
 rasp.q_FO + rasp.q_raspr + korob * tk.Kolvo_korob   
 <= case when q_max_ost>0 then q_max_ost else rasp.q_FO + rasp.q_raspr + korob * tk.Kolvo_korob end   
   
 --and tk.id_tov = 17148  
   
 ) rasp  
  
 inner join -- но не более, чем осталось распределить  
  
 #ost_r ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
 and rasp.rn<=ost_r.ОсталосьРаспр  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9801, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 insert into M2..raspr_hystory   
  ([number_r]  
  ,[id_tov]  
  ,[id_tt]  
  ,[id_kontr]  
  ,[rn_r]  
  ,[znach]  
  ,[sort_rz]  
  ,[prohod]  
  ,[sort_ost]  
  ,[sort_pr])  
 select   
 number_r,   
 id_tov ,   
 id_tt ,   
 id_kontr  ,   
 rn_r ,  
 znach  ,    
 sort_r ,    
 prohod ,   
 sort_ost  ,   
 sort_pr   
  from #rasp  
  
 update M2..rasp   
 set q_raspr = rasp1.znach + rasp.q_raspr , q_ko_ost = rasp.q_ko_ost +rasp1.znach , p1 = rasp1.p1  
 from M2..rasp with (rowlock, index(ind1))  
 inner join   
 (select id_tt , id_tov , SUM(znach) znach , max(p1) p1  
 from #rasp   
 group by id_tt , id_tov ) rasp1 on rasp1.id_tt=rasp.id_tt and rasp1.id_tov=rasp.id_tov  
 where rasp.number_r=@N  
  
  
 -- _______________________________________________________________________________  
  
  
      
    end  
  
      
      
-----------------------------------------------------------------------------------------------------  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1101, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  **/  
    
/**  
  
 Select tov.id_tov_vz gr_tov, ost_r.id_tov, ost_r.id_kontr,  
 ROW_NUMBER() over (partition by tov.id_tov_vz order by sum(q_plan_pr) desc ) rn  
 into #gr_tov  
 from   
 (Select r_u.id_tov , r_u.id_kontr ,   
 floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001) ОсталосьРаспр,  
 q_plan_pr  
 from  
 (select rasp.id_tov ,rasp.id_kontr , SUM(q_nuzno) q_nuzno, SUM(q_raspr) q_raspr ,   
 SUM (ttk.q_plan_pr*ttk.cena_pr) q_plan_pr  
 from m2..rasp with (index (ind1) )  
 inner join M2..tt_tov_kontr ttk with ( index (ind1)) on ttk.id_tov=rasp.id_tov and ttk.id_tt=rasp.id_tt and ttk.Number_r=@N   
 where rasp.number_r=@N  
 group by rasp.id_tov , rasp.id_kontr ) r_u  
 inner join M2..tov_kontr with (  INDEX(IX_tov_kontr_1)) on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 and number_r=@N) ost_r  
 inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=ost_r.id_tov and tov.Number_r=@N and isnull(tov.id_tov_vz,0)<>0  
 where ОсталосьРаспр >0.01 and not (tov.raspr_double=1 and tov.raspr_d_1_2=1)  
 group by tov.id_tov_vz , ost_r.id_tov, ost_r.id_kontr  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1102, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 Declare @rn as int   
  
 DECLARE crs CURSOR LOCAL FOR  
  
 select distinct rn  
 from #gr_tov  
 order by rn  
  
  OPEN crs  
  FETCH crs INTO @rn  
    
  WHILE NOT @@fetch_status = -1   
  BEGIN  
  
 delete from #rasp  
  
 insert into #rasp  
 Select @N , rasp.id_tov, rasp.id_tt , rasp.id_kontr, 2 , znach , rasp.rn , @rn prohod, rasp.sort_ost , rasp.sort_pr , 0 p1  
 from   
 (   
  
 Select tov.id_tov_vz , rasp.id_tov, rasp.id_tt , rasp.id_kontr ,   
  tk.Kolvo_korob znach,  
  rasp.sort_ost + korob * tk.Kolvo_korob * tov.ves ost , rasp.sort_ost , rasp.sort_pr ,   
 ROW_NUMBER() over ( partition by tov.id_tov order by rasp.sort_ost - rasp.q_min_ost + korob * tk.Kolvo_korob * tov.ves) rn   
 from M2..rasp with (index (ind1) )  
 inner join M2..Korob_add on 1=1  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr)) on tk.id_kontr=rasp.id_kontr and tk.id_tov=rasp.id_tov and tk.Number_r=@N   
 and tk.rasp_all=1  
 inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=rasp.id_tov and tov.Number_r=@N and isnull(tov.id_tov_vz,0)<>0  
  
 inner join #gr_tov gr_tov on gr_tov.rn=@rn and gr_tov.id_tov=rasp.id_tov -- только по сортировке товар  
 and gr_tov.id_kontr=rasp.id_kontr  
  
 -- найти коэф2  
 inner join M2..tt_tov_kontr ttk with ( index (ind1)) on rasp.id_kontr=ttk.id_kontr and rasp.id_tov=ttk.id_tov and rasp.id_tt=ttk.id_tt and ttk.Number_r=@N   
 and rasp.sort_ost + korob * tk.Kolvo_korob * tov.ves < ttk.k2 * Kolvo_korob * tov.ves  
 where rasp.number_r = @N and  
 rasp.q_FO + rasp.q_raspr + korob * tk.Kolvo_korob   
 <= case when q_max_ost>0 then q_max_ost else rasp.q_FO + rasp.q_raspr + korob * tk.Kolvo_korob end   
  
 ) rasp  
  
 inner join -- но не более, чем осталось распределить  
  
 (Select r_u.id_tov , r_u.id_kontr ,   
 floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
 (select id_tov ,id_kontr , SUM(q_nuzno) q_nuzno, SUM(q_raspr) q_raspr  
 from m2..rasp with (index (ind1) )  
 where number_r=@N  
 group by id_tov , id_kontr ) r_u  
 inner join M2..tov_kontr with (  INDEX(IX_tov_kontr_1)) on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 and number_r=@N) ost_r on ost_r.id_tov=rasp.id_tov and ost_r.id_kontr=rasp.id_kontr  
 and rasp.rn<=ost_r.ОсталосьРаспр  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1103, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 update M2..rasp   
 set q_raspr = rasp1.znach + rasp.q_raspr , q_ko_ost = rasp.q_ko_ost +rasp1.znach  
 from M2..rasp with (rowlock, index(ind1))  
 inner join   
 (select id_tt , id_tov , SUM(znach) znach   
 from #rasp   
 group by id_tt , id_tov ) rasp1 on rasp1.id_tt=rasp.id_tt and rasp1.id_tov=rasp.id_tov  
 where rasp.number_r=@N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1104, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
      
    --Declare @N int =  39458  
 if OBJECT_ID('tempdb..#vz2882') is not null drop table #vz2882  
 select rasp.id_tt , rasp.id_tov_vz_r id_tov_vz, sum(rasp.q_ko_ost * tov.ves) sort_ost,   
 sum(rasp.q_plan_pr * tov.ves) sort_pr  
 into #vz2882  
 from M2..rasp with ( index (ind1))  
 inner join M2..tov with (  index (IX_tov_1)) on   
 tov.id_tov=rasp.id_tov and tov.Number_r=@N and isnull(tov.id_tov_vz,0)<>0  
 where rasp.number_r=@N  
 group by rasp.id_tt , rasp.id_tov_vz_r  
  
    create clustered index ind1 on #vz2882 (id_tt,id_tov_vz)  
    --select * from #vz2882  
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 110401, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
      
 --Declare @N int =  39458                               
 update M2..rasp with (rowlock)  
 set sort_ost = vz.sort_ost , sort_pr=vz.sort_pr  
 --select rasp.sort_ost , vz.sort_ost , rasp.sort_pr, vz.sort_pr  
 from M2..rasp with (index(tt_tov_vz))  
 inner join #vz2882 vz   
 on vz.id_tov_vz=rasp.id_tov_vz_r and vz.id_tt=rasp.id_tt  
 where rasp.number_r = @N  
 and not (rasp.sort_ost = vz.sort_ost and rasp.sort_pr=vz.sort_pr)  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 1105, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 insert into M2..raspr_hystory   
  ([number_r]  
  ,[id_tov]  
  ,[id_tt]  
  ,[id_kontr]  
  ,[rn_r]  
  ,[znach]  
  ,[sort_rz]  
  ,[prohod]  
  ,[sort_ost]  
  ,[sort_pr])  
 select   
 number_r,   
 id_tov ,   
 id_tt ,   
 id_kontr  ,   
 rn_r ,  
 znach  ,    
 sort_r ,    
 prohod ,   
 sort_ost  ,   
 sort_pr   
 from #rasp  
  
  
  FETCH NEXT FROM crs INTO @rn  
  
  END  
  
  CLOSE crs  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 110, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
**/  
  
  
 -- _______________________________________________________________________________  
 -- добавляем оставшийся товар  
  
  
 -- исправить поля sort_ost и sort_pr, которые будут как раз использоваться в расчете  
  
 --declare @N as int =2738 , @potok as int =3  
  
 -- найти максимальное колво излишних коробок на ТТ  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9602, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 create table #id_tt_Макс_кор (id_tt int , Макс_кор int)  
 declare @date_max_kor as date = convert(date,getdate())  
  
 if exists( select * from m2..TT_max_kor where Date_add = @date_max_kor)  
 begin  
  
 insert into #id_tt_Макс_кор  
 select id_tt , Макс_кор   
 from m2..TT_max_kor  
 where DATE_add = @date_max_kor  
  
 end  
 else  
 begin  
  
 --declare @strТекстSQLЗапроса as nvarchar(4000)  
 SET @strТекстSQLЗапроса = '  
 insert into #id_tt_Макс_кор  
 exec ( ''  
 SELECT id_tt,   
 case when SUM(summa) / COUNT(DISTINCT CONVERT(date, date_tt)) <200000 then 0   
 else convert(int,1.0 * SUM(summa) / COUNT(DISTINCT CONVERT(date, date_tt)) / 1000 / 10) end  
 AS Макс_кор  
 FROM Reports..DT AS dtt     
 WHERE date_tt between DATEADD(week,-2,convert(date,getdate())) and   
 DATEADD(day,-3,convert(date,getdate()))  
 GROUP BY id_tt  
 '') at [SRV-SQL06]  
 '  
  
 --print @strТекстSQLЗапроса     
 exec sp_executeSQL @strТекстSQLЗапроса    
  
  update tm  
     set Макс_кор = 100  
     from #id_tt_Макс_кор tm  
     inner join M2..tt_format tt  
  on tt.id_TT = tm.id_tt   
            and tt.tt_format in (select tt_format from Reports..tt_format_project where project ='X5')  
  
     update tm  
     set Макс_кор = 0  
     from #id_tt_Макс_кор tm  
     inner join M2..tt tt  
  on tt.id_TT = tm.id_tt   
            and (tt.tt_format in ( 4,14) or tt.ТорговаяПлощадь<100)  
         
              
 insert into m2..TT_max_kor   
 select tm.id_tt ,  tm.Макс_кор  , @date_max_kor   
 from #id_tt_Макс_кор tm  
 left join m2..TT_max_kor tk on tm.id_tt=tk.id_tt and tk.date_add=@date_max_kor  
 where tk.id_tt is null  
  
 end  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 9603, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()  
   
  
---------------------------------------------------------------------------------------------------  
  
/**  
-- сначала избытки распределить на магазины, у которых по группе товара сняли по Остатки, тк нет товара  
  
   create table #rasp_gr_2 (id_tt int , id_group int , СуммаНехвОст int)   
   insert into #rasp_gr_2   
   select rh.id_tt , tg.id_group , SUM(grr.znach*tg.Price) СуммаНехвОст  
   from m2..prev_Group_raspr   grr with     
   inner join #tovgr tg on tg.id_tov = grr.id_tov  
   where  grr.number_r = @N and grr.type_prev=2  
   group by rh.id_tt , tg.id_group  
   having SUM(grr.znach*tg.Price)>0  
     
     
 delete from #ost_r  
 -- осталось распределить по каждому товару  
 insert into #ost_r  
 Select r_u.id_tov , r_u.id_kontr ,   
 floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
 (select id_tov ,id_kontr , SUM(q_nuzno) q_nuzno, SUM(q_raspr) q_raspr  
 from m2..rasp with (index (ind1) )  
 where number_r=@N   
  
 group by id_tov , id_kontr ) r_u  
 inner join M2..tov_kontr with (  INDEX(IX_tov_kontr_1)) on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 and number_r=@N  
   
    inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=r_u.id_tov and tov.Number_r=@N and not (tov.raspr_double=1 and tov.raspr_d_1_2=1)  
  
  
     
  delete from #rasp  
 insert into #rasp  
 Select @N , a.id_tov,a.id_tt , a.id_kontr , 4 , a.Kolvo_korob , a.rn , a.N_kor, a.Fact , a.ОсталосьРаспр , 0 p1 ,0 , 0 ,0  
 from  
 (select r.id_tov , r.id_tt , o.id_kontr , o.ОсталосьРаспр , r.Fact ,k.N_kor, tk.Kolvo_korob ,  
 ROW_NUMBER () over ( partition by o.id_tov , o.id_kontr   
 order by   
 k.N_kor ,  
 case when isnull(r2.zc_status,4)=4 then 1 when r2.zc_status=5 then 2 else 3 end  , -- чтоб избытки сначала распределять по 4 статусу, потом 5, а потом уже 2  
  r.Fact - (r2.q_FO + r2.q_raspr) desc , isnull(rh.q_kor_add,0) ,tm.Макс_кор desc ) rn  
  
 from #ost_r o  
   
 inner join #rasp_1 r on o.id_tov = r.id_tov  
   
    left join #tovLLED tovLLED on tovLLED.id_tov = o.id_tov  
    
 inner join (  
    select top 100 ROW_NUMBER() over (order by Number_r) N_kor   
    from m2..Raspr_zadanie   ) k on k.N_kor < = case when   
        tovLLED.id_tov is not null   
        or @date_rasp in ({d'2018-12-28'},{d'2018-12-29'},{d'2018-12-30'},{d'2018-12-31'})   
        then 100 else 1 end -- для всех товаров, кроме ЛЛ иЕД - 1 лишняя коробка  
  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr)) on tk.id_kontr=o.id_kontr and tk.id_tov=o.id_tov and tk.Number_r=@N   
 and tk.rasp_all=1  
  
 inner join m2..rasp r2 with (index (PK_rasp) )   
  on r.id_tov=r2.id_tov and r.id_tt=r2.id_tt and r2.Number_r=@N and r2.id_kontr = tk.id_kontr  
      
    inner join #tovgr tg on tg.id_tov = r2.id_tov   
    inner join #rasp_gr_2 rg2 on rg2.id_tt = r2.id_tt and rg2.id_group = tg.id_group  
      
  
 --left join #id_tt_Макс_кор tm on tm.id_tt=r.id_tt   
  
 left join #rh rh on rh.id_tt=r.id_tt  
  
  
  
 where  ОсталосьРаспр >0  
 and tk.Kolvo_korob * k.N_kor * tg.price <= rg2.СуммаНехвОст  
 -- не превысить максимальное, но кроме Статус2  
 and ((r2.q_FO + r2.q_raspr + tk.Kolvo_korob * k.N_kor < = r2.q_max_ost) or r2.q_max_ost<=0 or r2.zc_status=2)   
     
 ) a  
  
 where a.rn <= a.ОсталосьРаспр  
   
     
**/  
  
  
---------------------------------------------------------------------------------------------------  
  
   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 121, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 delete from #ost_r  
 -- осталось распределить по каждому товару  
 insert into #ost_r  
 Select r_u.id_tov , r_u.id_kontr ,   
 floor((tov_kontr.q_ost_sklad_calc - r_u.q_raspr) / Kolvo_korob +0.001) ОсталосьРаспр  
 from  
 (select id_tov ,id_kontr , SUM(q_nuzno) q_nuzno, SUM(q_raspr) q_raspr  
 from m2..rasp with (index (ind1) )  
 where number_r=@N   
  
 group by id_tov , id_kontr ) r_u  
 inner join M2..tov_kontr with (  INDEX(IX_tov_kontr_1)) on tov_kontr.id_tov=r_u.id_tov and tov_kontr.id_kontr=r_u.id_kontr   
 and number_r=@N  
   
    inner join M2..tov with (  index (IX_tov_1)) on tov.id_tov=r_u.id_tov and tov.Number_r=@N and not (tov.raspr_double=1 and tov.raspr_d_1_2=1)  
  
  
  
  
 --declare @N int =16103 , @date_rasp date = {d'2015-05-20'}  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 12201, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
      
    /**  
    --declare @Nvasql as nvarchar(4000),@result_table as varchar(100)='[srv-sql01].izbenkafin.dbo._InfoRg4020 '  
    if OBJECT_ID ('tempdb..#IRg4020') is not null drop table #IRg4020  
    create table #IRg4020 (_Fld5086 int)  
    set @nvaSQL ='insert into #IRg4020 (_Fld5086)  
      select distinct _Fld5086   
      from '+ @result_table +' with '  
       
 exec sp_executesql @nvaSQL  
  
   
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 12202, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
 **/  
        
 --declare @N int =78610 , @date_rasp date = {d'2018-11-11'}  
 select rh.id_tt , COUNT(*) q_kor_add  
 into #rh  
 from M2..raspr_hystory rh with (  index (ind2_h))  
 inner join M2..Raspr_zadanie rz   on rz.Number_r=rh.number_r  
 inner join M2..archive_rasp r   on r.number_r=rh.number_r and r.id_tt = rh.id_tt and r.id_tov = rh.id_tov and r.q_fact_pickup>0  
 --inner join #IRg4020  N_1c on rh.number_r=N_1c._Fld5086  
 left join #tovLLED tovLLED on tovLLED.id_tov = rh.id_tov  
 where rz.Date_r = @date_rasp and rh.znach>0 and rh.rn_r=4 and tovLLED.id_tov is null  
 group by rh.id_tt  
  
   -- if OBJECT_ID ('tempdb..#IRg4020') is not null drop table #IRg4020  
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 123, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
    create index ind2 on #rasp_1 (id_tov)  
  
 truncate table #rasp  
 insert into #rasp  
 Select @N , a.id_tov,a.id_tt , a.id_kontr , 4 , a.Kolvo_korob , a.rn , a.N_kor, a.Fact , a.ОсталосьРаспр , 0 p1 ,0 , 0 ,0, 0 ,  
  0  , 0  ,0,  0  
 from  
 (select r.id_tov , r.id_tt , o.id_kontr , o.ОсталосьРаспр , r.Fact ,k.N_kor, tk.Kolvo_korob ,  
 ROW_NUMBER () over ( partition by o.id_tov , o.id_kontr   
 order by   
 k.N_kor ,  
 case when isnull(r2.zc_status,4)=4 then 1 when r2.zc_status=5 then 2 else 3 end  , -- чтоб избытки сначала распределять по 4 статусу, потом 5, а потом уже 2  
  r.Fact - (r2.q_FO + r2.q_raspr) desc , isnull(rh.q_kor_add,0) ,tm.Макс_кор desc ) rn  
  
 from #ost_r o  
   
 inner join #rasp_1 r on o.id_tov = r.id_tov  
   
    left join #tovLLED tovLLED on tovLLED.id_tov = o.id_tov  
  
 inner join M2..tov_kontr tk with (  INDEX(PK_tov_kontr)) on tk.id_kontr=o.id_kontr and tk.id_tov=o.id_tov and tk.Number_r=@N   
 and tk.rasp_all=1  
     
 inner join (  
    select top 100 ROW_NUMBER() over (order by time_add) N_kor   
    from m2..Raspr_zadanie   ) k on k.N_kor < = case when   
        tovLLED.id_tov is not null   
        or @date_rasp in ({d'2018-12-28'},{d'2018-12-29'},{d'2018-12-30'},{d'2018-12-31'})   
        then 100   
        when tk.srok_godnosti <=5 then 3  else 1 end -- для всех товаров, кроме ЛЛ иЕД - 1 лишняя коробка  
  
  
  
 inner join m2..rasp r2 with (index (PK_rasp) )   
  on r.id_tov=r2.id_tov and r.id_tt=r2.id_tt and r2.Number_r=@N and o.id_kontr  = r2.id_kontr  
  
  
 left join #id_tt_Макс_кор tm on tm.id_tt=r.id_tt   
  
 left join #rh rh on rh.id_tt=r.id_tt  
  
    inner join M2..Tovari tov on tov.id_tov = o.id_tov  
    --left join M2..Group_koef_raspr g on g.id_group = tov.Group_raspr and r2.tt_format_rasp = g.tt_format_group  
  
    left join #rasp_gr_tt rgt on rgt.id_tt = r.id_tt and rgt.id_group_rasp = tov.Group_raspr  
  
 where isnull(rh.q_kor_add,0) + k.N_kor < = tm.Макс_кор  
 and ОсталосьРаспр >0  
   
 and ISNULL(rgt.Снять_еще,0)<=0  
   
 -- не превысить максимальное, но кроме Статус2  
 and ((r2.q_FO + r2.q_raspr + tk.Kolvo_korob * k.N_kor < = r2.q_max_ost) or r2.q_max_ost<=0 or r2.zc_status=2)   
     
    --and not (tov.id_group in (65, 10174, 10176) and g.type_gr='КоэфОст')  -- товар из ФРОВ кроме борщевого набора без Избытков  
    and not (tov.id_group in (65, 10174, 10176) and r2.koef_ost_pr_rasp is not null)  -- товар из ФРОВ кроме борщевого набора без Избытков  
 ) a  
  
 where a.rn <= a.ОсталосьРаспр  
    and @dont_use_wait_sklad=0 and @sql_raspr=0 and @only_zakaz=0  
      
  
 -- drop table #rh  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 124, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..rasp   
 set q_raspr = rasp1.znach + rasp.q_raspr , q_ko_ost = rasp.q_ko_ost +rasp1.znach  
 from M2..rasp with (rowlock, index(ind1))  
 inner join   
 (select id_tt , id_tov ,  id_kontr , SUM(znach) znach   
 from #rasp   
 group by id_tt , id_tov  , id_kontr ) rasp1 on rasp1.id_tt=rasp.id_tt and rasp1.id_tov=rasp.id_tov and rasp1.id_kontr=rasp.id_kontr  
 where rasp.number_r=@N  
  
  
/**  
-- новый кусок... меняет распределение с 4 на 2 тип, если в 4 попали тт и тов, которые попали в снитие товара по остатку.  
-- и удаляет снятие с Остатка, если это еще и Избытках  
  
-- Declare @N int = 95337  
update #rasp  
set rn_r=2  
--select  *  
from #rasp rh   
inner join   
(select top 1 with ties  rh.id_tt , rh.id_tov , prg.type_add_kor , rh.sort_r  
from #rasp rh   
inner join M2..prev_Group_raspr prg   on rh.id_tt = prg.id_tt and rh.id_tov = prg.id_tov and prg.number_r = @N  
where rh.rn_r=4 and prg.type_prev=2  
order by ROW_NUMBER() over (partition by  rh.id_tt , rh.id_tov order by  prg.type_add_kor)  
) f on f.sort_r = rh.sort_r and f.id_tt = rh.id_tt and f.id_tov = rh.id_tov  
  
-- Declare @N int = 95337  
delete M2..prev_Group_raspr   
--select *  
from M2..prev_Group_raspr prg    
inner join   
(select top 1 with ties  rh.id_tt , rh.id_tov , prg.type_add_kor   
from #rasp rh   
inner join M2..prev_Group_raspr prg   on rh.id_tt = prg.id_tt and rh.id_tov = prg.id_tov and prg.number_r = @N  
where rh.rn_r=2 and prg.type_prev=2  
order by ROW_NUMBER() over (partition by  rh.id_tt , rh.id_tov order by  prg.type_add_kor)  
) f on  f.id_tt = prg.id_tt and f.id_tov = prg.id_tov and f.type_add_kor = prg.type_add_kor and prg.type_prev=2  
where prg.number_r= @N  
  
**/  
  
  
  
/**  
--select *  
update M2..raspr_hystory  
set rn_r=2  
from M2..raspr_hystory rh    
inner join   
(select top 1 with ties rh.number_r , rh.id_tt , rh.id_tov , prg.type_add_kor , rh.id  
from M2..raspr_hystory rh    
inner join M2..prev_Group_raspr prg   on rh.id_tt = prg.id_tt and rh.id_tov = prg.id_tov and prg.number_r = rh.number_r  
where rh.rn_r=4 and prg.type_prev=2  
order by ROW_NUMBER() over (partition by rh.number_r , rh.id_tt , rh.id_tov order by  prg.type_add_kor)  
) f on f.id = rh.id  
  
delete M2..prev_Group_raspr   
from M2..prev_Group_raspr prg    
inner join   
(select top 1 with ties rh.number_r , rh.id_tt , rh.id_tov , prg.type_add_kor , rh.id   
from M2..raspr_hystory rh    
inner join M2..prev_Group_raspr prg   on rh.id_tt = prg.id_tt and rh.id_tov = prg.id_tov and prg.number_r = rh.number_r  
where rh.rn_r=2 and prg.type_prev=2  
order by ROW_NUMBER() over (partition by rh.number_r , rh.id_tt , rh.id_tov order by  prg.type_add_kor)  
) f on f.number_r = prg.number_r and f.id_tt = prg.id_tt and f.id_tov = prg.id_tov and f.type_add_kor = prg.type_add_kor and prg.type_prev=2  
**/  
  
  
     insert into m2..rasp_temp  
     (number_r , id_tov , id_tt , id_kontr  , rn_r ,  
   znach  ,  sort_r ,  prohod , sort_ost  , sort_pr  , p1  , znach_sum , znach_sum_narast , rn_gr , type_add_kor  ,  
   tt_format_rasp , id_group_rasp , price_rasp , koef_ost_pr_rasp )  
  select *  
     from #rasp  
  
 insert into M2..raspr_hystory  
  ([number_r]  
  ,[id_tov]  
  ,[id_tt]  
  ,[id_kontr]  
  ,[rn_r]  
  ,[znach]  
  ,[sort_rz]  
  ,[prohod]  
  ,[sort_ost]  
  ,[sort_pr])   
 select   
 number_r,   
 id_tov ,   
 id_tt ,   
 id_kontr  ,   
 rn_r ,  
 znach  ,    
 sort_r ,    
 prohod ,   
 sort_ost  ,   
 sort_pr   
  from #rasp  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 124001, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
  
 ---  а теперь проверить на превышение по группам еще раз  
  -- если есть превышение, то засчитать его в Избытки raspr_hystory с типом 4   
  -- и добавить в prev_Group_raspr и type_prev=1   
  
  
  
 ----------------------------------------------------------------------------------------  
 -- исправить tt в соответствии с приоритетами и убрать из распределения q_wait_sklad  
 -- и несколько характеристик  
  
  
 if exists (select * from #wait)  
 begin  
  
  create table #tt_waiting (id_tov int , id_kontr int , id_tt int , q_raspr real)  
  
   
  --declare @N int = 36734  
  --delete from #tt_waiting  
  insert into #tt_waiting  
     --declare @N int = 80357  
  select r.id_tov , r.id_kontr , r.id_tt , r.q_raspr  
  from  M2..rasp r  
  inner join #wait w on w.id_tov = r.id_tov  
  inner join M2..tov_kontr tk with (index(IX_tov_kontr_1) ) on tk.Number_r = @N and tk.id_tov = r.id_tov  
  and tk.id_kontr = r.id_kontr  
  where tk.q_wait_sklad>0  
  
    
 --declare @N int = 36734   
  update M2..rasp  
  set q_raspr = 0  
  --declare @N int = 36734   
  --select *  
  from M2..rasp r   
  inner join #tt_waiting rr on rr.id_tov = r.id_tov and rr.id_tt = r.id_tt and r.id_kontr = rr.id_kontr  
  where r.Number_r = @N   
    
     
 insert into m2..Raspr_tt_waiting (number_r , id_tov , id_kontr , id_tt, q_raspr )  
 select @N , id_tov , id_kontr , id_tt , q_raspr  
 from #tt_waiting  
  
  
 -- удалить тт, которые ушли в ожидаемые  
 delete M2..raspr_hystory  
 from #tt_waiting r  
 inner join M2..raspr_hystory rh with (index(ind1_h)) on rh.id_tov = r.id_tov and rh.id_tt = r.id_tt and rh.id_kontr = r.id_kontr  
 where rh.number_r = @N  
    
    
  
 -- убрать из остатка на складе для распределения ожидаемый товар  
 update m2..tov_kontr  
 set q_ost_sklad = tk.q_ost_sklad - tk.q_wait_sklad  
 from m2..tov_kontr tk  
 inner join #wait w on w.id_tov = tk.id_tov  
 where tk.Number_r = @N   
  
    -- убрать из заказов покупателям  
      
    update M2..rasp_zakaz_pok  
  set q_raspr =0 , q_raspr_cur=0  
  --declare @N int = 36734   
  --select *  
  from M2..rasp_zakaz_pok r   
  inner join #tt_waiting rr on rr.id_tov = r.id_tov and rr.id_tt = r.id_tt and ISNULL( r.id_kontr_new, r.id_kontr) = rr.id_kontr  
  where r.Number_r = @N   
      
  
  
  
 end  
  
  
 ----------------------------------------------------------------------------------------  
  
  
/**  
--вернуть обратно распределение вендинга  
if exists (Select * from #MCK)  
begin   
  
--вернуть обратно все подмены с +1000000  
update r  
set  number_r =  @N   
from M2..rasp r  
where r.number_r = @N + 1000000   
  
update r  
set   number_r = @N   
from M2..tt_tov_kontr r  
where r.number_r = @N + 1000000   
  
update r  
set q_ost_sklad = r.q_ost_sklad + n.q_nuzno_fact , q_ost_sklad_calc = r.q_ost_sklad_calc + n.q_nuzno_fact  
from M2..tov_kontr r  
inner join #nuzno_mck n on n.id_tov = r.id_tov and n.id_kontr = r.id_kontr  
where r.number_r = @N  
  
--select *  
update r  
set q_ost_sklad = r.q_ost_sklad + a.q_nuzno_fact  
from M2..tov_kontr_date r  
inner join  #zc_tov_kontr_date a on a.id_tov = r.id_tov and a.id_kontr = r.id_kontr and a.date_ost = r.date_ost  
where r.number_r = @N  
  
update r  
set q_ost_zal = r.q_ost_zal + a.q_nuzno_fact  
--select *  
from M2..tov_kontr_zal r  
inner join #zc_tov_kontr_zal a on a.id_tov = r.id_tov and a.id_kontr = r.id_kontr and a.id_zal = r.id_zal  
where r.number_r = @N  
  
  
  
end  
**/  
  
----------------------------------------------------------------------------  
  
  
  
  
 -- убрать разницу из последнего распределения до колва, которое на остатках  
  
 -- drop table #id_tt_Макс_кор  
 -- drop table #ost_r  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 130, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 Select rh.id_tov , rh.id_kontr , rh.id_tt ,rh.znach ,  
 ROW_NUMBER() over (partition by rh.id_tov , rh.id_kontr order by rh.rn_r desc , sort_rz desc ) rn  
 into #a_1  
 from M2..raspr_hystory rh with (  index (ind1_h))   
 where rh.number_r=@N   
 and znach>0   
  
 SELECT tk.id_tov , tk.id_kontr , q_rasp , tk.q_ost_sklad  
 into #b_1  
 from M2..tov_kontr tk with (  INDEX(IX_tov_kontr_1))  
 inner join  
  
 (select r.number_r,r.id_tov , r.id_kontr , SUM(r.q_raspr) q_rasp  
 from M2..rasp r with (index (ind1) )  
 where r.number_r=@N  
 group by r.number_r,r.id_tov , r.id_kontr  
 ) r on tk.id_kontr=r.id_kontr and tk.id_tov=r.id_tov  
  where tk.number_r=@N  
  and q_rasp > tk.q_ost_sklad  
    
 --select r.q_raspr , b.q_rasp - b.q_ost_sklad , a.znach  
 update M2..rasp  
 set q_raspr = r.q_raspr - (b.q_rasp - b.q_ost_sklad)  
 from M2..rasp r with (rowlock, index(ind1))   
 inner join #a_1 a on a.rn=1 and a.id_kontr=r.id_kontr and a.id_tov=r.id_tov and a.id_tt=r.id_tt  
  
 inner join #b_1 b on b.id_tov=r.id_tov and b.id_kontr=r.id_kontr  
  
 where r.number_r=@N  
 and a.znach > b.q_rasp - b.q_ost_sklad  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 140, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
 -- drop table #a_1  
 -- drop table #b_1  
  
  
  
 --------------------------------------  
 -- замену делаем на обратно с полных аналогов  
    
  -- обновляем tov_kontr_date  
  update M2..tov_kontr_date  
  set id_tov = tk.id_tov_init ,  
  q_ost_sklad = tkd.q_ost_sklad / ISNULL(tov.koef_pvz,1)  
  FROM M2..tov_kontr_date (rowlock) tkd  
  inner join M2..tov_kontr tk with (index(IX_tov_kontr_1) ) on tk.Number_r = @N and tk.id_tov = tkd.id_tov  
  and tk.id_kontr = tkd.id_kontr  
    
  inner join M2..tov on tov.Number_r = @N and tov.id_tov = tk.id_tov_init and tov.id_tov_pvz is not null  
    
  where tkd.Number_r = @N and tkd.id_tov <> tk.id_tov_init  
  
  -- обновляем tov_kontr_date  
  update [M2].[dbo].[tov_kontr_zal]  
  set id_tov = tk.id_tov_init ,   
  q_ost_zal = tkd.q_ost_zal / ISNULL(tov.koef_pvz,1)  
  FROM [M2].[dbo].[tov_kontr_zal] (rowlock) tkd  
  inner join M2..tov_kontr tk with (index(PK_tov_kontr) ) on tk.Number_r = @N and tk.id_tov = tkd.id_tov  
  and tk.id_kontr = tkd.id_kontr  
  
  inner join M2..tov on tov.Number_r = @N and tov.id_tov = tk.id_tov_init and tov.id_tov_pvz is not null  
    
  where tkd.Number_r = @N and tkd.id_tov <> tk.id_tov  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 141, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 --/**   
  -- обновляем tt_tov_kontr  
  
  update M2..tt_tov_kontr  
  set id_tov = tk.id_tov_init ,  
  q_FO = ttk.q_FO / ISNULL(tov.koef_pvz,1),   
  q_rashod_fact = ttk.q_rashod_fact / ISNULL(tov.koef_pvz,1) ,  
  max_ost_tt_tov = ttk.max_ost_tt_tov / ISNULL(tov.koef_pvz,1),  
  min_ost_tt_tov = ttk.min_ost_tt_tov / ISNULL(tov.koef_pvz,1),  
  q_min_ost = ttk.q_min_ost / ISNULL(tov.koef_pvz,1)  
  FROM M2..tt_tov_kontr (rowlock) ttk  
  inner join M2..tov_kontr tk with (index(PK_tov_kontr) ) on   
  tk.Number_r = @N and tk.id_tov = ttk.id_tov and tk.id_kontr = ttk.id_kontr  
  inner join M2..tov  on tov.Number_r = @N and tov.id_tov = tk.id_tov_init and tov.id_tov_pvz is not null  
    
  --inner join m2..tt on tt.id_TT = ttk.id_tt --and tt.tt_format<>10  
    
  where ttk.Number_r = @N and ttk.id_tov <> tk.id_tov_init  
    
  insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 142, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 --**/  
  
      
    While 1=1  
    BEGIN  
    BEGIN TRY      
      
    /**  
       update M2..rasp  
   set id_tov = tk.id_tov_init ,  
    q_FO = q_FO / ISNULL(tov.koef_pvz,1),  
    q_plan_pr = q_plan_pr / ISNULL(tov.koef_pvz,1),  
    q_min_ost = q_min_ost / ISNULL(tov.koef_pvz,1),  
    q_raspr = q_raspr / ISNULL(tov.koef_pvz,1),  
    q_ko_ost = q_ko_ost / ISNULL(tov.koef_pvz,1) ,  
    q_nuzno = q_nuzno / ISNULL(tov.koef_pvz,1),  
    q_max_ost = q_max_ost / ISNULL(tov.koef_pvz,1)  
   FROM M2..rasp ttk with(rowlock, index(ind1))   
   inner join M2..tov_kontr tk with (index(PK_tov_kontr) )   
    on tk.Number_r = @N   
     and tk.id_tov = ttk.id_tov  
     and tk.id_kontr = ttk.id_kontr  
   inner join M2..tov with ( index(pk_tov))   
    on tov.Number_r = @N   
     and tov.id_tov = tk.id_tov_init   
     and tov.id_tov_pvz is not null  
   where ttk.Number_r = @N and ttk.id_tov <> tk.id_tov_init  
  **/  
      
   -- обновляем rasp  
   --declare @N int = 77672  
   update M2..rasp  
   set id_tov = tk.id_tov_init ,  
    q_FO = q_FO / ISNULL(tk.koef_pvz,1),  
    q_plan_pr = q_plan_pr / ISNULL(tk.koef_pvz,1),  
    q_min_ost = q_min_ost / ISNULL(tk.koef_pvz,1),  
    q_raspr = q_raspr / ISNULL(tk.koef_pvz,1),  
    q_ko_ost = q_ko_ost / ISNULL(tk.koef_pvz,1) ,  
    q_nuzno = q_nuzno / ISNULL(tk.koef_pvz,1),  
    q_max_ost = q_max_ost / ISNULL(tk.koef_pvz,1)  
   FROM M2..rasp ttk with( index(PK_rasp))   
     
 inner join   
 (  
 --declare @N int = 77672  
 select tk.* , tov.koef_pvz  
 from   
 (select tov.* from M2..tov with ( index(pk_tov))   
 where tov.Number_r=@N and  tov.id_tov_pvz is not null) tov  
   
    inner join M2..tov_kontr tk with (index(PK_tov_kontr) )   
    on tk.Number_r = @N   
     and tov.id_tov = tk.id_tov_init ) tk  
   
 /**  
   M2..tov_kontr tk with (index(PK_tov_kontr) )   
   inner join M2..tov with ( index(pk_tov))   
    on tov.Number_r = @N   
     and tov.id_tov = tk.id_tov_init   
     and tov.id_tov_pvz is not null  
     **/  
       
   
    on  
     ttk.id_tov = tk.id_tov and ttk.id_kontr=tk.id_kontr and ttk.Number_r = @N   
       
     
   where ttk.id_tov <> tk.id_tov_init  
  
  insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 14201, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   --обновить историю  
   --declare @N int = 60248  
     
     
   update M2..raspr_hystory  
   set id_tov = tk.id_tov_init ,   znach = znach / ISNULL(tov.koef_pvz,1)   
     
     
   --select rh.id_tov , rh.znach , tov.koef_tov  
   FROM M2..raspr_hystory (rowlock) rh  
   inner join M2..tov_kontr (rowlock) tk   
    on tk.Number_r = @N and rh.id_tov=tk.id_tov and rh.id_kontr = tk.id_kontr  
   inner join M2..tov     
    on tov.Number_r = @N   
     and tov.id_tov = tk.id_tov_init   
     and tov.id_tov_pvz is not null  
   where rh.Number_r = @N and rh.id_tov <> tk.id_tov_init  
     
   -- и обновить prev_Group_raspr  
   --VERT закомментировал, блокирует другие распределения и запрос не выполняется  
   /*  
   --Declare @N int = 95337  
   update M2..raspr_hystory  
   set id_tov = tk.id_tov_init ,   znach = rh.znach / ISNULL(tov.koef_pvz,1)   
     
     
   --select rh.id_tov , rh.znach , tov.koef_tov  
   FROM M2..prev_Group_raspr (rowlock) rh  
   inner join M2..tov_kontr (rowlock) tk   
    on tk.Number_r = @N and rh.id_tov=tk.id_tov and rh.id_kontr = tk.id_kontr  
   inner join M2..tov     
    on tov.Number_r = @N   
     and tov.id_tov = tk.id_tov_init   
     and tov.id_tov_pvz is not null  
   where rh.Number_r = @N and rh.id_tov <> tk.id_tov_init  
   */     
     
     
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 1421, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 1421 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while  
     
  insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 143, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  -- обновляем tov_kontr  
    
    While 1=1  
    BEGIN  
    BEGIN TRY     
   update M2..tov_kontr  
   set id_tov = tk.id_tov_init ,   
    q_ost_sklad = q_ost_sklad / ISNULL(tov.koef_pvz,1) ,   
    q_ost_sklad_calc = q_ost_sklad_calc / ISNULL(tov.koef_pvz,1) ,   
    Kolvo_korob = Kolvo_korob / ISNULL(tov.koef_pvz,1) ,  
    q_wait_sklad = q_wait_sklad / ISNULL(tov.koef_pvz,1)  
   FROM M2..tov_kontr (rowlock) tk  
   inner join M2..tov     
    on tov.Number_r = @N and tov.id_tov = tk.id_tov_init and tov.id_tov_pvz is not null  
   where tk.Number_r = @N  and tk.id_tov <> tk.id_tov_init  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 1431, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 1431 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while  
    
  
    
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 144, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
  -- drop table #err_osn  
  
  
 ---------------------------- комплекты возвращаем обратно  
  
 if exists   
 (select *  
 from M2..tov_kontr   tk  
 inner join #complect c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
 )  
 begin  
 -- значит везде в таблицах меняем по комплектам данные обратно , те остатки по комплектам делим на колво во вложениях.  
 --  
  
 update M2..tov_kontr   
 set Kolvo_korob = 1 , q_ost_sklad = tk.q_ost_sklad / c.kolvo , q_wait_sklad = tk.q_wait_sklad / c.kolvo , q_ost_sklad_calc = tk.q_ost_sklad_calc / c.kolvo  
 from M2..tov_kontr (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 145, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tov_kontr_date   
 set q_ost_sklad = tk.q_ost_sklad / c.kolvo   
 from M2..tov_kontr_date (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 146, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tov_kontr_zal  
 set q_ost_zal = tk.q_ost_zal / c.kolvo   
 from M2..tov_kontr_zal (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 147, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..tt_tov_kontr  
 set q_rashod_fact = tk.q_rashod_fact / c.kolvo ,  
 max_ost_tt_tov = tk.max_ost_tt_tov / c.kolvo,  
 min_ost_tt_tov = tk.min_ost_tt_tov / c.kolvo,  
 q_FO = tk.q_FO / c.kolvo,  
 q_min_ost = tk.q_min_ost / c.kolvo,  
 q_plan_pr = tk.q_plan_pr / c.kolvo  
 from M2..tt_tov_kontr (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 148, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 update M2..rasp  
 set   
 q_FO = tk.q_FO / c.kolvo,  
 q_ko_ost = tk.q_ko_ost / c.kolvo ,  
 q_max_ost = tk.q_max_ost / c.kolvo ,  
 q_min_ost = tk.q_min_ost / c.kolvo,  
 q_nuzno = tk.q_nuzno / c.kolvo ,  
 q_plan_pr = tk.q_plan_pr / c.kolvo ,  
 q_raspr = tk.q_raspr / c.kolvo   
 from M2..rasp (rowlock) tk  
 inner join   
 (select c.id_tov , SUM(c.kolvo) kolvo  
 from #complect c  
 group by c.id_tov)  
  c on c.id_tov = tk.id_tov  
 where tk.Number_r = @N  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 149, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
 end  
  
      
------------------------------------------------------------------------------------------  
--  -- новая проверка, если в полных аналогах есть задвоенный товар 07.08.2018 Кривенко А.  
  
  
  update M2..tov_kontr  
  set id_kontr = zik.id_kontr  
  from M2..tov_kontr tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.new_id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
  
  update M2..tt_tov_kontr  
  set id_kontr = zik.id_kontr , id_kontr_v = zik.id_kontr  
  from M2..tt_tov_kontr tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.new_id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
       
  update M2..rasp  
  set id_kontr = zik.id_kontr, id_kontr_init =  zik.id_kontr  
  from M2..rasp tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.new_id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
    
    
  update M2..tov_kontr_date  
  set id_kontr = zik.id_kontr  
  from M2..tov_kontr_date tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.new_id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
  
  update M2..tov_kontr_zal  
  set id_kontr = zik.id_kontr  
  from M2..tov_kontr_zal tk  
  inner join #zamrena_id_kontr zik on zik.id_tov = tk.id_tov and zik.new_id_kontr = tk.id_kontr  
  where tk.Number_r=@N  
      
      
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 14901, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
       
------------------------------------------------------------------------------------------  
  
-- удалить чрезмерную поставку в регионы заморозки (более 40 коробок)  
  
begin try  
  
 -- получить план продаж, но не исправенный на полные аналоги  
 select q_plan_pr , id_tt, id_tov  
 into #ttk_init   
 from M2..tt_tov_kontr_init   ttk_init   
 where ttk_init.number_r=@N  
 create clustered index ind1 on #ttk_init (id_tt,id_tov)  
   
create table #cap_delivery (id_tt int , Изб int)  
 -- в этой табличке история распределения товаров по магазинам, где излишки  
 create table #cap_delivery_2 (id bigint, id_tov int, id_tt int , q real , Kolvo_korob real , rn int , ves real)  
 create table #cap_delivery_3 ( id_tt int  , rn int, Изб real )   
 create table #cap_delivery_4 ( id int , id_tt int  , id_tov int , q real)     
-- удалить чрезмерную поставку в регионы заморозки (более 40 коробок)  
  
  
-- таблица с магазинами, по которым более 40 коробок  
--Declare  @N int =  81597  
  
  
delete from #cap_delivery  
insert into #cap_delivery  
select r.id_tt , sum(r.znach / tk.Kolvo_korob) -40 Изб  
from M2..raspr_hystory r    
  inner join [Reports].[dbo].[tovar_temp_regim] tr on r.id_tov = tr.id_tov and tr.id_TempRegim=14  
  inner join M2..tt on tt.id_TT = r.id_tt and tt.id_group = 4 -- and tt.adress like ('%санкт%')  
  inner join M2..tov_kontr tk   on r.number_r = tk.Number_r and r.id_tov = tk.id_tov and r.id_kontr = tk.id_kontr  
  --inner join (select d.id_tt  from #dtt_z d group by d.id_tt having COUNT(*)>3) dtt on r.id_tt = dtt.id_tt  
    
  left join  
(select dt1.id_tt , MIN(dt1.Date_tt) Date_tt_min  
from #dtt_z dt  
left join  #dtt_z  dt1 on DT.id_tt = dt1.id_tt and dt.date_tt = dateadd(day,1,dt1.Date_tt)  
where (dt1.sum_z<100 or dt1.id_tt is null)  
and DT.date_tt > DATEADD(day,-18,getdate())   
group by dt1.id_tt) tt1 on tt1.id_tt = r.id_tt   
  
where r.number_r=@N  and   
(tt1.id_tt is null or GETDATE() > DATEADD(day,3,tt1.Date_tt_min))   
group by r.id_tt  
 having sum(r.znach / tk.Kolvo_korob)>40   
   
   
 if CONVERT(date,GETDATE()) not in ({d'2018-12-26'},{d'2018-12-27'})  
 if exists (select * from #cap_delivery)  
 begin  
    
--Declare  @N int = 65009  
  
  delete from #cap_delivery_2   
  insert into #cap_delivery_2   
 select r.id  , r.id_tov, r.id_tt , 1 --r.znach / tk.Kolvo_korob   
 , tk.Kolvo_korob ,     --                          sort_ost - лежит план продаж q_plan  
 ROW_NUMBER() over (  partition by r.id_tt order by r.rn_r desc,   
  
 case when rasp.q_FO - ttk_init.q_plan_pr + rasp.q_raspr - k.kor * tk.Kolvo_korob>= 0 then 0 else 1 end ,  
   
 k.kor *  
 case when rasp.q_FO - ttk_init.q_plan_pr + rasp.q_raspr - k.kor * tk.Kolvo_korob>= 0 then -1 else 1 end ,  
   
 r.sort_ost*pr.price) rn , 0   
  
from M2..raspr_hystory r    
inner join  #cap_delivery zp on r.id_tt = zp.id_tt  
  inner join [Reports].[dbo].[tovar_temp_regim] tr on r.id_tov = tr.id_tov and tr.id_TempRegim=14  
  inner join M2..tov_kontr tk   on r.number_r = tk.Number_r and r.id_tov = tk.id_tov and r.id_kontr = tk.id_kontr  
  inner join (select top 100 ROW_NUMBER() over (order by date_add) kor from jobs..Jobs_log   j) k on k.kor<= FLOOR(r.znach / tk.Kolvo_korob+0.01)  
  inner join M2..Tovari t on t.id_tov = r.id_tov and t.CatAss <> 3  
  inner join Reports..Price_1C_tov pr on pr.id_tov = r.id_tov  
  inner join M2..rasp   rasp on rasp.number_r=@N and r.id_tt = rasp.id_tt and r.id_tov = rasp.id_tov and rasp.q_zakaz=0  
  inner join #ttk_init  ttk_init on r.id_tt = ttk_init.id_tt and r.id_tov = ttk_init.id_tov     
  where r.number_r=@N  
    
    
  
  --select * from #cap_delivery_2   
    
  -- вот, как раз строка raspr_hystory до которой нужно удалить, и на ней тоже сделать update, если получилось больше, чем избыток свыше 40  
  --Declare  @N int = 65009  
    
  delete from #cap_delivery_3  
  insert into #cap_delivery_3  
  select top 1 with ties  a.id_tt , a.rn ,  a.q - a.Изб  
  from   
  (  
  select  z2.id_tt , z2.rn , z.Изб , sum(z22.q) q  --min(z2.rn),  sum(z22.q)  
  from #cap_delivery_2  z2  
  inner join #cap_delivery z on z.id_tt = z2.id_tt  
  inner join #cap_delivery_2 z22 on z2.id_tt = z22.id_tt and z22.rn<=z2.rn  
  group by z2.id_tt, z.Изб, z2.rn  
  having  sum(z22.q) >= z.Изб  
  )a  
  order by row_number() over ( partition by a.id_tt order by a.rn)  
    
  
    
  -- вот товары, которые нужно удалить из распределения  
  --Declare  @N int = 65009  
    
  delete from #cap_delivery_4  
  insert into #cap_delivery_4  
  select z2.id , z2.id_tt ,z2.id_tov , SUM(z2.Kolvo_korob)  
  from #cap_delivery_2 z2  
  inner join #cap_delivery_3 z3 on z2.id_tt = z3.id_tt and z2.rn<=z3.rn  
  --where z2.q - case when z2.rn=z3.rn then z3.Изб else 0  end>0  
  --and floor(((z2.q - case when z2.rn=z3.rn then z3.Изб else 0  end )/ z2.ves +0.01) / z2.Kolvo_korob) * z2.Kolvo_korob>0  
  group by z2.id , z2.id_tt ,z2.id_tov  
    
   
  --Declare  @N int = 65009  
  insert into m2..cap_delivery   
      ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_zone]        
      ,[q]  
      ,[type_ins])  
  select @N number_r, c.id_tt , c.id_tov , 0 , c.q  ,1  
  from #cap_delivery_4 c  
  
    
   -- удаление из rasp  
     
   update r  
   set q_raspr = r.q_raspr - z.q  
     
   -- Declare  @N int = 65009   
   -- select *  
   from M2..rasp r   
   inner join   
   (select z.id_tov , z.id_tt , sum(z.q) q  
   from #cap_delivery_4 z   
   group by z.id_tov , z.id_tt) z on r.id_tt = z.id_tt and r.id_tov = z.id_tov  
   where r.number_r =  @N and r.q_raspr - z.q>-0.001  
    
  --select * , r.znach - z2.q  
  update r  
  set znach = r.znach - z2.q  
  from M2..raspr_hystory r   
  inner join #cap_delivery_4 z2 on r.id = z2.id  
  where r.znach - z2.q>-0.001  
    
  end  
    
------------------------------------------------------------------------------------------    
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 14902, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
  
------------------------------------------------------------------------------------------  
  
-- удалить чрезмерную поставку , более 100% от максимального веса продажи  
  
-- только для зон , что есть в tt_zone_cap_delivery (2249,602,338,4550)  
--Declare @id_zone int  
--Select @id_zone = id_zone  
--from m2..Raspr_zadanie   
--where Number_r =@n  
  
if @id_zone in   
(select tz.id_zone  
from M2..tt_zone_cap_delivery tz  
inner join  
(select max(tz.date_r) date_r  
from M2..tt_zone_cap_delivery tz) tz1 on tz1.date_r=tz.date_r)  
begin  
  
-- макс дата из tt_zone_cap_delivery   
  
--Declare @id_zone int = 602 , @N int = 65925  
  
-- таблица с магазинами, где избыточная поставка  
--declare @N int = 81597 , @id_zone int  = 602  
delete from #cap_delivery   
insert into #cap_delivery  
  
  
select a.id_tt , a.вес - b.q_max  
from   
(  
--Declare @id_zone int = 602 , @N int = 81597  
select r.id_tt ,  
SUM(r.q_raspr*tov.ves) вес  
from M2..rasp r with ( index(ind1))  
inner join m2..tov   tov on r.number_r=tov.Number_r and r.id_tov = tov.id_tov and tov.Skladir_1C=1  
inner join M2..tov_kontr tk   on r.number_r = tk.Number_r and r.id_tov = tk.id_tov and r.id_kontr = tk.id_kontr and tk.rasp_all=0    
--inner join (select d.id_tt  from #dtt_z d group by d.id_tt having COUNT(*)>3) dtt on r.id_tt = dtt.id_tt  
  
inner join M2..Tovari t on t.id_tov = r.id_tov and not (t.Vivedena=0 and t.IsUpakovka=0 and t.IsUpakovka=0  
and (t.bez_ostatkov=1 or t.hoz_tovar=1 or t.id_group in (10302,10254) or (t.Ves<0.05 and t.id_group=10205) ) ) and  t.CatAssStr = 'особыетовары'  
  
  
left join  
(select dt1.id_tt , MIN(dt1.Date_tt) Date_tt_min  
from #dtt_z dt  
left join  #dtt_z  dt1 on DT.id_tt = dt1.id_tt and dt.date_tt = dateadd(day,1,dt1.Date_tt)  
where (dt1.sum_z<100 or dt1.id_tt is null)  
and DT.date_tt > DATEADD(day,-18,getdate())   
group by dt1.id_tt) tt1 on tt1.id_tt = r.id_tt   
  
  
where r.number_r=@N and   
(tt1.id_tt is null or GETDATE() > DATEADD(day,3,tt1.Date_tt_min))   
group by r.id_tt   
)a  
inner join  
(  
--Declare @id_zone int = 602 , @N int = 65932  
select tz.id_tt, q_max  
from M2..tt_zone_cap_delivery tz  
inner join  
(select max(tz.date_r) date_r  
from M2..tt_zone_cap_delivery tz) tz1 on tz1.date_r=tz.date_r  
where tz.id_zone = @id_zone  
)b on a.id_tt = b.id_tt  
where a.вес > b.q_max --and a.вес> 30 --case when @id_zone in (2249,602) then 100 else 50 end  
  
  
 if exists (select * from #cap_delivery)  
 begin  
   
--declare @N int = 81597 , @id_zone int  = 602  
   
   
 delete from #cap_delivery_2   
  
 insert into #cap_delivery_2   
   
 --declare @N int = 81597 , @id_zone int  = 602  
 select r.id  , r.id_tov, r.id_tt , tk.Kolvo_korob * tov.ves  ,tk.Kolvo_korob ,    
 -- rn_r , k.kor , r.sort_ost * pr.price , rasp.q_FO - ttk_init.q_plan_pr + rasp.q_raspr - k.kor * tk.Kolvo_korob  ,  
               --                          sort_ost - лежит план продаж q_plan  
 ROW_NUMBER() over ( partition by r.id_tt order by r.rn_r desc,   
   
 case when rasp.q_FO - ttk_init.q_plan_pr + rasp.q_raspr - k.kor * tk.Kolvo_korob>= 0 then 0 else 1 end ,  
   
 k.kor *  
 case when rasp.q_FO - ttk_init.q_plan_pr + rasp.q_raspr - k.kor * tk.Kolvo_korob>= 0 then -1 else 1 end ,  
   
 r.sort_ost * pr.price ) rn , tov.ves --, rn_r, sort_rz  
  
--select r.znach / tk.Kolvo_korob , r.* , (r.sort_rz - k.kor*tk.Kolvo_korob) / r.sort_ost , tk.Kolvo_korob  
  from M2..raspr_hystory r    
  inner join   #cap_delivery zp on r.id_tt = zp.id_tt  
  inner join M2..tov_kontr tk   on r.number_r = tk.Number_r and r.id_tov = tk.id_tov and r.id_kontr = tk.id_kontr and tk.rasp_all=0   
  inner join M2..tov   on r.number_r = tov.Number_r and r.id_tov = tov.id_tov and tov.Skladir_1C=1  
    
  inner join M2..Tovari t on t.id_tov = r.id_tov and not (t.Vivedena=0 and t.IsUpakovka=0 and t.IsUpakovka=0  
  and (t.bez_ostatkov=1 or t.hoz_tovar=1 or t.id_group in (10302,10254) or (t.Ves<0.05 and t.id_group=10205) )) and t.CatAssStr <> 'особыетовары'  
  
  
  inner join (select top 100 ROW_NUMBER() over (order by date_add) kor from jobs..Jobs_log   j) k on k.kor<= FLOOR(r.znach / tk.Kolvo_korob+0.01)  
  inner join Reports..Price_1C_tov pr on pr.id_tov = r.id_tov  
  inner join M2..rasp   rasp on rasp.number_r=@N and r.id_tt = rasp.id_tt and r.id_tov = rasp.id_tov  and rasp.q_zakaz=0  
  inner join #ttk_init ttk_init on  r.id_tt = ttk_init.id_tt and r.id_tov = ttk_init.id_tov   
  --inner join M2..Tovari t on t.id_tov = r.id_tov and t.CatAss <> 3  
  where  r.number_r=@N  
  --order by (r.sort_rz - k.kor*tk.Kolvo_korob) / r.sort_ost desc  
    
  --select * from #cap_delivery_2   
    
  -- вот, как раз строка raspr_hystory до которой нужно удалить, и на ней тоже сделать update, если получилось больше, чем избыток   
    
--declare @N int = 81597 , @id_zone int  = 602  
    
  delete from #cap_delivery_3  
  
  insert into #cap_delivery_3  
  select top 1 with ties  a.id_tt , a.rn ,  a.q - a.Изб  
  from   
  (  
  select  z2.id_tt , z2.rn , z.Изб , sum(z22.q) q  --min(z2.rn),  sum(z22.q)  
  from #cap_delivery_2  z2  
  inner join  #cap_delivery z on z.id_tt = z2.id_tt  
  inner join #cap_delivery_2 z22 on z2.id_tt = z22.id_tt and z22.rn<=z2.rn  
  group by z2.id_tt, z.Изб, z2.rn  
  having  sum(z22.q) >= z.Изб  
  )a  
  order by row_number() over ( partition by a.id_tt order by a.rn)  
    
 --select * from #cap_delivery_3 c  
 --where c.id_tt=11201  
 --order by rn  
    
  -- вот товары, которые нужно удалить из распределения  
  
--declare @N int = 81597 , @id_zone int  = 602  
    
  delete from #cap_delivery_4  
   
  insert into #cap_delivery_4  
  select z2.id , z2.id_tt ,z2.id_tov , SUM(z2.Kolvo_korob)  
  from #cap_delivery_2 z2  
  inner join #cap_delivery_3 z3 on z2.id_tt = z3.id_tt and z2.rn<=z3.rn  
  --where z2.q - case when z2.rn=z3.rn then z3.Изб else 0  end>0  
  --and floor(((z2.q - case when z2.rn=z3.rn then z3.Изб else 0  end )/ z2.ves +0.01) / z2.Kolvo_korob) * z2.Kolvo_korob>0  
  group by z2.id , z2.id_tt ,z2.id_tov  
   
  --create unique clustered index ind1 on #cap_delivery_4 (id_tt,id_tov)  
   
   -- удаление из rasp  
     
  insert into m2..cap_delivery   
      ([number_r]  
      ,[id_tt]  
      ,[id_tov]  
      ,[id_zone]  
      ,[q]  
      ,[type_ins])  
  select @N number_r, c.id_tt , c.id_tov , @id_zone , c.q , 2  
  from #cap_delivery_4 c  
   
 /**  
  select tt.name_TT , t.Name_tov , SUM(c.q)  
  from #cap_delivery_4 c   
  inner join M2..Tovari t on c.id_tov = t.id_tov  
  inner join M2..Tt tt on c.id_tt = tt.id_TT  
  group by tt.name_TT , t.Name_tov  
  order by tt.name_TT , t.Name_tov  
 **/  
    
    
   update r  
   set q_raspr = r.q_raspr - z.q  
     
   --  Declare @id_zone int = 602 , @N int = 65925  
   --select *    
   from M2..rasp r   
   inner join   
   (select z.id_tov , z.id_tt , sum(z.q) q  
   from #cap_delivery_4 z   
   group by z.id_tov , z.id_tt) z on r.id_tt = z.id_tt and r.id_tov = z.id_tov  
   where r.number_r =  @N and r.q_raspr - z.q>-0.001  
    
  --select * , r.znach - z2.q  
  update r  
  set znach = r.znach - z2.q  
  from M2..raspr_hystory r   
  inner join #cap_delivery_4 z2 on r.id = z2.id  
  where r.znach - z2.q>-0.001  
    
   
 end  
  
  
  
  
end  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 14950, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
end try  
begin catch  
  
  
    insert into jobs..error_jobs(job_name , message , number_step , id_job)  
    select @job_name , ERROR_MESSAGE() , 14950 , @id_job  
  
end catch  
  
--------------------------------------------------------------------------------------  
  
WHILE 1=1  
BEGIN  
 BEGIN TRY  
-- добавить в rasp нехватающие строки из tt_tov_kontr  
  
  insert into m2..rasp  
    ( [number_r]  
     ,[id_tt]  
     ,[id_tov]  
     ,[id_kontr]  
     ,[q_FO]  
     ,[q_plan_pr]  
     ,[q_min_ost]  
     ,[q_raspr]  
     ,[q_ko_ost]  
     ,[q_nuzno]  
     ,[sort_ost]  
     ,[sort_pr]  
     ,[id_kontr_init]  
     ,[q_max_ost]  
     ,[id_zal]  
     ,[date_add]  
     ,[id_tov_vz_r]  
     ,[q_nuzno_init]  
     ,[zc_status]  
     ,[zc_type_add]  
     ,[zc_koef_ost]  
     ,[zc_date_add]  
     ,[p1]  
     ,[p2]  
     ,[p3]  
     ,[q_fact_pickup]  
     ,[q_fact_pickup_itog]  
     ,[q_fo_fact]  
     ,[q_sales]  
     ,[q_zc]  
     ,[q_lost]  
     ,[p4]  
     ,[q_zakaz]  
     )  
  
  select   
      abs(ttk.[number_r])  
     ,ttk.[id_tt]  
     ,ttk.[id_tov]  
     ,ttk.[id_kontr]  
     ,ttk.[q_FO]  
     ,ttk.[q_plan_pr]  
     ,ttk.[q_min_ost]  
     ,0 [q_raspr]  
     ,ttk.[q_FO]-ttk.[q_plan_pr]  
     ,  
          
     master.dbo.maxz(ttk.[q_plan_pr] - ttk.[q_FO],0 )  
          
     [q_nuzno]  
     ,0  
     ,0  
     ,ttk.[id_kontr]  
     ,ttk.max_ost_tt_tov  
     ,ttk.[id_zal]  
     ,GETDATE()  
     ,null  
     ,null  
     ,null [zc_status]  
     ,null [zc_type_add]  
     ,null [zc_koef_ost]  
     ,null [zc_date_add]  
     ,null [p1]  
     ,null [p2]  
     ,null [p3]  
     ,null [q_fact_pickup]  
     ,null [q_fact_pickup_itog]  
     ,null [q_fo_fact]  
     ,null [q_sales]  
     ,null [q_zc]  
     ,null [q_lost]  
     ,null [p4]  
    ,ttk.[q_zakaz]  
          
  from m2..tt_tov_kontr ttk  
  left join m2..rasp r on ttk.Number_r = abs(r.Number_r) and ttk.id_tt = r.id_tt and ttk.id_tov = r.id_tov and ttk.id_kontr=r.id_kontr  
  where ttk.Number_r=@N and r.number_r is null  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 14951, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 14951 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )  
   RETURN         
   end  
  
   END CATCH   
 END--while  
------  
  
  
  
 --------------------------------------  
  
 -- drop table #rasp  
 ---- drop table #gr_tov  
  
 -- drop table #rasp_1  
  
 -- drop table #koef1  
 Update M2..Raspr_zadanie  
 set Date_end = GETDATE() , Status=3  
 from M2..Raspr_zadanie (rowlock) rz  
 where rz.Number_r=@N  
  
  
  
if @sql_raspr = 0  
begin  
    
if exists (select * from M2..rasp with ( index(ind1)) where number_r = @N)  
if (select r.Status from M2..Raspr_zadanie r   where Number_r = @N)=3  
begin  
  
 /* доработка Минеева (прямая запись в 1с) */  
 --declare @datRaspr datetime  
 --declare @date_rasp date , @n int = 18538   
 --Select @date_rasp = CONVERT(datetime, r.Date_r) FROM M2..Raspr_zadanie   r  
 --where r.Number_r = @N  
  
  
  
    
 if OBJECT_ID('tempdb..#Poryadok') is not null drop table #Poryadok  
  
 SELECT RegObecpech._Fld2726RRef as TTref, RegObecpech._Fld5207RRef as Gruppa, RegObecpech._Fld2728RRef as Sklad  
 INTO #Poryadok  
       FROM IzbenkaFin.dbo._InfoRg2725   RegObecpech  
       INNER JOIN  
       (SELECT RegObecpech._Period, RegObecpech._Fld2726RRef as TTref, RegObecpech._Fld5207RRef as Gruppa,  
       ROW_NUMBER() OVER (PARTITION BY RegObecpech._Fld2726RRef, RegObecpech._Fld5207RRef ORDER BY RegObecpech._Period Desc) as rn FROM IzbenkaFin.dbo._InfoRg2725   RegObecpech)  
        as VZ_Max  
        ON RegObecpech._Fld2726RRef = VZ_Max.TTref and RegObecpech._Fld5207RRef = VZ_Max.Gruppa and RegObecpech._Period = VZ_Max._Period and VZ_Max.rn = 1    
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 15010, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
  
  
    --create clustered index ind1 on #Poryadok (Gruppa,TTref)  
      
    --declare @N int = 60110  
    SELECT Tov.Ref as TovRef, TT.Ref as TTRef   
    into #a_RG  
       FROM M2..rasp as r with (INDEX (ind1),rowlock)  
       inner JOIN M2.dbo.Tovari   as Tov   
       ON r.id_tov = Tov.id_tov  
       inner JOIN M2.dbo.tt   as TT   
       ON r.id_tt = TT.id_TT  
       WHERE r.number_r = @N  
   
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 15011, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
   
 -- новая логика нахождения пар товар-тт, которые нужно удалить в 1С  
   
 -- добавляем в товар-тт из rasp склад и группуУРЗ   
 --declare @N int = 60110  
 SELECT Tov.Ref as TovRef, a.TTRef, Poryadok.Sklad, Poryadok.Gruppa   
 into #a_RG_1  
       FROM  #a_RG a  
       inner JOIN M2.dbo.Tovari   as Tov ON a.TovRef = Tov.Ref  
       inner JOIN #Poryadok as Poryadok  
       ON Tov.ГруппаУРЗ = Poryadok.Gruppa and a.TTRef = Poryadok.TTref  
        
 create clustered index ind1 on #Poryadok (Gruppa,Sklad,TTRef)   
    create clustered index ind1 on #a_RG_1 (Gruppa, Sklad,TTRef)  
      
 -- находим tt, которые есть в 1С у склада и группыУРЗ, но нет в rasp  
 select poryadok.*  
 into #poryadok_1  
 FROM   
 #Poryadok as Poryadok   
  left JOIN #a_RG_1 VZ_Zapros  
  ON VZ_Zapros.Gruppa = Poryadok.Gruppa and VZ_Zapros.Sklad = Poryadok.Sklad  and  VZ_Zapros.TTRef = Poryadok.TTref  
 where  VZ_Zapros.TTRef is null  
   
    drop index ind1 on #a_RG_1   
  create clustered index ind1 on #Poryadok_1 (Gruppa,Sklad)   
    create clustered index ind1 on #a_RG_1 (Gruppa, Sklad)     
      
 -- и вот получает новые товар-тт, которые нужно добавить в #a_RG  
 insert into #a_RG  
    select distinct VZ_Zapros.TovRef, VZ_Zapros.TtRef  
 from  #poryadok_1 Poryadok  
 inner join  #a_RG_1 VZ_Zapros   
 on VZ_Zapros.Gruppa = Poryadok.Gruppa and VZ_Zapros.Sklad = Poryadok.Sklad  
   
 /**        
 insert into #a_RG  
       SELECT DISTINCT VZ_Zapros.TovRef, Poryadok.TTRef  
 FROM   
 (SELECT Tov.Ref as TovRef, TT.Ref as TTRef, Poryadok.Sklad, Poryadok.Gruppa   
       FROM M2..rasp as r with (INDEX (ind1),rowlock)  
       LEFT OUTER JOIN M2.dbo.Tovari   as Tov   
       ON r.id_tov = Tov.id_tov  
       LEFT OUTER JOIN M2.dbo.tt   as TT  
       ON r.id_tt = TT.id_TT  
       left JOIN #Poryadok as Poryadok  
       ON Tov.ГруппаУРЗ = Poryadok.Gruppa and TT.Ref = Poryadok.TTref  
       WHERE r.number_r = @N ) VZ_Zapros  
  INNER JOIN #Poryadok as Poryadok  
  ON VZ_Zapros.Gruppa = Poryadok.Gruppa and VZ_Zapros.Sklad = Poryadok.Sklad        
    **/  
      
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 15012, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
    /**  
    --Declare @n int = 51574  
 Select *  
 into #a_RG  
 from   
 (SELECT Tov.Ref as TovRef, TT.Ref as TTRef   
       FROM M2..rasp as r with (INDEX (ind1),rowlock)  
       LEFT OUTER JOIN M2.dbo.Tovari   as Tov   
       ON r.id_tov = Tov.id_tov  
       LEFT OUTER JOIN M2.dbo.tt   as TT   
       ON r.id_tt = TT.id_TT  
       WHERE r.number_r = @N  
         
       UNION ALL  
         
       SELECT DISTINCT VZ_Zapros.TovRef, Poryadok.TTRef  
 FROM   
 (SELECT Tov.Ref as TovRef, TT.Ref as TTRef, Poryadok.Sklad, Poryadok.Gruppa   
       FROM M2..rasp as r with (INDEX (ind1),rowlock)  
       LEFT OUTER JOIN M2.dbo.Tovari   as Tov   
       ON r.id_tov = Tov.id_tov  
       LEFT OUTER JOIN M2.dbo.tt   as TT  
       ON r.id_tt = TT.id_TT  
       left JOIN #Poryadok as Poryadok  
       ON Tov.ГруппаУРЗ = Poryadok.Gruppa and TT.Ref = Poryadok.TTref  
       WHERE r.number_r = @N) VZ_Zapros  
  INNER JOIN #Poryadok as Poryadok  
  ON VZ_Zapros.Gruppa = Poryadok.Gruppa and VZ_Zapros.Sklad = Poryadok.Sklad  
 )b  
 **/  
  
 create clustered index ind1 on  #a_RG (TTRef,TovRef)  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 15020, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
--declare @Nvasql as nvarchar(4000),@result_table as varchar(100)='izbenkafin.dbo._InfoRg4020 ',@n as int=51574,@date_rasp as date=getdate()  
 if OBJECT_ID('tempdb..#_SimpleKey') is not null drop table #_SimpleKey  
  
    create table #_SimpleKey (_SimpleKey binary(16))  
 select @nvaSQL ='  
 INSERT into #_SimpleKey (_SimpleKey)   
 SELECT distinct Rg._SimpleKey   
 FROM ' +@result_table+ ' as Rg   
  INNER JOIN #a_RG a  
   ON Rg._Fld4023RRef = a.TTRef and Rg._Fld4022RRef = a.TovRef  
    and CONVERT(date, Rg._Fld4021) = '''  
    +CONVERT(varchar(8), DATEADD(YEAR, 2000, @date_rasp),112)+''''  
      
   
 while 1=1  
 begin  
  begin try  
  exec sp_executesql @nvaSQL   
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
   -- запись в лог факта блокировки  
   insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
   select @id_job , 15021, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
   select @getdate = getdate()    
  end  
  else  
  begin  
      set @err_str=isnull(ERROR_MESSAGE(),'')  
   insert into jobs..error_jobs (job_name , message , number_step , id_job)  
   select @Job_name , @err_str , 15021 , @id_job  
   -- прочая ошибка - выход    
   RAISERROR (@err_str,   
        16, -- Severity.    
        1 -- State.    
        )   
   RETURN        
   end  
  
   END CATCH   
   END--while  
        
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 151, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()         
  
  
 while 1=1  
 begin  
  begin try  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 160, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
  -- declare @N int = 59668 , @date_rasp date = {d'2018-09-01'}  
  
    
  if OBJECT_ID('tempdb..#rh_2') is not null drop table #rh_2  
    
  select rh.id_tt, rh.id_tov , SUM(rh.znach) znach  
  into #rh_2  
  from M2..raspr_hystory rh with (  index(ind1_h))  
  where rh.number_r = @N and rh.rn_r=4  
  group by rh.id_tt, rh.id_tov  
    
    
  if OBJECT_ID('tempdb..#Harac') is not null drop table #Harac  
  SELECT Hk.id_kontr, Hk.id_tov, MAX(Hk.HaracRef) as HaracRef   
  into #Harac  
  FROM M2.dbo.Har_kontr as Hk     
  where isnull(not_active,0)<>1  
  GROUP BY Hk.id_kontr, Hk.id_tov  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 161, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()    
    
  --declare @N int = 19192 , @date_rasp date = {d'2015-09-12'}   
    
  --while @N < 16946 --16887  
  --begin  
  /*теперь запишем данные в таблицу*/  
    
  --declare @N int = 19790 , @date_rasp date = {d'2015-09-01'}   
    
  if OBJECT_ID('tempdb..#1C_add') is not null drop table #1C_add  
    
  create clustered index ind1 on #rh_2 (id_tt , id_tov)  
  create clustered index ind1 on #Harac (id_tov , id_kontr)  
  
  -- declare @N int = 59668 , @date_rasp date = {d'2018-09-01'}  
    
  SELECT DATEADD(YEAR, 2000, @date_rasp) d_rasp,   
  isnull(TT.Ref, 0x00000000000000000000000000000000) tt,   
  isnull(Tov.Ref, 0x00000000000000000000000000000000) tov,   
  isnull(Harac.HaracRef, 0x00000000000000000000000000000000) harac,   
  ISNULL(Rz._Fld4908RRef, 0x00000000000000000000000000000000) Fld4908RRef,   
    
  sum(r.q_raspr) q_raspr,   
  DATEADD(YEAR, 2000, GETDATE()) date_get,   
  @N N,   
  0 a1,   
  CAST(NEWID() as BINARY(16)) id  
  , max(isnull(koef1.Koef1 * Kolvo_korob_koef1 ,0)) q_nuzno  
  , sum(r.q_plan_pr) q_plan_pr  
  ,SUM( isnull(rh.znach,0)) znach ,   
   sum(r.q_min_ost) a2  
   ,convert(binary(16),0) Fld7160RRef  
    
  into #1C_add  
  FROM M2..rasp as r with (rowlock, INDEX (ind1))  
    
  left join #rh_2 rh on rh.id_tt = r.id_tt and rh.id_tov= r.id_tov   
    
  LEFT OUTER JOIN M2.dbo.Tovari as Tov    
  ON r.id_tov = Tov.id_tov  
    
  LEFT OUTER JOIN M2.dbo.tt as TT    
  ON r.id_tt = TT.id_TT  
    
  LEFT OUTER JOIN #Harac as Harac  
  ON r.id_tov = Harac.id_tov and r.id_kontr = Harac.id_kontr  
    
  inner JOIN M2..raspr_zadanie as Rz   ON Rz.Number_r = @N  
  
  left join #koef1 koef1 on r.id_tov=koef1.id_tov and r.id_tt=koef1.id_tt  
  
  WHERE r.number_r = @N and r.q_raspr>0  
    
  group by   
  isnull(TT.Ref, 0x00000000000000000000000000000000) ,   
  isnull(Tov.Ref, 0x00000000000000000000000000000000) ,   
  isnull(Harac.HaracRef, 0x00000000000000000000000000000000) ,   
  ISNULL(Rz._Fld4908RRef,0x00000000000000000000000000000000)  
  
 -- drop table #rh_2  
 -- drop table #Harac  
  
  
  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 168, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
  
 /*сначала почистим регистр 1с-ки*/  
--declare @Nvasql as nvarchar(4000),@result_table as varchar(100)='izbenkafin.dbo._InfoRg4020 ',@n as int=51574  
  
 select @nvaSQL='DELETE ' + @result_table  +'  
  FROM ' + @result_table  +' i   
  inner join #_SimpleKey s on i._SimpleKey = s._SimpleKey '  
    
 exec sp_executesql @nvaSQL   
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 169, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
  
--SELECT *  
--FROM #1C_add  
   
 set @nvaSQL ='INSERT INTO '+@result_table+' with (rowlock)  
     ([_Fld4021]  
     ,[_Fld4023RRef]       ,[_Fld4022RRef]  
     ,[_Fld4214RRef]  
     ,[_Fld4908RRef]  
     ,[_Fld4024]  
     ,[_Fld5063]  
     ,[_Fld5086]  
     ,[_Fld5665]  
     ,[_SimpleKey]  
     ,[_Fld6339]  
     ,[_Fld6340]  
     ,[_Fld6341]  
     ,_Fld6362   
     ,_Fld7160RRef   
     ,_Fld8342)   
    SELECT *, 0 from #1C_add'   
  --select 1  
 DECLARE @cn int = 0   
  
 SET @err = 1  
  
 WHILE @err = 1 AND @cn < 2  
 BEGIN   
   BEGIN TRY   
     EXEC sp_executesql @nvaSQL  
  
    BREAK  
   END TRY  
   BEGIN CATCH  
     SET @cn = @cn + 1  
  
     IF  ERROR_NUMBER() = 2601 -- Невозможно вставить повторяющуюся ключевую строку  
        AND   
          @cn = 1   
    BEGIN  
     -- запись в лог факта блокировки  
     INSERT INTO jobs..Jobs_log ([id_job],[number_step],[duration])   
     SELECT @id_job , 1691, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
  
     SELECT @getdate = GETDATE()    
       
     INSERT INTO [M2].[dbo].[_InfoRg4020_ins_err](  
          [d_rasp],  
       [tt],  
       [tov],  
       [harac],  
       [_Fld4908RRef],  
       [q_raspr],  
       [date_get],  
       [N],  
       [id],  
       [q_nuzno],  
       [q_plan_pr],  
       [znach],  
       [a2],  
       [_Fld7160RRef]  
        )  
     SELECT    
          d_rasp,   
      tt,   
      tov,   
      harac,   
      Fld4908RRef,   
      q_raspr,   
      date_get,   
      N,   
      id,  
      q_nuzno,  
      q_plan_pr,  
      znach ,   
      a2,  
      Fld7160RRef  
     FROM #1C_add   
       -- Отправляем сообщения по списку контактов          
       EXEC [a_kor_jobs].[dbo].[Send_notification]   
                         @Msg          = 'Err:Невозм вст повт ключ строку в объект dbo._InfoRg4020(повт попытка)',   
                         @TypeContact  = 'sql',  
                            @OutgoingTypeId = 4  
    END  
    ELSE  
    BEGIN  
      SET @err=0  
  
      SET @err_str = isnull(ERROR_MESSAGE(), '')   
  
      INSERT INTO jobs..error_jobs (job_name , message , number_step , id_job)  
      SELECT @Job_name, @err_str, 1692, @id_job      
        
      -- прочая ошибка - выход    
      RAISERROR (@err_str,   
           16, -- Severity.    
           1 -- State.    
           )    
    END         
   END CATCH  
  END --while  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
 select @id_job , 170, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@@ROWCOUNT  
 select @getdate = getdate()   
  
        BREAK  
      END TRY  
      BEGIN CATCH  
        IF ERROR_NUMBER() = 1205 -- вызвала взаимоблокировку ресурсов                            
        BEGIN  
         -- запись в лог факта блокировки  
    insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
    select @id_job , 1692, DATEDIFF(MILLISECOND , @getdate ,GETDATE())  
  END  
  ELSE  
  BEGIN  
    -- прочая ошибка - отправка ошибки   
    insert into jobs..error_jobs(job_name , message , number_step , id_job)  
    select @job_name , ERROR_MESSAGE() , 1694 , @id_job  
    -- прочая ошибка - выход    
    RAISERROR (@err_str,   
         16, -- Severity.    
         1 -- State.    
         )   
    RETURN        
  END  
  END CATCH   
 end -- while  
 /* конец доработки Минеева */  
  
  if OBJECT_ID('tempdb..#_SimpleKey')is not null drop table #_SimpleKey  
  
  
    
  
  
  update M2..Raspr_zadanie   
  set date_reg_1C = GETDATE()  
  from M2..Raspr_zadanie (rowlock) r  
  where r.Number_r = @N  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 180, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
--declare @Nvasql as nvarchar(4000),@result_table as varchar(100)='[srv-sql01].izbenkafin.dbo._InfoRg4020 ',@n as int=51564  
   
 if OBJECT_ID ('tempdb..#Cur_IRg4020') is not null drop table #Cur_IRg4020   
 create table #Cur_IRg4020 (TTRef binary(16), TovRef binary(16),HarRef binary(16), q_r decimal(15,3))  
   
 select @nvaSQL ='  
 insert into #Cur_IRg4020(TTRef,TovRef,HarRef,q_r)  
 select _Fld4023RRef TTRef ,  _Fld4022RRef TovRef, _Fld4214RRef HarRef , _Fld4024 q_r   
 from ' + @result_table + ' i    
 where i._Fld5086 = ' + rtrim(@n)  
 exec sp_executesql @nvaSQL  
   
  
  select *  
  into #f  
  from (select TT._Fld758 id_tt_1C ,  Tovari._Fld760 id_tov_1C , id_kontr , i.q_r , 0 q_raspr  
    from #Cur_IRg4020 i   
     LEFT OUTER JOIN  IzbenkaFin.dbo._Reference29 AS Tovari    
      ON i.TovRef = Tovari._IDRRef   
     LEFT OUTER JOIN  IzbenkaFin.dbo._Reference42 AS TT   
      ON i.TTRef = TT._IDRRef  
     left join m2..Har_kontr hk on hk.HaracRef = i.HarRef  
      
  
    union all  
  
    SELECT id_tt , id_tov , id_kontr , 0 , q_raspr  
    FROM M2..rasp with (  index (ind1))  
    where rasp.number_r =@N  
    ) f  
  
        if OBJECT_ID ('tempdb..#Cur_IRg4020') is not null drop table #Cur_IRg4020   
  
  select @N Расчет , tt.name_TT ТТ , tov.Name_tov Товар , k.nova_kontr Произв ,   
  SUM(q_r) Колво1С, SUM(q_raspr) КолвоSQL , 1 rn  
  into #aa  
  from   
  #f a  
   inner join M2..Tovari tov on tov.id_tov = a.id_tov_1C   
   inner join M2..tt on tt.id_tt = a.id_tt_1C   
   inner join M2..kontr k on k.id_kontr = a.id_kontr  
  group by tt.name_TT , tov.Name_tov , k.nova_kontr  
  having abs (SUM(q_r) - SUM(q_raspr) )>=1  
  
  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 190, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
   
 if @test_raspr =0  
 begin  
  set @desc  = 'Расхождения между Распределением 1С и SQL ' + rtrim(@n)  
   select @email_t='##' +replace(rtrim(NEWID()) , '-' , '_')   
  
    
     
  SELECT @nvaSQL = ' Select * into ' + @email_t + ' from #aa'   
  EXEC sp_executesql @nvaSQL  
  
  --set @email= 'andy.krivenko@gmail.com;zakaz@izbenka.msk.ru ;digovtsova@mail.ru '  
  set @email=  jobs.dbo.Email_Notification_list('распределение')  
  exec a_kor_reports..send_email_results_auto -11, 0, 1, 0,0, @desc, @email, @email_t,'',0  
  
  SELECT @nvaSQL = 'if object_id(''tempdb..' + @email_t + ''') is not null   
    drop table ' + @email_t   
  EXEC sp_executesql @nvaSQL  
  
 /**  - убрал АК 01.09.18  
 insert into jobs..Jobs_log ([id_job],[number_step],[duration])   
 select @id_job , 200, DATEDIFF(MILLISECOND , @getdate ,GETDATE())   
 select @getdate = getdate()   
    
  set @desc  = 'Ошибка! Аналоги в распределении ' + rtrim(@n)  
  select @email_t='##' +replace(rtrim(NEWID()) , '-' , '_')   
  
  
     
  SELECT @nvaSQL = ' Select *  into ' + @email_t + '   
   from Reports..Err_Full_analog_in_raspr where Number_r=' + rtrim(@n)   
  EXEC sp_executesql @nvaSQL  
  
  exec Reports..send_email_results_auto -11, 0, 1, 0,0, @desc, @email, @email_t,'',0  
  
  SELECT @nvaSQL = 'if object_id(''tempdb..' + @email_t + ''') is not null   
    drop table ' + @email_t   
  EXEC sp_executesql @nvaSQL   
  **/  
    
 end --@test_raspr =0    
end  
  
  if OBJECT_ID('tempdb..#1C_add') is not null drop table #1C_add  
  if OBJECT_ID('tempdb..#f') is not null drop table #f   
  if OBJECT_ID('tempdb..#aa') is not null drop table #aa  
    
insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
select @id_job , 300, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@N  
select @getdate = getdate()   
   
 if @test_raspr=0   
 begin   
     --OD 2018-12-10 Ворошилов попросил отключить.  
  ----отправим смс об успешном выполнении на склад  
  --insert into ies..outgoing (Number,Message, AddDate,Project)  
    
  ----insert into A1_SMPP ..OutboundSMS (Number,Message,AddDate,SrcAddr )  
  --select sms.number,'Распределение '+rtrim(@N) + ' выполнилось успешно.' , GETDATE(), 'Vkusvill'   
  --from  m2..Raspr_Notification_info as sms   
  
  
  
  exec a_kor..send_email_job @id_job, @N  
  
select @getdate = getdate()    
    
    
    
    
 end  
  
   
insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
select @id_job , 301, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@N  
select @getdate = getdate()   
   
   
  
-- добавлен запуск расчета из sql для расчета товаров с нулевыми остатками  
-- для этого сравнить списко товаров в распределении с тем, что бы рассчитаны, если в распределении есть те, что нет в расчете - запустить  
  
--declare @N int = 84853  
  
  
   Declare @ТекстИДТоваров nvarchar(max)   
  
  Select   
         @ТекстИДТоваров  = [text_id_tov]  
  From M2..Raspr_zadanie as rz    
  where Rz.Number_r = @N  
  
     --declare @strТекстSQLЗапроса nvarchar(max)  
  
     create table #tov (id_tov int)  
     Set @strТекстSQLЗапроса =   
     'insert into #tov  
     select distinct tf.id_tov_2 val  
     from master..ParsingStr (''' + rtrim(@ТекстИДТоваров) + ''', '','') a  
     inner join reports..tovari_vse_poln_analogi_complect tf on tf.id_tov_1 = a.val  
     '     
     --print @strТекстSQLЗапроса  
     EXEC sp_executeSQL @strТекстSQLЗапроса      
       
       
     if exists (Select *  
     from #tov t1  
     left join m2..tov t2 on t2.Number_r = @n and t1.id_tov = t2.id_tov  
     where t2.Number_r is null  
     )   
     begin --значит есть удаленные товары  
       
     insert into [M2].[dbo].[Raspr_zadanie]  
     (  
       [Number_r]  
      ,[Date_r]  
      ,[type_rz]  
      ,[user_add]  
      ,[time_add]  
      ,[id_sklad]  
      ,[Status]  
      ,[id_zone]  
      ,[comment]  
      ,[test_raspr]  
      ,[result_table]  
      ,[text_id_tov]  
      ,[text_id_tt]  
      ,[str_vse_skladi]  
      ,[str_sled_dnem]  
      ,[str_virt_sklad_id_tt]  
      ,[type_raspr]  
      ,[sql_raspr]   
     )  
     select   
       rz1.Number_r  
      ,[Date_r]  
      ,[type_rz]  
      ,[user_add]  
      ,GETDATE()  
      ,[id_sklad]  
      ,2  
      ,[id_zone]  
      ,[comment]  
      ,[test_raspr]  
      ,[result_table]  
      ,[text_id_tov]  
      ,[text_id_tt]  
      ,[str_vse_skladi]  
      ,[str_sled_dnem]  
      ,[str_virt_sklad_id_tt]  
      ,[type_raspr]  
      ,1  
       From M2..Raspr_zadanie as rz   
       INNER JOIN  
  (select TOP 1 rz.Number_r + 1 Number_r  
   From M2..Raspr_zadanie as rz ORDER BY time_add DESC  
   ) rz1 on 1=1  
  where Rz.Number_r = @N       
     end  
end   
  
else  
  
 Update M2..Raspr_zadanie  
 set date_reg_1C = GETDATE()   
 from M2..Raspr_zadanie (rowlock) rz  
 where rz.Number_r=@N  
    
  
  
-- удалить запись из активных таблиц  
Declare @err_del int = 0  
  
while @err_del = 0  
begin  
set @err_del = 1  
begin try   
   
--declare @N int = 4842  
  
delete r  
output deleted.*  into M2..archive_rasp  
--select *  
from M2..rasp r  
where r.number_r = @N  
  
--declare @N int = 3939  
delete r  
output deleted.*  into M2..archive_tt_tov_kontr  
--select *  
from M2..tt_tov_kontr r  
where r.number_r in  (@N,-@N)  
  
end try  
begin catch  
  
Set @err_del = 0  
                 
end catch  
  
end  
  
insert into jobs..Jobs_log ([id_job],[number_step],[duration],par1)   
select @id_job , 302, DATEDIFF(MILLISECOND , @getdate ,GETDATE()) ,@N  
select @getdate = getdate()   
  
  
  
   
 -- drop table #koef1  
  
 /**  
 select *  
 from #w_all w  
 where w.id_tov = 503  
 order by id_tt  
  
 select *  
 from #ttk_tt_tov w  
 where w.id_tov = 503  
 order by id_tt  
 **/  
   
end try  
begin catch  
  
    set @err_str=isnull(ERROR_MESSAGE(),'')  
  
    if @test_raspr =0  
    BEGIN  
  exec a_kor..Notification_Raspr_err @id_job,@err_str,@N  
  
  RAISERROR (@err_str,   
       16, -- Severity.    
       1 -- State.    
       )   
          
 END   
end catch  
END
GO