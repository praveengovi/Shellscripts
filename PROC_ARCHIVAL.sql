USE [CCRISVAL]
GO
/****** Object:  StoredProcedure [dbo].[PROC_CCRIS_PART_4_ARCH_MODULES]    Script Date: 08/21/2014 20:09:22 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[PROC_CCRIS_PART_4_ARCH_MODULES] ( @BL_TABLE_NAME VARCHAR(100), @ARCHIVAL_FLAG VARCHAR(20))
AS 

BEGIN 
SET NOCOUNT ON 

DECLARE 
    @BATCH_ID VARCHAR(50),
    @BUSINESS_DT DATETIME,
    @RUN_TIMESTAMP DATETIME,
    @MP_STATUS_RUN_CNT INT,
    @NEAR_LIVE_RETENTION_PD_ST INTEGER,
    @NEAR_LIVE_RETENTION_PD_END INTEGER,
    @TAPE_RETENTION_PD INTEGER
     
    SET @NEAR_LIVE_RETENTION_PD_ST=4
    SET @NEAR_LIVE_RETENTION_PD_END=7
    SET @TAPE_RETENTION_PD=7
    select
        @BATCH_ID=BATCH_ID,
        @BUSINESS_DT=BUSINESS_DT,
        @RUN_TIMESTAMP=RUN_TIMESTAMP
    from
        CCRISVAL.dbo.CCRIS_CONFIG_TABLE 
    where 
        RUN_TIMESTAMP=(select max(RUN_TIMESTAMP) from CCRISVAL.dbo.CCRIS_CONFIG_TABLE);

        
IF  ( @BL_TABLE_NAME ='tblcreditposition' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
            
            BEGIN TRANSACTION;
                
                INSERT INTO 
                    [PFIv2].dbo.tbl_arch_creditposition
                    (
                           [fi_code]
                          ,[app_sys_code]
                          ,[master_account_no]
                          ,[sub_account_no]
                          ,[position_date]
                          ,[total_outstanding]
                          ,[month_in_arrears]
                          ,[no_of_inst_in_arrears]
                          ,[amount_undrawn]
                          ,[account_status]
                          ,[LastMaintDate]
                          ,[ExportDate]
                          ,[ErrCode]
                          ,[MaintStatus]
                          ,[Valid_Flag]
                          ,[MAkey]
                          ,[SAkey]
                          ,[CPKey]
                          ,[original_account_status]
                          ,[last_payment_date]
                          ,[subsi_leger_bal]
                          ,[write_off_date]
                          ,[MRchksum]
                          ,[loan_sold_sec_market]
                          ,[amount_disb_month]
                          ,[amount_repaid_month]
                          ,[account_status_date]
                          ,[chksum]
                          ,[int_amount_disb_month]
                          ,[int_amount_repaid_month] 
                          ,[PPN_DT]
                          ,[NewStatus]                                                    
                          ,[BATCH_ID]
                          ,[BUSINESS_DT]
                          ,[RUN_TIMESTAMP]
                    )
                    select TOP 10000
                           [fi_code]
                          ,[app_sys_code]
                          ,[master_account_no]
                          ,[sub_account_no]
                          ,[position_date]
                          ,[total_outstanding]
                          ,[month_in_arrears]
                          ,[no_of_inst_in_arrears]
                          ,[amount_undrawn]
                          ,[account_status]
                          ,[LastMaintDate]
                          ,[ExportDate]
                          ,[ErrCode]
                          ,[MaintStatus]
                          ,[Valid_Flag]
                          ,[MAkey]
                          ,[SAkey]
                          ,[CPKey]
                          ,[original_account_status]
                          ,[last_payment_date]
                          ,[subsi_leger_bal]
                          ,[write_off_date]
                          ,[MRchksum]
                          ,[loan_sold_sec_market]
                          ,[amount_disb_month]
                          ,[amount_repaid_month]
                          ,[account_status_date]
                          ,[chksum]
                          ,[int_amount_disb_month]
                          ,[int_amount_repaid_month]
                          ,[PPN_DT]
                          ,[NewStatus]                          
                          ,@BATCH_ID
                          ,@BUSINESS_DT
                          ,@RUN_TIMESTAMP
                     from  
                     [PFIv2].dbo.tblcreditposition (nolock) 
                     --where
                    --    ExportDate between (dateadd(year, -@NEAR_LIVE_RETENTION_PD_ST,@RUN_TIMESTAMP)) and (dateadd(year, -@NEAR_LIVE_RETENTION_PD_END,@RUN_TIMESTAMP))
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH



IF  ( @BL_TABLE_NAME ='tblProvision' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

        BEGIN TRY         
                
            BEGIN TRANSACTION;
                    
            INSERT INTO 
                [PFIv2].dbo.tbl_arch_Provision
                (
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[sub_account_no]
                      ,[position_date]
                      ,[classification]
                      ,[month_in_arrears]
                      ,[principal_outstanding]
                      ,[interest_outstanding]
                      ,[other_charges]
                      ,[realisable_value]
                      ,[iis_open_balance]
                      ,[iis_suspend_amount]
                      ,[iis_written_back_amount]
                      ,[iis_written_off_amount]
                      ,[iis_loan_sold_to_danaharta]
                      ,[iis_transfer_to_provision]
                      ,[sp_open_balance]
                      ,[sp_charged]
                      ,[sp_written_back_amount]
                      ,[sp_written_off_amount]
                      ,[sp_loan_sold_to_danaharta]
                      ,[sp_transfer_to_provision]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[SAkey]
                      ,[total_outstanding_PV]
                      ,[iis_reversal]
                      ,[sp_reversal]
                      ,[iis_close_bal]
                      ,[sp_close_bal]
                      ,[PVKey]
                      ,[MRchksum]
                      ,[prov_tag]
                      ,[prov_date]
                      ,[impaired_loan]
                      ,[ind_impair_provision_amount]
                      ,[impair_loan_written_back_amount]
                      ,[loan_written_off_amount]
                      ,[new_realisable_value]
                      ,[new_iis_open_balance]
                      ,[new_iis_suspend_amount]
                      ,[new_iis_written_back_amount]
                      ,[new_iis_written_off_amount]
                      ,[new_iis_loan_sold_to_danaharta]
                      ,[new_iis_transfer_to_provision]
                      ,[new_sp_open_balance]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
                )
                SELECT TOP 10000
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[sub_account_no]
                      ,[position_date]
                      ,[classification]
                      ,[month_in_arrears]
                      ,[principal_outstanding]
                      ,[interest_outstanding]
                      ,[other_charges]
                      ,[realisable_value]
                      ,[iis_open_balance]
                      ,[iis_suspend_amount]
                      ,[iis_written_back_amount]
                      ,[iis_written_off_amount]
                      ,[iis_loan_sold_to_danaharta]
                      ,[iis_transfer_to_provision]
                      ,[sp_open_balance]
                      ,[sp_charged]
                      ,[sp_written_back_amount]
                      ,[sp_written_off_amount]
                      ,[sp_loan_sold_to_danaharta]
                      ,[sp_transfer_to_provision]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[SAkey]
                      ,[total_outstanding_PV]
                      ,[iis_reversal]
                      ,[sp_reversal]
                      ,[iis_close_bal]
                      ,[sp_close_bal]
                      ,[PVKey]
                      ,[MRchksum]
                      ,[prov_tag]
                      ,[prov_date]
                      ,[impaired_loan]
                      ,[ind_impair_provision_amount]
                      ,[impair_loan_written_back_amount]
                      ,[loan_written_off_amount]
                      ,[new_realisable_value]
                      ,[new_iis_open_balance]
                      ,[new_iis_suspend_amount]
                      ,[new_iis_written_back_amount]
                      ,[new_iis_written_off_amount]
                      ,[new_iis_loan_sold_to_danaharta]
                      ,[new_iis_transfer_to_provision]
                      ,[new_sp_open_balance]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                  FROM [PFIv2].dbo.tblProvision (nolock)
                 -- where
                --  ExportDate between (dateadd(year, -@NEAR_LIVE_RETENTION_PD_ST,@RUN_TIMESTAMP)) and (dateadd(year, -@NEAR_LIVE_RETENTION_PD_END,@RUN_TIMESTAMP))        
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblsubaccount' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
                            
            BEGIN TRANSACTION;
                
            INSERT INTO [PFIv2].dbo.tbl_arch_SubAccount
                (
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[sub_account_no]
                      ,[facility_type]
                      ,[syndicated]
                      ,[special_fund_scheme]
                      ,[loanpurpose]
                      ,[financingconcept]
                      ,[original_tenure]
                      ,[principal_repay_term]
                      ,[restrusche]
                      ,[date_sold_to_cagamas]
                      ,[sold_to_cagamas]
                      ,[date_sold_to_danaharta]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[SAkey]
                      ,[FMSGL]
                      ,[prod_cd]
                      ,[dept_cd]
                      ,[sector_cd]
                      ,[business_activity_cd]
                      ,[smi_indicator]
                      ,[card_type]
                      ,[loan_utilisation_state]
                      ,[remaining_maturity]
                      ,[maturity_date]
                      ,[interest_rate_info]
                      ,[MRchksum]
                      ,[disbursed_curr]
                      ,[loan_currency]
                      ,[interest_rate]
                      ,[rebate_rate]
                      ,[asset_purchase_value]
                      ,[dfi_finan_detail]
                      ,[pricing_type]
                      ,[maturitydate]
                      ,[first_disb_date]
                      ,[loan_utilised]
                      ,[priority_sector]
                      ,[loan_util_location]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
                )

SELECT
                     a.[fi_code]
                    ,a.[app_sys_code]
                    ,a.[master_account_no]
                    ,a.[sub_account_no]
                    ,a.[facility_type]
                    ,a.[syndicated]
                    ,a.[special_fund_scheme]
                    ,a.[loanpurpose]
                    ,a.[financingconcept]
                    ,a.[original_tenure]
                    ,a.[principal_repay_term]
                    ,a.[restrusche]
                    ,a.[date_sold_to_cagamas]
                    ,a.[sold_to_cagamas]
                    ,a.[date_sold_to_danaharta]
                    ,a.[LastMaintDate]
                    ,a.[ExportDate]
                    ,a.[ErrCode]
                    ,a.[MaintStatus]
                    ,a.[Valid_Flag]
                    ,a.[MAkey]
                    ,a.[SAkey]
                    ,a.[FMSGL]
                    ,a.[prod_cd]
                    ,a.[dept_cd]
                    ,a.[sector_cd]
                    ,a.[business_activity_cd]
                    ,a.[smi_indicator]
                    ,a.[card_type]
                    ,a.[loan_utilisation_state]
                    ,a.[remaining_maturity]
                    ,a.[maturity_date]
                    ,a.[interest_rate_info]
                    ,a.[MRchksum]
                    ,a.[disbursed_curr]
                    ,a.[loan_currency]
                    ,a.[interest_rate]
                    ,a.[rebate_rate]
                    ,a.[asset_purchase_value]
                    ,a.[dfi_finan_detail]
                    ,a.[pricing_type]
                    ,a.[maturitydate]
                    ,a.[first_disb_date]
                    ,a.[loan_utilised]
                    ,a.[priority_sector]
                    ,a.[loan_util_location]
                    ,a.[chksum]
                    ,a.[PPN_DT]
                    ,a.[NewStatus]                    
                    ,@BATCH_ID
                    ,@BUSINESS_DT
                    ,@RUN_TIMESTAMP
from  
    [PFIv2].dbo.tblsubaccount (nolock) a,
(
(

            select                     
                        sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]
                    ,count(*) AS DUMMY
            from  
                    [PFIv2].dbo.tblsubaccount (nolock) sb_acc,
                    [PFIv2].dbo.tbl_arch_Provision (nolock) prov
            where 
                         sb_acc.[fi_code]=prov.[fi_code]
                     and sb_acc.[app_sys_code]=prov.[app_sys_code]
                     and sb_acc.[master_account_no]=prov.[master_account_no]                     
                     and prov.BATCH_ID=@BATCH_ID
                     and prov.BUSINESS_DT=@BUSINESS_DT
                     and prov.RUN_TIMESTAMP=@RUN_TIMESTAMP
            group by 
                     sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]
            Having count(*)=1
EXCEPT            
            select                     
                        sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]                    
                    ,1 AS DUMMY
            from  
                    [PFIv2].dbo.tblsubaccount (nolock) sb_acc,
                    [PFIv2].dbo.tbl_arch_creditposition (nolock) cp
            where 
                         sb_acc.[fi_code]=cp.[fi_code]
                     and sb_acc.[app_sys_code]=cp.[app_sys_code]
                     and sb_acc.[master_account_no]=cp.[master_account_no]
                     and sb_acc.[sub_account_no]=cp.[sub_account_no]
                     and cp.BATCH_ID=@BATCH_ID
                     and cp.BUSINESS_DT=@BUSINESS_DT
                     and cp.RUN_TIMESTAMP=@RUN_TIMESTAMP
)
    UNION 
(

            select                     
                        sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]                    
                    ,1 AS DUMMY
            from  
                    [PFIv2].dbo.tblsubaccount (nolock) sb_acc,
                    [PFIv2].dbo.tbl_arch_creditposition (nolock) cp
            where 
                         sb_acc.[fi_code]=cp.[fi_code]
                     and sb_acc.[app_sys_code]=cp.[app_sys_code]
                     and sb_acc.[master_account_no]=cp.[master_account_no]
                     and sb_acc.[sub_account_no]=cp.[sub_account_no]
                     and cp.BATCH_ID=@BATCH_ID
                     and cp.BUSINESS_DT=@BUSINESS_DT
                     and cp.RUN_TIMESTAMP=@RUN_TIMESTAMP
                     
EXCEPT            

select                     
                        sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]
                    ,count(*) AS DUMMY
            from  
                    [PFIv2].dbo.tblsubaccount (nolock) sb_acc,
                    [PFIv2].dbo.tbl_arch_Provision (nolock) prov
            where 
                         sb_acc.[fi_code]=prov.[fi_code]
                     and sb_acc.[app_sys_code]=prov.[app_sys_code]
                     and sb_acc.[master_account_no]=prov.[master_account_no]                     
                     and prov.BATCH_ID=@BATCH_ID
                     and prov.BUSINESS_DT=@BUSINESS_DT
                     and prov.RUN_TIMESTAMP=@RUN_TIMESTAMP
            group by 
                     sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]
            Having count(*)=1
) 
) SDID6_SUB_ACCT
WHERE 
            a.[fi_code]=SDID6_SUB_ACCT.[fi_code]
        and a.[app_sys_code]=SDID6_SUB_ACCT.[app_sys_code]
        and a.[master_account_no]=SDID6_SUB_ACCT.[master_account_no]
        and a.[sub_account_no]=SDID6_SUB_ACCT.[sub_account_no]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
IF  ( @BL_TABLE_NAME ='tblMasterAccount' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
                
            BEGIN TRANSACTION;
                    
INSERT INTO [PFIv2].dbo.tbl_arch_MasterAccount 
                (
                                                   [fi_code]
                                                  ,[app_sys_code]
                                                  ,[master_account_no]
                                                  ,[appr_limit_curr]
                                                  ,[appr_limit_actual_curr]
                                                  ,[appr_limit_rm]
                                                  ,[appr_date]
                                                  ,[LastMaintDate]
                                                  ,[ExportDate]
                                                  ,[ErrCode]
                                                  ,[MaintStatus]
                                                  ,[Valid_Flag]
                                                  ,[MAkey]
                                                  ,[relationship_status]
                                                  ,[chksum]
                                                  ,[PPN_DT]
                                                  ,[NewStatus]                                                  
                                                  ,[BATCH_ID]
                                                  ,[BUSINESS_DT]
                                                  ,[RUN_TIMESTAMP]

                )

                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[appr_limit_curr]
                      ,a.[appr_limit_actual_curr]
                      ,a.[appr_limit_rm]
                      ,a.[appr_date]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[MAkey]
                      ,a.[relationship_status]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP                                            
                from  
                    [PFIv2].dbo.tblMasterAccount (nolock) a,
                (
					select fi_code,app_sys_code,master_account_no 
					from   
					[PFIv2].dbo.tblmasteraccount 
					EXCEPT
							 (
							 select 
								 SUB_ACCT.fi_code,
								 SUB_ACCT.app_sys_code,
								 SUB_ACCT.master_account_no
							 from 
								[PFIv2].dbo.tblsubaccount  SUB_ACCT
								LEFT OUTER JOIN 
								( select fi_code,app_sys_code,master_account_no,sub_account_no,1 AS AVL_FLG from [PFIv2].dbo.tbl_arch_creditposition where  BATCH_ID=@BATCH_ID and BUSINESS_DT=@BUSINESS_DT and RUN_TIMESTAMP=@RUN_TIMESTAMP) ARCH_CP
								ON 
								(SUB_ACCT.fi_code=ARCH_CP.fi_code AND SUB_ACCT.app_sys_code=ARCH_CP.app_sys_code AND SUB_ACCT.master_account_no=ARCH_CP.master_account_no AND SUB_ACCT.sub_account_no=ARCH_CP.sub_account_no)                        
							WHERE 
								ARCH_CP.AVL_FLG is null
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]
                        
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
                        
IF  ( @BL_TABLE_NAME ='tblAccountHolder' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
INSERT INTO [PFIv2].dbo.tbl_arch_AccountHolder 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[type]
                      ,[custno]
                      ,[name_sort_key]
                      ,[custname]
                      ,[entity_type]
                      ,[id_no1]
                      ,[id_no2]
                      ,[birthdate]
                      ,[idic_fi_code]
                      ,[idic_app_sys_code]
                      ,[MaintStatus]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MAkey]
                      ,[AHPKchksum]
                      ,[cust_cd]
                      ,[group_borrower_id]
                      ,[group_borrower_name]
                      ,[MRchksum]
                      ,[chksum]
                      ,[IDICkey]
                      ,[AHKey]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)

                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[type]
                      ,a.[custno]
                      ,a.[name_sort_key]
                      ,a.[custname]
                      ,a.[entity_type]
                      ,a.[id_no1]
                      ,a.[id_no2]
                      ,a.[birthdate]
                      ,a.[idic_fi_code]
                      ,a.[idic_app_sys_code]
                      ,a.[MaintStatus]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MAkey]
                      ,a.[AHPKchksum]
                      ,a.[cust_cd]
                      ,a.[group_borrower_id]
                      ,a.[group_borrower_name]
                      ,a.[MRchksum]
                      ,a.[chksum]
                      ,a.[IDICkey]
                      ,a.[AHKey]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblAccountHolder (nolock) a,
                (
                    select 
                         Acct_Hld.[fi_code]
                        ,Acct_Hld.[app_sys_code]
                        ,Acct_Hld.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountHolder (nolock) Acct_Hld
                    where 
                            Mast_Acct.[fi_code]=Acct_Hld.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Hld.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Hld.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Hld.[fi_code]
                        ,Acct_Hld.[app_sys_code]
                        ,Acct_Hld.[master_account_no]
                     having count(*)=1
                     
                     except 
    (             
                     select 
                         Leg_Act.[fi_code]
                        ,Leg_Act.[app_sys_code]
                        ,Leg_Act.[master_account_no]
                        ,1 as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblLegalAction (nolock) Leg_Act
                    where 
                            Mast_Acct.[fi_code]=Leg_Act.[fi_code]
                        and Mast_Acct.[app_sys_code]=Leg_Act.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Leg_Act.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                   
                   except 
                   
                   select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
            )
            
            except             
                (    
                
                   select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
                            
                except                                              
                
                     select 
                         Leg_Act.[fi_code]
                        ,Leg_Act.[app_sys_code]
                        ,Leg_Act.[master_account_no]
                        ,1 as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblLegalAction (nolock) Leg_Act
                    where 
                            Mast_Acct.[fi_code]=Leg_Act.[fi_code]
                        and Mast_Acct.[app_sys_code]=Leg_Act.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Leg_Act.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP

            )
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]            
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblLegalAction' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
            
            BEGIN TRANSACTION;

                INSERT INTO [PFIv2].dbo.tbl_arch_LegalAction 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[legal_action]
                      ,[date_of_legal_status]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[LAKey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)


                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[legal_action]
                      ,a.[date_of_legal_status]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[MAkey]
                      ,a.[LAKey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblLegalAction (nolock) a,
                (
                
                    select 
                         Leg_Act.[fi_code]
                        ,Leg_Act.[app_sys_code]
                        ,Leg_Act.[master_account_no]
                        ,1 as DUMMY
                    from [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblLegalAction (nolock) Leg_Act 
                    where 
                            Mast_Acct.[fi_code]=Leg_Act.[fi_code]
                        and Mast_Acct.[app_sys_code]=Leg_Act.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Leg_Act.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP

                    EXCEPT 
(

                    select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
                    
                    except 
                                            
                    select 
                         Acct_Hld.[fi_code]
                        ,Acct_Hld.[app_sys_code]
                        ,Acct_Hld.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountHolder (nolock) Acct_Hld
                    where 
                            Mast_Acct.[fi_code]=Acct_Hld.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Hld.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Hld.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Hld.[fi_code]
                        ,Acct_Hld.[app_sys_code]
                        ,Acct_Hld.[master_account_no]
                     having count(*)=1
)                     

except 

(

                    select 
                         Acct_Hld.[fi_code]
                        ,Acct_Hld.[app_sys_code]
                        ,Acct_Hld.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountHolder (nolock) Acct_Hld
                    where 
                            Mast_Acct.[fi_code]=Acct_Hld.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Hld.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Hld.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Hld.[fi_code]
                        ,Acct_Hld.[app_sys_code]
                        ,Acct_Hld.[master_account_no]
                     having count(*)=1
                    
                    except 
                                                         
                     select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
)                     

                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]            
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
IF  ( @BL_TABLE_NAME ='tblAccountCollateral' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
                            
            BEGIN TRANSACTION;
                    
                INSERT INTO [PFIv2].dbo.tbl_arch_AccountCollateral 
(
                   [fi_code]
                  ,[app_sys_code]
                  ,[master_account_no]
                  ,[sub_account_no]
                  ,[collateralrefno]
                  ,[col_type]
                  ,[col_value]
                  ,[col_fi_code]
                  ,[col_app_sys_code]
                  ,[MaintStatus]
                  ,[LastMaintDate]
                  ,[ExportDate]
                  ,[ErrCode]
                  ,[MAkey]
                  ,[SAkey]
                  ,[ACkey]
                  ,[ACKey2]
                  ,[col_key]
                  ,[rel_indc]
                  ,[financing_refinancing]
                  ,[auction_date]
                  ,[auction_price]
                  ,[purchase_date]
                  ,[purchase_price]
                  ,[vehicle_type]
                  ,[vehicle_year_of_financing]
                  ,[repo_tag]
                  ,[repo_date]
                  ,[repo_reserve_val]
                  ,[ACPKchksum]
                  ,[chksum]
                  ,[MRchksum]
                  ,[PPN_DT]
                  ,[NewStatus]                  
                  ,[BATCH_ID]
                  ,[BUSINESS_DT]
                  ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[sub_account_no]
                      ,a.[collateralrefno]
                      ,a.[col_type]
                      ,a.[col_value]
                      ,a.[col_fi_code]
                      ,a.[col_app_sys_code]
                      ,a.[MaintStatus]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MAkey]
                      ,a.[SAkey]
                      ,a.[ACkey]
                      ,a.[ACKey2]
                      ,a.[col_key]
                      ,a.[rel_indc]
                      ,a.[financing_refinancing]
                      ,a.[auction_date]
                      ,a.[auction_price]
                      ,a.[purchase_date]
                      ,a.[purchase_price]
                      ,a.[vehicle_type]
                      ,a.[vehicle_year_of_financing]
                      ,a.[repo_tag]
                      ,a.[repo_date]
                      ,a.[repo_reserve_val]
                      ,a.[ACPKchksum]
                      ,a.[chksum]
                      ,a.[MRchksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblAccountCollateral (nolock) a,
                (
                   select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
                     
                     EXCEPT 
                     
                     
(
                select 
                         Leg_Act.[fi_code]
                        ,Leg_Act.[app_sys_code]
                        ,Leg_Act.[master_account_no]
                        ,1 as DUMMY
                    from [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblLegalAction (nolock) Leg_Act 
                    where 
                            Mast_Acct.[fi_code]=Leg_Act.[fi_code]
                        and Mast_Acct.[app_sys_code]=Leg_Act.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Leg_Act.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                  EXCEPT 
                  
                      select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
                  
)
                     
EXCEPT 

(

              
                      select 
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                        ,count(*) as DUMMY
                    from 
                         [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblAccountCollateral (nolock) Acct_Coll
                    where 
                            Mast_Acct.[fi_code]=Acct_Coll.[fi_code]
                        and Mast_Acct.[app_sys_code]=Acct_Coll.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Acct_Coll.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                     group by
                         Acct_Coll.[fi_code]
                        ,Acct_Coll.[app_sys_code]
                        ,Acct_Coll.[master_account_no]
                     having count(*)=1
              EXCEPT                  

                   select 
                         Leg_Act.[fi_code]
                        ,Leg_Act.[app_sys_code]
                        ,Leg_Act.[master_account_no]
                        ,1 as DUMMY
                    from [PFIv2].dbo.tbl_arch_MasterAccount (nolock) Mast_Acct,
                         [PFIv2].dbo.tblLegalAction (nolock) Leg_Act 
                    where 
                            Mast_Acct.[fi_code]=Leg_Act.[fi_code]
                        and Mast_Acct.[app_sys_code]=Leg_Act.[app_sys_code]
                        and Mast_Acct.[master_account_no]=Leg_Act.[master_account_no]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                        
)
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]
            
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCorporation' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
                
            BEGIN TRANSACTION;
                    
                    INSERT INTO [PFIv2].dbo.tbl_arch_Corporation 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[custno]
                      ,[name_sort_key]
                      ,[custname]
                      ,[reg_no]
                      ,[date_of_reg]
                      ,[country_of_reg]
                      ,[type]
                      ,[rcc_nrcc]
                      ,[entity_type]
                      ,[resident_status]
                      ,[cust_maint_date]
                      ,[addr1]
                      ,[addr2]
                      ,[addr3]
                      ,[addr4]
                      ,[last_addr_maint_date]
                      ,[country_code]
                      ,[post_code]
                      ,[city_town]
                      ,[state_code]
                      ,[phone_1]
                      ,[phone_2]
                      ,[phone_3]
                      ,[phone_4]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[OWkey]
                      ,[cust_cd]
                      ,[group_borrower_id]
                      ,[group_borrower_name]
                      ,[corporate_status]
                      ,[industrial_sector]
                      ,[member_of_bank_rakyat]
                      ,[IDICkey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[custno]
                      ,a.[name_sort_key]
                      ,a.[custname]
                      ,a.[reg_no]
                      ,a.[date_of_reg]
                      ,a.[country_of_reg]
                      ,a.[type]
                      ,a.[rcc_nrcc]
                      ,a.[entity_type]
                      ,a.[resident_status]
                      ,a.[cust_maint_date]
                      ,a.[addr1]
                      ,a.[addr2]
                      ,a.[addr3]
                      ,a.[addr4]
                      ,a.[last_addr_maint_date]
                      ,a.[country_code]
                      ,a.[post_code]
                      ,a.[city_town]
                      ,a.[state_code]
                      ,a.[phone_1]
                      ,a.[phone_2]
                      ,a.[phone_3]
                      ,a.[phone_4]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[OWkey]
                      ,a.[cust_cd]
                      ,a.[group_borrower_id]
                      ,a.[group_borrower_name]
                      ,a.[corporate_status]
                      ,a.[industrial_sector]
                      ,a.[member_of_bank_rakyat]
                      ,a.[IDICkey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCorporation (nolock) a,
                (
                        select      
                                    corp_ach.[fi_code]
                                   ,corp_ach.[app_sys_code]
                                   ,corp_ach.[custno]
                                   ,corp_ach.[custname]
                                   ,corp_ach.[reg_no]
                                   ,count(*) AS DUMMY
                        from
                        (                
                        select
                                    a.[fi_code]
                                   ,a.[app_sys_code]
                                   ,a.[custno]
                                   ,a.[custname]
                                   ,a.[reg_no]
                        from
                        [PFIv2].dbo.tblCorporation (nolock) a,
                        (
                                            select 
                                                 [fi_code]
                                                ,[app_sys_code]
                                                ,[custno]
                                                ,[custname]
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountHolder (nolock) 
                                            where 
                                              BATCH_ID=@BATCH_ID
                                              and BUSINESS_DT=@BUSINESS_DT
                                              and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                              and entity_type <> '11'
                        ) b
                        where 
                                                    a.[fi_code]=b.[fi_code]
                                                and a.[app_sys_code]=b.[app_sys_code]
                                                and a.[custno]=b.[custno]
                                                and a.[custname]=b.[custname]
                        ) corp_ach,
                        [PFIv2].dbo.tblOwner (nolock) OWN
                        where 
                                                 corp_ach.[fi_code]=OWN.[fi_code]
                                                AND corp_ach.[app_sys_code]=OWN.[app_sys_code]
                                                AND corp_ach.[custno]=OWN.[custno]
                                                AND corp_ach.[custname]=OWN.[custname]
                                                AND corp_ach.[reg_no]=OWN.[reg_no]
                        group by 
                                    corp_ach.[fi_code]
                                   ,corp_ach.[app_sys_code]
                                   ,corp_ach.[custno]
                                   ,corp_ach.[custname]
                                   ,corp_ach.[reg_no]
                        having count(*)=1
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[custno]=b.[custno]
                        and a.[custname]=b.[custname]
                    
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblOwner' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
                
            BEGIN TRANSACTION;
                    
INSERT INTO [PFIv2].dbo.tbl_arch_Owner 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[custno]
                      ,[custname]
                      ,[reg_no]
                      ,[name_sort_key]
                      ,[owner_name_sort_key]
                      ,[ownername]
                      ,[owner_id_no1]
                      ,[owner_id_no2]
                      ,[owner_last_maint_date]
                      ,[MaintStatus]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[chksum]
                      ,[OWkey]
                      ,[OWKey2]
                      ,[OWPKchksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
               SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[custno]
                      ,a.[custname]
                      ,a.[reg_no]
                      ,a.[name_sort_key]
                      ,a.[owner_name_sort_key]
                      ,a.[ownername]
                      ,a.[owner_id_no1]
                      ,a.[owner_id_no2]
                      ,a.[owner_last_maint_date]
                      ,a.[MaintStatus]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[chksum]
                      ,a.[OWkey]
                      ,a.[OWKey2]
                      ,a.[OWPKchksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblOwner (nolock) a,
                (
                    select 
                         CORP.[fi_code]
                        ,CORP.[app_sys_code]
                        ,CORP.[custno]
                        ,CORP.[custname]
                        ,CORP.[reg_no]
                        ,COUNT(*) AS DUMMY
                    from 
                        [PFIv2].dbo.tbl_arch_Corporation (nolock) CORP,
                        [PFIv2].dbo.tblOwner (nolock) OWN
                    where 
                          CORP.[fi_code]=OWN.[fi_code]
                      and CORP.[app_sys_code]=OWN.[app_sys_code]
                      and CORP.[custno]=OWN.[custno]
                      and CORP.[custname]=OWN.[custname]
                      and CORP.[reg_no]=OWN.[reg_no]                          
                      AND BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      GROUP BY 
                         CORP.[fi_code]
                        ,CORP.[app_sys_code]
                        ,CORP.[custno]                        
                        ,CORP.[custname]                    
                        ,CORP.[reg_no]
                     HAVING COUNT(*)=1    
                ) b
                where 
                          a.[fi_code]=b.[fi_code]
                      and a.[app_sys_code]=b.[app_sys_code]
                      and a.[custno]=b.[custno]
                      and a.[custname]=b.[custname]
                      and a.[reg_no]=b.[reg_no]            
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblIndividual' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
                INSERT INTO [PFIv2].dbo.tbl_arch_Individual
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[custno]
                      ,[name_sort_key]
                      ,[custname]
                      ,[id_no1]
                      ,[id_no2]
                      ,[birth_date]
                      ,[gender]
                      ,[nationality]
                      ,[marital_status]
                      ,[bumi_code]
                      ,[entity_type]
                      ,[resident_status]
                      ,[cust_maint_date]
                      ,[addr1]
                      ,[addr2]
                      ,[addr3]
                      ,[addr4]
                      ,[last_addr_maint_date]
                      ,[country_code]
                      ,[post_code]
                      ,[city_town]
                      ,[state_code]
                      ,[phone_1]
                      ,[phone_2]
                      ,[phone_3]
                      ,[phone_4]
                      ,[employer_name]
                      ,[occupation]
                      ,[last_empdet_maint_date]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[cust_cd]
                      ,[group_borrower_id]
                      ,[group_borrower_name]
                      ,[member_of_bank_rakyat]
                      ,[IDICkey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
               SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[custno]
                      ,a.[name_sort_key]
                      ,a.[custname]
                      ,a.[id_no1]
                      ,a.[id_no2]
                      ,a.[birth_date]
                      ,a.[gender]
                      ,a.[nationality]
                      ,a.[marital_status]
                      ,a.[bumi_code]
                      ,a.[entity_type]
                      ,a.[resident_status]
                      ,a.[cust_maint_date]
                      ,a.[addr1]
                      ,a.[addr2]
                      ,a.[addr3]
                      ,a.[addr4]
                      ,a.[last_addr_maint_date]
                      ,a.[country_code]
                      ,a.[post_code]
                      ,a.[city_town]
                      ,a.[state_code]
                      ,a.[phone_1]
                      ,a.[phone_2]
                      ,a.[phone_3]
                      ,a.[phone_4]
                      ,a.[employer_name]
                      ,a.[occupation]
                      ,a.[last_empdet_maint_date]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[cust_cd]
                      ,a.[group_borrower_id]
                      ,a.[group_borrower_name]
                      ,a.[member_of_bank_rakyat]
                      ,a.[IDICkey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblIndividual (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[custno]
                        ,[custname]
                        ,[entity_type]
                        ,[id_no1]
                        ,[birthdate]
                    from 
                        [PFIv2].dbo.tbl_arch_AccountHolder (nolock) 
                    where 
                            BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                        and entity_type='11'
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[custno]=b.[custno]
                        and a.[custname]=b.[custname]
                        and a.[entity_type]=b.[entity_type]
                        and a.[id_no1]=b.[id_no1]
                        and a.[birth_date]=b.[birthdate] 
                    
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCProperty' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
                INSERT INTO [PFIv2].dbo.tbl_arch_CProperty
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[collateralrefno]
                      ,[InstrumentOfClaim]
                      ,[RankOfCharge]
                      ,[Shared]
                      ,[LandUse]
                      ,[PropertyDesc]
                      ,[PropertyLocation]
                      ,[ForcedSaleValue]
                      ,[ReservedValue]
                      ,[ValuationDate]
                      ,[Valuer]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[ACkey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
               SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[collateralrefno]
                      ,a.[InstrumentOfClaim]
                      ,a.[RankOfCharge]
                      ,a.[Shared]
                      ,a.[LandUse]
                      ,a.[PropertyDesc]
                      ,a.[PropertyLocation]
                      ,a.[ForcedSaleValue]
                      ,a.[ReservedValue]
                      ,a.[ValuationDate]
                      ,a.[Valuer]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[ACkey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCProperty (nolock) a,
                (
                    select 
                         ACCT_COL.[fi_code]
                        ,ACCT_COL.[app_sys_code]
                        ,ACCT_COL.[collateralrefno]
                        ,count(*) as DUMMY
                    from 
                        [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                        [PFIv2].dbo.tblCProperty (nolock) TBL_CPRO
                    where 
                            ACCT_COL.[fi_code]=TBL_CPRO.[fi_code]
                        and ACCT_COL.[app_sys_code]=TBL_CPRO.[app_sys_code]
                        and ACCT_COL.[collateralrefno]=TBL_CPRO.[collateralrefno]    
                        and BATCH_ID=@BATCH_ID
                        and BUSINESS_DT=@BUSINESS_DT
                        and RUN_TIMESTAMP=@RUN_TIMESTAMP
                        and col_type=10 
                     GROUP BY 
                          ACCT_COL.[fi_code]
                        ,ACCT_COL.[app_sys_code]
                        ,ACCT_COL.[collateralrefno]
                      HAVING COUNT(*)=1
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                                        
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCMotor' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
            BEGIN TRANSACTION;
            
            
            INSERT INTO [PFIv2].dbo.tbl_arch_CMotor
(
                                           [fi_code]
                                          ,[app_sys_code]
                                          ,[collateralrefno]
                                          ,[InstrumentOfClaim]
                                          ,[Model]
                                          ,[EngineNo]
                                          ,[ChassisNo]
                                          ,[VehicleNo]
                                          ,[LastMaintDate]
                                          ,[ExportDate]
                                          ,[ErrCode]
                                          ,[MaintStatus]
                                          ,[Valid_Flag]
                                          ,[ACkey]
                                          ,[chksum]
                                          ,[PPN_DT]
                                          ,[NewStatus]                                          
                                          ,[BATCH_ID]
                                          ,[BUSINESS_DT]
                                          ,[RUN_TIMESTAMP]
)
                                select 
                                           a.[fi_code]
                                          ,a.[app_sys_code]
                                          ,a.[collateralrefno]
                                          ,a.[InstrumentOfClaim]
                                          ,a.[Model]
                                          ,a.[EngineNo]
                                          ,a.[ChassisNo]
                                          ,a.[VehicleNo]
                                          ,a.[LastMaintDate]
                                          ,a.[ExportDate]
                                          ,a.[ErrCode]
                                          ,a.[MaintStatus]
                                          ,a.[Valid_Flag]
                                          ,a.[ACkey]
                                          ,a.[chksum]
                                          ,a.[PPN_DT]
                                          ,a.[NewStatus]                                          
                                          ,@BATCH_ID
                                          ,@BUSINESS_DT
                                          ,@RUN_TIMESTAMP
                                from  
                                    [PFIv2].dbo.tblCMotor (nolock) a,
                                        (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCMotor (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=30 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                                        ) b    
                                where 
                                            a.[fi_code]=b.[fi_code]
                                        and a.[app_sys_code]=b.[app_sys_code]
                                        and a.[collateralrefno]=b.[collateralrefno]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
                    
IF  ( @BL_TABLE_NAME ='tblCCarrier' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 
            BEGIN TRANSACTION;
            
            INSERT INTO [PFIv2].dbo.tbl_arch_CCarrier
(
                               [fi_code]
                              ,[app_sys_code]
                              ,[collateralrefno]
                              ,[InstrumentOfClaim]
                              ,[ColDesc]
                              ,[ValuationDate]
                              ,[LastMaintDate]
                              ,[ExportDate]
                              ,[ErrCode]
                              ,[MaintStatus]
                              ,[Valid_Flag]
                              ,[ACkey]
                              ,[chksum]
                              ,[PPN_DT]
                              ,[NewStatus]                              
                              ,[BATCH_ID]
                              ,[BUSINESS_DT]
                              ,[RUN_TIMESTAMP]
)
                SELECT 
                                  a.[fi_code]
                              ,a.[app_sys_code]
                              ,a.[collateralrefno]
                              ,a.[InstrumentOfClaim]
                              ,a.[ColDesc]
                              ,a.[ValuationDate]
                              ,a.[LastMaintDate]
                              ,a.[ExportDate]
                              ,a.[ErrCode]
                              ,a.[MaintStatus]
                              ,a.[Valid_Flag]
                              ,a.[ACkey]
                              ,a.[chksum]
                              ,a.[PPN_DT]
                              ,a.[NewStatus]                              
                              ,@BATCH_ID
                              ,@BUSINESS_DT
                              ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCCarrier (nolock) a,
                    (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCCarrier (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=40 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                    ) b    
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
            
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCPlant' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                
                INSERT INTO [PFIv2].dbo.tbl_arch_CPlant
(
                               [fi_code]
                              ,[app_sys_code]
                              ,[collateralrefno]
                              ,[InstrumentOfClaim]
                              ,[ColDesc]
                              ,[ValuationDate]
                              ,[LastMaintDate]
                              ,[ExportDate]
                              ,[ErrCode]
                              ,[MaintStatus]
                              ,[Valid_Flag]
                              ,[ACkey]
                              ,[chksum]
                              ,[PPN_DT]
                              ,[NewStatus]                              
                              ,[BATCH_ID]
                              ,[BUSINESS_DT]
                              ,[RUN_TIMESTAMP]
)
                SELECT 
                               a.[fi_code]
                              ,a.[app_sys_code]
                              ,a.[collateralrefno]
                              ,a.[InstrumentOfClaim]
                              ,a.[ColDesc]
                              ,a.[ValuationDate]
                              ,a.[LastMaintDate]
                              ,a.[ExportDate]
                              ,a.[ErrCode]
                              ,a.[MaintStatus]
                              ,a.[Valid_Flag]
                              ,a.[ACkey]
                              ,a.[chksum]
                              ,a.[PPN_DT]
                              ,a.[NewStatus]                              
                              ,@BATCH_ID
                              ,@BUSINESS_DT
                              ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCPlant (nolock) a,
                                        (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCPlant (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=60 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                                        ) b    
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCConcession' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
                INSERT INTO [PFIv2].dbo.tbl_arch_Cconcession

(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[ColDesc]
                                      ,[ValuationDate]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)

                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[ColDesc]
                                      ,a.[ValuationDate]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCConcession (nolock) a,
                                        (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCConcession (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=50 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                                        ) b    
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCAssets' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
            
            INSERT INTO [PFIv2].dbo.tbl_arch_CAssets
(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[ColDesc]
                                      ,[ValuationDate]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[ColDesc]
                                      ,a.[ValuationDate]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCAssets (nolock) a,
                    (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCAssets (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=29 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                    ) b    
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCOtherAssets' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
            
            INSERT INTO [PFIv2].dbo.tbl_arch_COtherAssets
(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[ColDesc]
                                      ,[ValuationDate]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[ColDesc]
                                      ,a.[ValuationDate]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                          
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCOtherAssets (nolock) a,
                    (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCProperty (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=90 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                ) b    
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
            
IF  ( @BL_TABLE_NAME ='tblCGuarantees' AND @ARCHIVAL_FLAG='NEAR-LIVE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
            
            INSERT INTO [PFIv2].dbo.tbl_arch_Cguarantees
(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[Nature]
                                      ,[GuarantorName]
                                      ,[GuarantorIDNo]
                                      ,[CountryOfOrigin]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[guarantee_fee_rate]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                                                            
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[Nature]
                                      ,a.[GuarantorName]
                                      ,a.[GuarantorIDNo]
                                      ,a.[CountryOfOrigin]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[guarantee_fee_rate]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCGuarantees (nolock) a,
                                        (
                                            select 
                                                 ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                                ,count(*) as DUMMY
                                            from 
                                                [PFIv2].dbo.tbl_arch_AccountCollateral (nolock) ACCT_COL,
                                                [PFIv2].dbo.tblCGuarantees (nolock) TBL_Ctbl
                                            where 
                                                    ACCT_COL.[fi_code]=TBL_Ctbl.[fi_code]
                                                and ACCT_COL.[app_sys_code]=TBL_Ctbl.[app_sys_code]
                                                and ACCT_COL.[collateralrefno]=TBL_Ctbl.[collateralrefno]    
                                                and BATCH_ID=@BATCH_ID
                                                and BUSINESS_DT=@BUSINESS_DT
                                                and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                                and col_type=70 
                                             GROUP BY 
                                                  ACCT_COL.[fi_code]
                                                ,ACCT_COL.[app_sys_code]
                                                ,ACCT_COL.[collateralrefno]
                                              HAVING COUNT(*)=1
                                        ) b    
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH

--------------------------------------------------------------------------------------------------------            
--------------------------------------------------------------------------------------------------------
                       -- TAPE ---
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

IF ( @BL_TABLE_NAME ='tblcreditposition' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
            
            BEGIN TRANSACTION;
                
                INSERT INTO 
                    [CCRISARCH].[dbo].tbl_arch_tape_CreditPosition
                    (
                           [fi_code]
                          ,[app_sys_code]
                          ,[master_account_no]
                          ,[sub_account_no]
                          ,[position_date]
                          ,[total_outstanding]
                          ,[month_in_arrears]
                          ,[no_of_inst_in_arrears]
                          ,[amount_undrawn]
                          ,[account_status]
                          ,[LastMaintDate]
                          ,[ExportDate]
                          ,[ErrCode]
                          ,[MaintStatus]
                          ,[Valid_Flag]
                          ,[MAkey]
                          ,[SAkey]
                          ,[CPKey]
                          ,[original_account_status]
                          ,[last_payment_date]
                          ,[subsi_leger_bal]
                          ,[write_off_date]
                          ,[MRchksum]
                          ,[loan_sold_sec_market]
                          ,[amount_disb_month]
                          ,[amount_repaid_month]
                          ,[account_status_date]
                          ,[chksum]
                          ,[int_amount_disb_month]
                          ,[int_amount_repaid_month]
                          ,[PPN_DT]
                          ,[NewStatus]                           
                          ,[BATCH_ID]
                          ,[BUSINESS_DT]
                          ,[RUN_TIMESTAMP]
                    )
                    select TOP 10000
                           [fi_code]
                          ,[app_sys_code]
                          ,[master_account_no]
                          ,[sub_account_no]
                          ,[position_date]
                          ,[total_outstanding]
                          ,[month_in_arrears]
                          ,[no_of_inst_in_arrears]
                          ,[amount_undrawn]
                          ,[account_status]
                          ,[LastMaintDate]
                          ,[ExportDate]
                          ,[ErrCode]
                          ,[MaintStatus]
                          ,[Valid_Flag]
                          ,[MAkey]
                          ,[SAkey]
                          ,[CPKey]
                          ,[original_account_status]
                          ,[last_payment_date]
                          ,[subsi_leger_bal]
                          ,[write_off_date]
                          ,[MRchksum]
                          ,[loan_sold_sec_market]
                          ,[amount_disb_month]
                          ,[amount_repaid_month]
                          ,[account_status_date]
                          ,[chksum]
                          ,[int_amount_disb_month]
                          ,[int_amount_repaid_month]
                          ,[PPN_DT]
                          ,[NewStatus]                          
                          ,@BATCH_ID
                          ,@BUSINESS_DT
                          ,@RUN_TIMESTAMP
                     from  
                     [PFIv2].dbo.tblcreditposition (nolock)
                     --where
                    --    ExportDate < (dateadd(year, -@TAPE_RETENTION_PD,@RUN_TIMESTAMP))
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH


IF  ( @BL_TABLE_NAME ='tblProvision' AND @ARCHIVAL_FLAG='TAPE' )

        BEGIN TRY         
                
            BEGIN TRANSACTION;
                    
            INSERT INTO 
                [CCRISARCH].[dbo].tbl_arch_tape_Provision
                (
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[sub_account_no]
                      ,[position_date]
                      ,[classification]
                      ,[month_in_arrears]
                      ,[principal_outstanding]
                      ,[interest_outstanding]
                      ,[other_charges]
                      ,[realisable_value]
                      ,[iis_open_balance]
                      ,[iis_suspend_amount]
                      ,[iis_written_back_amount]
                      ,[iis_written_off_amount]
                      ,[iis_loan_sold_to_danaharta]
                      ,[iis_transfer_to_provision]
                      ,[sp_open_balance]
                      ,[sp_charged]
                      ,[sp_written_back_amount]
                      ,[sp_written_off_amount]
                      ,[sp_loan_sold_to_danaharta]
                      ,[sp_transfer_to_provision]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[SAkey]
                      ,[total_outstanding_PV]
                      ,[iis_reversal]
                      ,[sp_reversal]
                      ,[iis_close_bal]
                      ,[sp_close_bal]
                      ,[PVKey]
                      ,[MRchksum]
                      ,[prov_tag]
                      ,[prov_date]
                      ,[impaired_loan]
                      ,[ind_impair_provision_amount]
                      ,[impair_loan_written_back_amount]
                      ,[loan_written_off_amount]
                      ,[new_realisable_value]
                      ,[new_iis_open_balance]
                      ,[new_iis_suspend_amount]
                      ,[new_iis_written_back_amount]
                      ,[new_iis_written_off_amount]
                      ,[new_iis_loan_sold_to_danaharta]
                      ,[new_iis_transfer_to_provision]
                      ,[new_sp_open_balance]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
                )
                SELECT TOP 10000
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[sub_account_no]
                      ,[position_date]
                      ,[classification]
                      ,[month_in_arrears]
                      ,[principal_outstanding]
                      ,[interest_outstanding]
                      ,[other_charges]
                      ,[realisable_value]
                      ,[iis_open_balance]
                      ,[iis_suspend_amount]
                      ,[iis_written_back_amount]
                      ,[iis_written_off_amount]
                      ,[iis_loan_sold_to_danaharta]
                      ,[iis_transfer_to_provision]
                      ,[sp_open_balance]
                      ,[sp_charged]
                      ,[sp_written_back_amount]
                      ,[sp_written_off_amount]
                      ,[sp_loan_sold_to_danaharta]
                      ,[sp_transfer_to_provision]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[SAkey]
                      ,[total_outstanding_PV]
                      ,[iis_reversal]
                      ,[sp_reversal]
                      ,[iis_close_bal]
                      ,[sp_close_bal]
                      ,[PVKey]
                      ,[MRchksum]
                      ,[prov_tag]
                      ,[prov_date]
                      ,[impaired_loan]
                      ,[ind_impair_provision_amount]
                      ,[impair_loan_written_back_amount]
                      ,[loan_written_off_amount]
                      ,[new_realisable_value]
                      ,[new_iis_open_balance]
                      ,[new_iis_suspend_amount]
                      ,[new_iis_written_back_amount]
                      ,[new_iis_written_off_amount]
                      ,[new_iis_loan_sold_to_danaharta]
                      ,[new_iis_transfer_to_provision]
                      ,[new_sp_open_balance]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                  FROM [PFIv2].dbo.tblProvision (nolock) 
                 -- where
                 -- ExportDate < (dateadd(year, -@TAPE_RETENTION_PD,@RUN_TIMESTAMP))
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblsubaccount' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
                            
            BEGIN TRANSACTION;
                
            INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_SubAccount
                (
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[sub_account_no]
                      ,[facility_type]
                      ,[syndicated]
                      ,[special_fund_scheme]
                      ,[loanpurpose]
                      ,[financingconcept]
                      ,[original_tenure]
                      ,[principal_repay_term]
                      ,[restrusche]
                      ,[date_sold_to_cagamas]
                      ,[sold_to_cagamas]
                      ,[date_sold_to_danaharta]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[SAkey]
                      ,[FMSGL]
                      ,[prod_cd]
                      ,[dept_cd]
                      ,[sector_cd]
                      ,[business_activity_cd]
                      ,[smi_indicator]
                      ,[card_type]
                      ,[loan_utilisation_state]
                      ,[remaining_maturity]
                      ,[maturity_date]
                      ,[interest_rate_info]
                      ,[MRchksum]
                      ,[disbursed_curr]
                      ,[loan_currency]
                      ,[interest_rate]
                      ,[rebate_rate]
                      ,[asset_purchase_value]
                      ,[dfi_finan_detail]
                      ,[pricing_type]
                      ,[maturitydate]
                      ,[first_disb_date]
                      ,[loan_utilised]
                      ,[priority_sector]
                      ,[loan_util_location]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
                )

SELECT
                     a.[fi_code]
                    ,a.[app_sys_code]
                    ,a.[master_account_no]
                    ,a.[sub_account_no]
                    ,a.[facility_type]
                    ,a.[syndicated]
                    ,a.[special_fund_scheme]
                    ,a.[loanpurpose]
                    ,a.[financingconcept]
                    ,a.[original_tenure]
                    ,a.[principal_repay_term]
                    ,a.[restrusche]
                    ,a.[date_sold_to_cagamas]
                    ,a.[sold_to_cagamas]
                    ,a.[date_sold_to_danaharta]
                    ,a.[LastMaintDate]
                    ,a.[ExportDate]
                    ,a.[ErrCode]
                    ,a.[MaintStatus]
                    ,a.[Valid_Flag]
                    ,a.[MAkey]
                    ,a.[SAkey]
                    ,a.[FMSGL]
                    ,a.[prod_cd]
                    ,a.[dept_cd]
                    ,a.[sector_cd]
                    ,a.[business_activity_cd]
                    ,a.[smi_indicator]
                    ,a.[card_type]
                    ,a.[loan_utilisation_state]
                    ,a.[remaining_maturity]
                    ,a.[maturity_date]
                    ,a.[interest_rate_info]
                    ,a.[MRchksum]
                    ,a.[disbursed_curr]
                    ,a.[loan_currency]
                    ,a.[interest_rate]
                    ,a.[rebate_rate]
                    ,a.[asset_purchase_value]
                    ,a.[dfi_finan_detail]
                    ,a.[pricing_type]
                    ,a.[maturitydate]
                    ,a.[first_disb_date]
                    ,a.[loan_utilised]
                    ,a.[priority_sector]
                    ,a.[loan_util_location]
                    ,a.[chksum]
                    ,a.[PPN_DT]
                    ,a.[NewStatus]                    
                    ,@BATCH_ID
                    ,@BUSINESS_DT
                    ,@RUN_TIMESTAMP
from  
    [PFIv2].dbo.tblsubaccount (nolock) a,
(
            select                     
                        sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]
            from  
                    [PFIv2].dbo.tblsubaccount (nolock) sb_acc,
                    [CCRISARCH].[dbo].tbl_arch_tape_CreditPosition (nolock) cp
            where 
                         sb_acc.[fi_code]=cp.[fi_code]
                     and sb_acc.[app_sys_code]=cp.[app_sys_code]
                     and sb_acc.[master_account_no]=cp.[master_account_no]
                     and sb_acc.[sub_account_no]=cp.[sub_account_no]
                     and cp.BATCH_ID=@BATCH_ID
                     and cp.BUSINESS_DT=@BUSINESS_DT
                     and cp.RUN_TIMESTAMP=@RUN_TIMESTAMP
                     
UNION

            select                     
                        sb_acc.[fi_code]
                    ,sb_acc.[app_sys_code]
                    ,sb_acc.[master_account_no]
                    ,sb_acc.[sub_account_no]
            from  
                    [PFIv2].dbo.tblsubaccount (nolock) sb_acc,
                    [CCRISARCH].[dbo].tbl_arch_tape_Provision (nolock) prov
            where 
                         sb_acc.[fi_code]=prov.[fi_code]
                     and sb_acc.[app_sys_code]=prov.[app_sys_code]
                     and sb_acc.[master_account_no]=prov.[master_account_no]
                     and sb_acc.[sub_account_no]=prov.[sub_account_no]
                     and prov.BATCH_ID=@BATCH_ID
                     and prov.BUSINESS_DT=@BUSINESS_DT
                     and prov.RUN_TIMESTAMP=@RUN_TIMESTAMP
                     
) b
where 
            a.[fi_code]=b.[fi_code]
        and a.[app_sys_code]=b.[app_sys_code]
        and a.[master_account_no]=b.[master_account_no]
        and a.[sub_account_no]=b.[sub_account_no]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
IF  ( @BL_TABLE_NAME ='tblMasterAccount' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
                
            BEGIN TRANSACTION;
                    
INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_MasterAccount 
                (
                                                   [fi_code]
                                                  ,[app_sys_code]
                                                  ,[master_account_no]
                                                  ,[appr_limit_curr]
                                                  ,[appr_limit_actual_curr]
                                                  ,[appr_limit_rm]
                                                  ,[appr_date]
                                                  ,[LastMaintDate]
                                                  ,[ExportDate]
                                                  ,[ErrCode]
                                                  ,[MaintStatus]
                                                  ,[Valid_Flag]
                                                  ,[MAkey]
                                                  ,[relationship_status]
                                                  ,[chksum]
                                                  ,[PPN_DT]
                                                  ,[NewStatus]                                                  
                                                  ,[BATCH_ID]
                                                  ,[BUSINESS_DT]
                                                  ,[RUN_TIMESTAMP]

                )

                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[appr_limit_curr]
                      ,a.[appr_limit_actual_curr]
                      ,a.[appr_limit_rm]
                      ,a.[appr_date]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[MAkey]
                      ,a.[relationship_status]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP                                            
                from  
                    [PFIv2].dbo.tblMasterAccount (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[master_account_no]
                    from [CCRISARCH].[dbo].tbl_arch_tape_SubAccount 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]
                        
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
                        
IF  ( @BL_TABLE_NAME ='tblAccountHolder' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_AccountHolder 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[type]
                      ,[custno]
                      ,[name_sort_key]
                      ,[custname]
                      ,[entity_type]
                      ,[id_no1]
                      ,[id_no2]
                      ,[birthdate]
                      ,[idic_fi_code]
                      ,[idic_app_sys_code]
                      ,[MaintStatus]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MAkey]
                      ,[AHPKchksum]
                      ,[cust_cd]
                      ,[group_borrower_id]
                      ,[group_borrower_name]
                      ,[MRchksum]
                      ,[chksum]
                      ,[IDICkey]
                      ,[AHKey]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)

                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[type]
                      ,a.[custno]
                      ,a.[name_sort_key]
                      ,a.[custname]
                      ,a.[entity_type]
                      ,a.[id_no1]
                      ,a.[id_no2]
                      ,a.[birthdate]
                      ,a.[idic_fi_code]
                      ,a.[idic_app_sys_code]
                      ,a.[MaintStatus]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MAkey]
                      ,a.[AHPKchksum]
                      ,a.[cust_cd]
                      ,a.[group_borrower_id]
                      ,a.[group_borrower_name]
                      ,a.[MRchksum]
                      ,a.[chksum]
                      ,a.[IDICkey]
                      ,a.[AHKey]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblAccountHolder (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[master_account_no]
                    from [CCRISARCH].[dbo].tbl_arch_tape_MasterAccount 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]            
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblLegalAction' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
            
            BEGIN TRANSACTION;

                INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_LegalAction 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[master_account_no]
                      ,[legal_action]
                      ,[date_of_legal_status]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[MAkey]
                      ,[LAKey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[legal_action]
                      ,a.[date_of_legal_status]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[MAkey]
                      ,a.[LAKey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblLegalAction (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[master_account_no]
                    from [CCRISARCH].[dbo].tbl_arch_tape_MasterAccount (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
IF  ( @BL_TABLE_NAME ='tblAccountCollateral' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
                            
            BEGIN TRANSACTION;
                    
                INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral 
(
                   [fi_code]
                  ,[app_sys_code]
                  ,[master_account_no]
                  ,[sub_account_no]
                  ,[collateralrefno]
                  ,[col_type]
                  ,[col_value]
                  ,[col_fi_code]
                  ,[col_app_sys_code]
                  ,[MaintStatus]
                  ,[LastMaintDate]
                  ,[ExportDate]
                  ,[ErrCode]
                  ,[MAkey]
                  ,[SAkey]
                  ,[ACkey]
                  ,[ACKey2]
                  ,[col_key]
                  ,[rel_indc]
                  ,[financing_refinancing]
                  ,[auction_date]
                  ,[auction_price]
                  ,[purchase_date]
                  ,[purchase_price]
                  ,[vehicle_type]
                  ,[vehicle_year_of_financing]
                  ,[repo_tag]
                  ,[repo_date]
                  ,[repo_reserve_val]
                  ,[ACPKchksum]
                  ,[chksum]
                  ,[MRchksum]
                  ,[PPN_DT]
                  ,[NewStatus]                  
                  ,[BATCH_ID]
                  ,[BUSINESS_DT]
                  ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[master_account_no]
                      ,a.[sub_account_no]
                      ,a.[collateralrefno]
                      ,a.[col_type]
                      ,a.[col_value]
                      ,a.[col_fi_code]
                      ,a.[col_app_sys_code]
                      ,a.[MaintStatus]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MAkey]
                      ,a.[SAkey]
                      ,a.[ACkey]
                      ,a.[ACKey2]
                      ,a.[col_key]
                      ,a.[rel_indc]
                      ,a.[financing_refinancing]
                      ,a.[auction_date]
                      ,a.[auction_price]
                      ,a.[purchase_date]
                      ,a.[purchase_price]
                      ,a.[vehicle_type]
                      ,a.[vehicle_year_of_financing]
                      ,a.[repo_tag]
                      ,a.[repo_date]
                      ,a.[repo_reserve_val]
                      ,a.[ACPKchksum]
                      ,a.[chksum]
                      ,a.[MRchksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblAccountCollateral (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[master_account_no]
                    from [CCRISARCH].[dbo].tbl_arch_tape_MasterAccount (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[master_account_no]=b.[master_account_no]
            
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCorporation' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
                
            BEGIN TRANSACTION;
                    
                    INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_Corporation 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[custno]
                      ,[name_sort_key]
                      ,[custname]
                      ,[reg_no]
                      ,[date_of_reg]
                      ,[country_of_reg]
                      ,[type]
                      ,[rcc_nrcc]
                      ,[entity_type]
                      ,[resident_status]
                      ,[cust_maint_date]
                      ,[addr1]
                      ,[addr2]
                      ,[addr3]
                      ,[addr4]
                      ,[last_addr_maint_date]
                      ,[country_code]
                      ,[post_code]
                      ,[city_town]
                      ,[state_code]
                      ,[phone_1]
                      ,[phone_2]
                      ,[phone_3]
                      ,[phone_4]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[OWkey]
                      ,[cust_cd]
                      ,[group_borrower_id]
                      ,[group_borrower_name]
                      ,[corporate_status]
                      ,[industrial_sector]
                      ,[member_of_bank_rakyat]
                      ,[IDICkey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[custno]
                      ,a.[name_sort_key]
                      ,a.[custname]
                      ,a.[reg_no]
                      ,a.[date_of_reg]
                      ,a.[country_of_reg]
                      ,a.[type]
                      ,a.[rcc_nrcc]
                      ,a.[entity_type]
                      ,a.[resident_status]
                      ,a.[cust_maint_date]
                      ,a.[addr1]
                      ,a.[addr2]
                      ,a.[addr3]
                      ,a.[addr4]
                      ,a.[last_addr_maint_date]
                      ,a.[country_code]
                      ,a.[post_code]
                      ,a.[city_town]
                      ,a.[state_code]
                      ,a.[phone_1]
                      ,a.[phone_2]
                      ,a.[phone_3]
                      ,a.[phone_4]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[OWkey]
                      ,a.[cust_cd]
                      ,a.[group_borrower_id]
                      ,a.[group_borrower_name]
                      ,a.[corporate_status]
                      ,a.[industrial_sector]
                      ,a.[member_of_bank_rakyat]
                      ,a.[IDICkey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCorporation (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[custno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountHolder (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and entity_type <> '11'
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[custno]=b.[custno]
                    
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblOwner' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
                
            BEGIN TRANSACTION;
                    
INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_Owner 
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[custno]
                      ,[custname]
                      ,[reg_no]
                      ,[name_sort_key]
                      ,[owner_name_sort_key]
                      ,[ownername]
                      ,[owner_id_no1]
                      ,[owner_id_no2]
                      ,[owner_last_maint_date]
                      ,[MaintStatus]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[chksum]
                      ,[OWkey]
                      ,[OWKey2]
                      ,[OWPKchksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[custno]
                      ,a.[custname]
                      ,a.[reg_no]
                      ,a.[name_sort_key]
                      ,a.[owner_name_sort_key]
                      ,a.[ownername]
                      ,a.[owner_id_no1]
                      ,a.[owner_id_no2]
                      ,a.[owner_last_maint_date]
                      ,a.[MaintStatus]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[chksum]
                      ,a.[OWkey]
                      ,a.[OWKey2]
                      ,a.[OWPKchksum]
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblOwner (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[custno]
                        ,[name_sort_key]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_Corporation (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      AND entity_type in ('21','22')
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[custno]=b.[custno]
                        and a.[name_sort_key]=b.[name_sort_key]                
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblIndividual' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
                INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_Individual
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[custno]
                      ,[name_sort_key]
                      ,[custname]
                      ,[id_no1]
                      ,[id_no2]
                      ,[birth_date]
                      ,[gender]
                      ,[nationality]
                      ,[marital_status]
                      ,[bumi_code]
                      ,[entity_type]
                      ,[resident_status]
                      ,[cust_maint_date]
                      ,[addr1]
                      ,[addr2]
                      ,[addr3]
                      ,[addr4]
                      ,[last_addr_maint_date]
                      ,[country_code]
                      ,[post_code]
                      ,[city_town]
                      ,[state_code]
                      ,[phone_1]
                      ,[phone_2]
                      ,[phone_3]
                      ,[phone_4]
                      ,[employer_name]
                      ,[occupation]
                      ,[last_empdet_maint_date]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[cust_cd]
                      ,[group_borrower_id]
                      ,[group_borrower_name]
                      ,[member_of_bank_rakyat]
                      ,[IDICkey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[custno]
                      ,a.[name_sort_key]
                      ,a.[custname]
                      ,a.[id_no1]
                      ,a.[id_no2]
                      ,a.[birth_date]
                      ,a.[gender]
                      ,a.[nationality]
                      ,a.[marital_status]
                      ,a.[bumi_code]
                      ,a.[entity_type]
                      ,a.[resident_status]
                      ,a.[cust_maint_date]
                      ,a.[addr1]
                      ,a.[addr2]
                      ,a.[addr3]
                      ,a.[addr4]
                      ,a.[last_addr_maint_date]
                      ,a.[country_code]
                      ,a.[post_code]
                      ,a.[city_town]
                      ,a.[state_code]
                      ,a.[phone_1]
                      ,a.[phone_2]
                      ,a.[phone_3]
                      ,a.[phone_4]
                      ,a.[employer_name]
                      ,a.[occupation]
                      ,a.[last_empdet_maint_date]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[cust_cd]
                      ,a.[group_borrower_id]
                      ,a.[group_borrower_name]
                      ,a.[member_of_bank_rakyat]
                      ,a.[IDICkey]
                      ,a.[chksum]
                      ,a.[PPN_DT]
                      ,[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblIndividual (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[custno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountHolder (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and entity_type='11'
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[custno]=b.[custno]
                    
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCProperty' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
                INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_CProperty
(
                       [fi_code]
                      ,[app_sys_code]
                      ,[collateralrefno]
                      ,[InstrumentOfClaim]
                      ,[RankOfCharge]
                      ,[Shared]
                      ,[LandUse]
                      ,[PropertyDesc]
                      ,[PropertyLocation]
                      ,[ForcedSaleValue]
                      ,[ReservedValue]
                      ,[ValuationDate]
                      ,[Valuer]
                      ,[LastMaintDate]
                      ,[ExportDate]
                      ,[ErrCode]
                      ,[MaintStatus]
                      ,[Valid_Flag]
                      ,[ACkey]
                      ,[chksum]
                      ,[PPN_DT]
                      ,[NewStatus]                      
                      ,[BATCH_ID]
                      ,[BUSINESS_DT]
                      ,[RUN_TIMESTAMP]
)
                SELECT 
                       a.[fi_code]
                      ,a.[app_sys_code]
                      ,a.[collateralrefno]
                      ,a.[InstrumentOfClaim]
                      ,a.[RankOfCharge]
                      ,a.[Shared]
                      ,a.[LandUse]
                      ,a.[PropertyDesc]
                      ,a.[PropertyLocation]
                      ,a.[ForcedSaleValue]
                      ,a.[ReservedValue]
                      ,a.[ValuationDate]
                      ,a.[Valuer]
                      ,a.[LastMaintDate]
                      ,a.[ExportDate]
                      ,a.[ErrCode]
                      ,a.[MaintStatus]
                      ,a.[Valid_Flag]
                      ,a.[ACkey]
                      ,a.[chksum]                  
                      ,a.[PPN_DT]
                      ,a.[NewStatus]                      
                      ,@BATCH_ID
                      ,@BUSINESS_DT
                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCProperty (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral (nolock)
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=10 
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCMotor' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
            BEGIN TRANSACTION;
            
            
            INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_CMotor
(
                                           [fi_code]
                                          ,[app_sys_code]
                                          ,[collateralrefno]
                                          ,[InstrumentOfClaim]
                                          ,[Model]
                                          ,[EngineNo]
                                          ,[ChassisNo]
                                          ,[VehicleNo]
                                          ,[LastMaintDate]
                                          ,[ExportDate]
                                          ,[ErrCode]
                                          ,[MaintStatus]
                                          ,[Valid_Flag]
                                          ,[ACkey]
                                          ,[chksum]
                                          ,[PPN_DT]
                                          ,[NewStatus]                                          
                                          ,[BATCH_ID]
                                          ,[BUSINESS_DT]
                                          ,[RUN_TIMESTAMP]
)
                                select 
                                           a.[fi_code]
                                          ,a.[app_sys_code]
                                          ,a.[collateralrefno]
                                          ,a.[InstrumentOfClaim]
                                          ,a.[Model]
                                          ,a.[EngineNo]
                                          ,a.[ChassisNo]
                                          ,a.[VehicleNo]
                                          ,a.[LastMaintDate]
                                          ,a.[ExportDate]
                                          ,a.[ErrCode]
                                          ,a.[MaintStatus]
                                          ,a.[Valid_Flag]
                                          ,a.[ACkey]
                                          ,a.[chksum]
                                          ,a.[PPN_DT]
                                          ,a.[NewStatus]                                          
                                          ,@BATCH_ID
                                          ,@BUSINESS_DT
                                          ,@RUN_TIMESTAMP
                                from  
                                    [PFIv2].dbo.tblCMotor (nolock) a,
                                (
                                    select 
                                         [fi_code]
                                        ,[app_sys_code]
                                        ,[collateralrefno]
                                    from 
                                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral 
                                    where 
                                          BATCH_ID=@BATCH_ID
                                      and BUSINESS_DT=@BUSINESS_DT
                                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                                      and col_type=30 
                                ) b
                                where 
                                            a.[fi_code]=b.[fi_code]
                                        and a.[app_sys_code]=b.[app_sys_code]
                                        and a.[collateralrefno]=b.[collateralrefno]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
                    
IF  ( @BL_TABLE_NAME ='tblCCarrier' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 
            BEGIN TRANSACTION;
            
            INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_CCarrier
(
                               [fi_code]
                              ,[app_sys_code]
                              ,[collateralrefno]
                              ,[InstrumentOfClaim]
                              ,[ColDesc]
                              ,[ValuationDate]
                              ,[LastMaintDate]
                              ,[ExportDate]
                              ,[ErrCode]
                              ,[MaintStatus]
                              ,[Valid_Flag]
                              ,[ACkey]
                              ,[chksum]
                              ,[PPN_DT]
                              ,[NewStatus]                              
                              ,[BATCH_ID]
                              ,[BUSINESS_DT]
                              ,[RUN_TIMESTAMP]
)
                SELECT 
                                  a.[fi_code]
                              ,a.[app_sys_code]
                              ,a.[collateralrefno]
                              ,a.[InstrumentOfClaim]
                              ,a.[ColDesc]
                              ,a.[ValuationDate]
                              ,a.[LastMaintDate]
                              ,a.[ExportDate]
                              ,a.[ErrCode]
                              ,a.[MaintStatus]
                              ,a.[Valid_Flag]
                              ,a.[ACkey]
                              ,a.[chksum]
                              ,a.[PPN_DT]
                              ,a.[NewStatus]                              
                              ,@BATCH_ID
                              ,@BUSINESS_DT
                              ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCCarrier (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=40 
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
            
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCPlant' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                
                INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_CPlant
(
                               [fi_code]
                              ,[app_sys_code]
                              ,[collateralrefno]
                              ,[InstrumentOfClaim]
                              ,[ColDesc]
                              ,[ValuationDate]
                              ,[LastMaintDate]
                              ,[ExportDate]
                              ,[ErrCode]
                              ,[MaintStatus]
                              ,[Valid_Flag]
                              ,[ACkey]
                              ,[chksum]
                              ,[PPN_DT]
                              ,[NewStatus]                              
                              ,[BATCH_ID]
                              ,[BUSINESS_DT]
                              ,[RUN_TIMESTAMP]
)
                SELECT 
                               a.[fi_code]
                              ,a.[app_sys_code]
                              ,a.[collateralrefno]
                              ,a.[InstrumentOfClaim]
                              ,a.[ColDesc]
                              ,a.[ValuationDate]
                              ,a.[LastMaintDate]
                              ,a.[ExportDate]
                              ,a.[ErrCode]
                              ,a.[MaintStatus]
                              ,a.[Valid_Flag]
                              ,a.[ACkey]
                              ,a.[chksum]
                              ,a.[PPN_DT]
                              ,a.[NewStatus]                              
                              ,@BATCH_ID
                              ,@BUSINESS_DT
                              ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCPlant (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=60 
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCConcession' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
                    
                INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_Cconcession

(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[ColDesc]
                                      ,[ValuationDate]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[ColDesc]
                                      ,a.[ValuationDate]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCConcession (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=50 
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCAssets' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
            
            INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_CAssets
(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[ColDesc]
                                      ,[ValuationDate]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[ColDesc]
                                      ,a.[ValuationDate]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCAssets (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=29 
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
IF  ( @BL_TABLE_NAME ='tblCOtherAssets' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
            
            INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_COtherAssets
(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[ColDesc]
                                      ,[ValuationDate]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[ColDesc]
                                      ,a.[ValuationDate]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCOtherAssets (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=90
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH
            
            
IF  ( @BL_TABLE_NAME ='tblCGuarantees' AND @ARCHIVAL_FLAG='TAPE' )

            BEGIN TRY 

            BEGIN TRANSACTION;
            
            INSERT INTO [CCRISARCH].[dbo].tbl_arch_tape_Cguarantees
(
                                       [fi_code]
                                      ,[app_sys_code]
                                      ,[collateralrefno]
                                      ,[InstrumentOfClaim]
                                      ,[Nature]
                                      ,[GuarantorName]
                                      ,[GuarantorIDNo]
                                      ,[CountryOfOrigin]
                                      ,[LastMaintDate]
                                      ,[ExportDate]
                                      ,[ErrCode]
                                      ,[MaintStatus]
                                      ,[Valid_Flag]
                                      ,[ACkey]
                                      ,[guarantee_fee_rate]
                                      ,[chksum]
                                      ,[PPN_DT]
                                      ,[NewStatus]                                      
                                      ,[BATCH_ID]
                                      ,[BUSINESS_DT]
                                      ,[RUN_TIMESTAMP]
)
                SELECT 
                                       a.[fi_code]
                                      ,a.[app_sys_code]
                                      ,a.[collateralrefno]
                                      ,a.[InstrumentOfClaim]
                                      ,a.[Nature]
                                      ,a.[GuarantorName]
                                      ,a.[GuarantorIDNo]
                                      ,a.[CountryOfOrigin]
                                      ,a.[LastMaintDate]
                                      ,a.[ExportDate]
                                      ,a.[ErrCode]
                                      ,a.[MaintStatus]
                                      ,a.[Valid_Flag]
                                      ,a.[ACkey]
                                      ,a.[guarantee_fee_rate]
                                      ,a.[chksum]
                                      ,a.[PPN_DT]
                                      ,a.[NewStatus]                                      
                                      ,@BATCH_ID
                                      ,@BUSINESS_DT
                                      ,@RUN_TIMESTAMP
                from  
                    [PFIv2].dbo.tblCGuarantees (nolock) a,
                (
                    select 
                         [fi_code]
                        ,[app_sys_code]
                        ,[collateralrefno]
                    from 
                        [CCRISARCH].[dbo].tbl_arch_tape_AccountCollateral (nolock) 
                    where 
                          BATCH_ID=@BATCH_ID
                      and BUSINESS_DT=@BUSINESS_DT
                      and RUN_TIMESTAMP=@RUN_TIMESTAMP
                      and col_type=70
                ) b
                where 
                            a.[fi_code]=b.[fi_code]
                        and a.[app_sys_code]=b.[app_sys_code]
                        and a.[collateralrefno]=b.[collateralrefno]
                    
                
            COMMIT TRANSACTION;

            END TRY

            BEGIN CATCH

                PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
                      ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
                      ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
                      ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
                      ', Line ' + CONVERT(varchar(5), ERROR_LINE());
                PRINT ERROR_MESSAGE();
                
                    IF XACT_STATE() <> 0
                BEGIN
                    ROLLBACK TRANSACTION;
                END

            END CATCH


END 