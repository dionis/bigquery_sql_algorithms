WITH exclusions as (SELECT UPPER(ClaimID) as excclaim FROM `pora-hms.rawdata.inpora` WHERE AlgorithmID IN (0,1,14,1490 )),

base as (  SELECT DISTINCT fp.ClaimID as ClaimID,  cast(fp.Line_Seq as string) as Clm_Ln,  fp.Line_Seq as IClm_Ln,
fp.MemberID,   fp.ProgramName, fp.ProviderID ,  fp.AffiliationID, fp.FED_TAX_ID as Prov_TaxID,   CAST (fp.PaidDate as DATE) PAID_DATE, CAST (fp.ClaimBeginDate as  DATE)DOS_FROM,  CAST (fp.ClaimEndDate as DATE)  DOS_TO, fp.PlaceOfService as POS, fp.Claim_Sub as ClaimStatus,   fp.Revenue_Code,  fp.HCPC_CODE, fp.MODIFIER_CD, fp.Units as Units_Billed, fp.LineWithHoldAmt as Tot_Clm_Discount, fp.CoinsuranceAmt as Tot_Clm_CoIns, fp.CopayAmt as TotClm_Cpay, fp.InterestPaid as InterestDays,     fp.LinePaid as Line_Amt_Paid, fp.ClaimType as ClaimFormType,  fp.TOS,fp.BillType,fp.ParNonPar,fp.ServiceBeginDate,fp.DischargeStatus,fp.MODIFIER_CD2,fp.MODIFIER_CD3,fp.MODIFIER_CD4
FROM `pora-hms.rawdata.finalPool_v2` fp  
where  fp.LinePaid > 0 and  fp.Claim_Sub = 'PAID'  and fp.LineStatus = 'PAID'  and fp.LineCOB = 0  and fp.LinePaid > 5 
--and fp.DischargeStatus = '30'
and fp.BillType not in ('0211', '0212', '0213' ,'0214','0115','0117','0112','0113', '0132','0133')
and fp.BillType not like '02%' -- exclude in script all SNF (Type of bills 02XX) per Irene 06/24/2024
and fp.BillType not like '03%' -- exclude in script all Home Health (Type of bills 03XX) per Irene 06/24/2024
and  fp.ClaimType = 'I'  and fp.PaidDate > CAST (DATE_SUB(CURRENT_DATE(),interval 36 MONTH) AS DATE) 

order by fp.ClaimID, IClm_Ln  ) 

, ScenarioOne as (
  select DISTINCT 'Scenario 1' Bucket,b.ClaimID, b.IClm_Ln, b.DischargeStatus, b.Line_Amt_Paid, o.claimid refClaim, o.DischargeStatus ref_DischargeStatus ,b.ProgramName,b.BillType, o.BillType refBillType,b.ParNonPar,o.ParNonPar refParNonPar, b.Prov_TaxID,b.MemberID, b.Revenue_Code, o.Revenue_Code refRevenue_Code, b.HCPC_CODE, o.HCPC_CODE refHCPC_CODE
from base b
inner join `pora-hms.rawdata.finalPool_v2` o 
  on b.MemberID = o.MemberID and b.ProviderID = o.ProviderID  and b.AffiliationID = o.AffiliationID and b.ServiceBeginDate = o.ServiceBeginDate
  and b.ClaimFormType = o.ClaimType  and b.TOS = o.TOS
where  o.LinePaid > 0 and  o.Claim_Sub = 'PAID'  and o.LineStatus = 'PAID'  and o.LinePaid > 5 and o.LineCOB = 0 
and o.DischargeStatus in ('01','02','03','04','05','06','07','09','21','30','43','50','51','61','62','63','64','65','66','69','70','82','83','85','86','90','91','93','94')
and  o.ClaimType = 'I' 
-- For QA 
and ( -- added for sample
   b.BillType = '0131' and o.BillType in ('0131','0137') -- outpatient
   or b.BillType = '0111' and o.BillType in ('0111','0117') -- inpatient
) 
and (b.Revenue_Code = o.Revenue_Code and b.HCPC_CODE = o.HCPC_CODE) 
and b.DischargeStatus <> o.DischargeStatus

and o.BillType not in ('0211', '0212', '0213' ,'0214','0115','0117','0112','0113', '0132','0133')
and o.BillType not like '02%' -- exclude in script all SNF (Type of bills 02XX) per Irene 06/24/2024
and o.BillType not like '03%' -- exclude in script all Home Health (Type of bills 03XX) per Irene 06/24/2024
and '25' not in  (ifnull(b.MODIFIER_CD,''),', ',ifnull(b.MODIFIER_CD2,''),', ',ifnull(b.MODIFIER_CD3,''),', ',ifnull(b.MODIFIER_CD4,'')) -- Added 09-25-2024 White Paper Updated 
),

#########################################################################################################################


otherdischarge as (
 (
    SELECT * FROM FilterScenarioOneClean WHERE row_number_sum_string = 1
        UNION DISTINCT
    SELECT * FROM FilterScenarioOneNotInters
 ) 

 UNION ALL
   
   (
    SELECT * FROM FilterScenarioTwoClean WHERE row_number_sum_string = 1
        UNION DISTINCT
    SELECT * FROM FilterScenarioTwoNotInters
 ) 

)

,
otherdischargefinal as (select ClaimID,max(refClaim) refClaim from otherdischarge 
  group by ClaimID)
/*
Services billed and paid, (service codes XXX/XXXX) billed and paid on claim #____________with different discharge status code-overlapping services billed and paid, on one claim or multiple claims, on the same dos as the overpayment claim
*/
,Final as (
  select od.Bucket , od.ClaimID CLAIM,sum(Line_Amt_Paid) OVPAMT, 
CONCAT('Services billed and paid, (service codes ',STRING_AGG(DISTINCT Case when od.Revenue_Code = RefRevenue_Code then od.Revenue_Code else '' end),STRING_AGG(DISTINCT case when od.HCPC_CODE = od.refHCPC_Code then '/' else '' end),STRING_AGG(DISTINCT case when od.HCPC_CODE = od.refHCPC_Code then od.HCPC_CODE else '' end),') billed and paid on claim #',od.ClaimID,' with different discharge status code-overlapping services billed and paid, on one claim or multiple claims, on the same dos as the overpayment claim ')  as LETTERCOMMENT,
CONCAT('Claim ',od.ClaimID,' w/ Discharge Status(',DischargeStatus, ') has overlapping services on claim ',(od.refClaim),' w/ Discharge Status(',ref_DischargeStatus, ').') INTERNALCOMMENT 
, 3 as STATUS,'pora@360healthsystems.com'as USERID, '25' as VENDORNAME, '1490' as ALGOID, CURRENT_DATE()IDDATE, (od.refClaim) as ReferenceClaim 
,concat(STRING_AGG(DISTINCT Case when od.Revenue_Code = RefRevenue_Code then od.Revenue_Code else '' end),'/',STRING_AGG(DISTINCT case when od.HCPC_CODE = od.refHCPC_Code then od.HCPC_CODE else '' end)) codes
,STRING_AGG(DISTINCT od.Revenue_Code) Revenue_Code,STRING_AGG(DISTINCT refRevenue_Code) refRevenue_Code,STRING_AGG(DISTINCT od.HCPC_CODE) HCPC_CODE ,STRING_AGG(DISTINCT refHCPC_CODE) refHCPC_CODE
from otherdischarge od 
join otherdischargefinal odf on od.ClaimID = odf.ClaimID and od.refClaim = odf.refClaim
left join exclusions exc on od.ClaimID=exc.excclaim 
where exc.excclaim is null 
Group by od.Bucket, od.ClaimID,DischargeStatus,od.ref_DischargeStatus,od.Prov_TaxID,MemberID,od.refClaim
having OVPAMT>= 10    

)

,QA_Final as (
SELECT distinct 
g.Bucket, g.CLAIM, g.OVPAMT, Concat(LETTERCOMMENT,
case when t.ProgramName = 'PP' then ' as per Maryland Medicaid guidelines.' 
  when t.ProgramName = 'MA' then ' as per CMS guidelines.' 
  else ' as per JHHC policy' end ) as LETTERCOMMENT,
  Concat('Service code ',g.codes,' is to be fully.') INTERNALCOMMENT,
  g.Status, g.USERID,VENDORNAME,ALGOID,IDDATE,MAX(ReferenceClaim) ReferenceClaim
  ,Concat('Service code ',g.codes,' is to be fully.') AdjustmentCOMMENT,t.ProgramName
  ,g.Revenue_Code,  refRevenue_Code,g.HCPC_CODE, refHCPC_CODE
FROM Final g  
  join `pora-hms.rawdata.finalPool_v2` t on g.CLAIM = t.ClaimID
  left join `pora-hms.rawdata.multiplan` mp on t.ClaimID = mp.ClaimId 
where mp.ClaimID is null
--and t.ProgramName = 'USFHP'
--and g.CLAIM = '20210503211350450001'
GROUP BY g.Bucket, g.CLAIM, g.OVPAMT,t.ProgramName,g.Status, g.USERID,VENDORNAME,ALGOID,IDDATE,
LETTERCOMMENT,
g.Revenue_Code,  refRevenue_Code,g.HCPC_CODE, refHCPC_CODE,g.codes
),

QA_Final_Max AS (
  SELECT *, MAX(OVPAMT) AS MAX_VALUE, 
    ROW_NUMBER() OVER (
            PARTITION BY CLAIM           
        ) AS row_number_max,
   FROM QA_Final
    GROUP BY CLAIM, Bucket, OVPAMT, ProgramName, Status,  USERID,VENDORNAME,ALGOID,IDDATE,LETTERCOMMENT,
    Revenue_Code,  refRevenue_Code, HCPC_CODE, refHCPC_CODE, INTERNALCOMMENT, ReferenceClaim, AdjustmentComment
)

SELECT * EXCEPT (MAX_VALUE, row_number_max)
 FROM QA_Final_Max 
  WHERE QA_Final_Max.row_number_max = 1     
Order by QA_Final_Max.CLAIM

/*
----QA fileds
Select distinct 
f.CLAIM, f.OVPAMT, LETTERCOMMENT,INTERNALCOMMENT,
f.Status, f.USERID,VENDORNAME,ALGOID,IDDATE,MAX(ReferenceClaim) ReferenceClaim,
AdjustmentCOMMENT,f.ProgramName

,max(b.ProviderID)  ProviderID, max( b.AffiliationID) AffiliationID, b.ParNonPar, max(b.DischargeStatus) DischargeStatus,max(b.BillType) BillType,max(b.TOS) TOS, MAX(b.PlaceOfservice) as POS, MAX(b.DRG_Num) DRG_Num, MAX(b.AdmitDate) AdmitDate, MAX(b.DischargeDate) DischargeDate
,MAX(r.ProviderID) refProviderID ,  MAX(r.AffiliationID) refAffiliationID, MAX(r.ParNonPar) refParNonPar, MAX(r.DischargeStatus) refDischargeStatus,MAX(r.BillType) refBillType, MAX(r.TOS) refTOS, MAX(r.PlaceOfservice) as refPOS, MAX(r.DRG_Num) refDRG_Num, MAX(r.AdmitDate) refAdmitDate, MAX(r.DischargeDate) refDischargeDate
,f.Revenue_Code,  refRevenue_Code,f.HCPC_CODE, refHCPC_CODE

from QA_Final f
  join `pora-hms.rawdata.finalPool_v2`  b on f.CLAIM = b.ClaimID
  left join `pora-hms.rawdata.finalPool_v2`  r on f.ReferenceClaim = r.ClaimID
  GROUP BY f.CLAIM, f.OVPAMT, LETTERCOMMENT,INTERNALCOMMENT,
f.Status, f.USERID,VENDORNAME,ALGOID,IDDATE
,AdjustmentCOMMENT,f.ProgramName
, b.ParNonPar
,f.Revenue_Code,  refRevenue_Code,f.HCPC_CODE, refHCPC_CODE
order by f.CLAIM
*/