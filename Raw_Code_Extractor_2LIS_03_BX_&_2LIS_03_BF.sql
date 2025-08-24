with 
    t156 as (
        select *
        from RAW.SAP_DEV.T156
    ),

    t156c as (
        select *
        from RAW.SAP_DEV.T156C
    ),

    t156m as (
        select *
        from RAW.SAP_DEV.T156M  
    ),

    tmca as (
        select *
        from RAW.SAP_DEV.TMCA
    ),

    mkpf as (
        select *  
        from RAW.SAP_DEV.MKPF
    ),
    
    mseg as (
        select *
        from RAW.SAP_DEV.MSEG
    ),

    final as (
            select
////////////////////////bstaus (this logic comes from the link of bstaus bsttyp)///////////////////
            case 
                when 
                           t156c.xlabst = 'X' 
                        or t156c.xklabs = 'X' 
                        or t156c.xmeikl = 'X'
                        or t156c.xmsprl = 'X'
                        or t156c.xmtvla = 'X'
                    then 'A'
                    
                when      
                           t156c.xinsme = 'X'
                        or t156c.xkinsm = 'X'
                        or t156c.xmeikq = 'X'
                        or t156c.xmsprq = 'X'
                        or t156c.xmtvqu = 'X'
                    then 'B'
                    
                when t156c.xretme = 'X'
                    then 'C'
                    
                when       
                           t156c.xspeme = 'X'
                        or t156c.xkspem = 'X'
                        or t156c.xmeiks = 'X'
                        or t156c.xmsprs = 'X'
                        or t156c.xmtvsp = 'X'
                    then 'D'
                    
                when      
                           t156c.xeinme = 'X'
                        or t156c.xkeinm = 'X'
                        or t156c.xmeike = 'X'
                        or t156c.xmspre = 'X'
                        or t156c.xmtvei = 'X'    
                    then 'E'
                    
                when t156c.xumlme = 'X' or t156c.xumlmc = 'X'
                    then 'F'

                when t156c.xmkubl = 'X'
                    then 'G'
                    
                when       t156c.xtrame = 'X'
                        or t156c.xsatra = 'X'
                        or t156c.xsqtra = 'X'
                    then 'H'
                    
                when t156c.xmklkk = 'X'
                    then 'K'

                when t156c.xmkqkk = 'X'
                    then 'L'

                when t156c.xmkekk = 'X'
                    then 'M'

                when t156c.xmklkl = 'X'
                    then 'N'

                when t156c.xmkqkl = 'X'
                    then 'O'

                when t156c.xmkekl = 'X'
                    then 'P'

                 when t156c.xmslbo = 'X'
                    then 'Q'
                    
                when t156c.xmsqbo = 'X'
                    then 'R'

                when t156c.xmsebo = 'X'
                    then 'S'

                when t156c.xglgmg = 'X'
                    then 'T'

                when t156c.xmslbu = 'X'
                    then 'U'

                when 
                       t156c.xbwesb = 'X' 
                    or t156c.xsabwe = 'X'
                    or t156c.xsqbwe = 'X'
                    then 'W'

                when t156c.xmkukk = 'X'
                    then 'X'
                    
                else 'V'
                    
            end as bstaus,
/////////////////////bsttyp (this logic comes from the link of bstaus bsttyp)//////////////////    
            case 
                when 
                       t156c.xglgmg = 'X'
                    or t156c.xlabst = 'X'
                    or t156c.xeinme = 'X'
                    or t156c.xinsme = 'X'
                    or t156c.xspeme = 'X'
                    or t156c.xumlme = 'X'
                    or t156c.xumlmc = 'X'
                    or t156c.xtrame = 'X'
                    or t156c.xmklkk = 'X'
                    or t156c.xmkqkk = 'X'
                    or t156c.xmkekk = 'X'
                    or t156c.xmklkl = 'X'
                    or t156c.xmkqkl = 'X'
                    or t156c.xmkekl = 'X'
                    or t156c.xmslbo = 'X'
                    or t156c.xmsqbo = 'X'
                    or t156c.xmsebo = 'X'
                    or t156c.xbwesb = 'X'
                    or t156c.xmslbu = 'X'
                    or t156c.xmkukk = 'X'
                then '-'
            
                when 
                       t156c.xklabs = 'X'
                    or t156c.xkinsm = 'X'
                    or t156c.xkspem = 'X'
                    or t156c.xkeinm = 'X'
                then 'K'

                when 
                       t156c.xmeikl = 'X'
                    or t156c.xmeikq = 'X'
                    or t156c.xmeiks = 'X'
                    or t156c.xmeike = 'X'
                    or t156c.xmkubl = 'X'
                    or t156c.xsatra = 'X'
                    or t156c.xsabwe = 'X'
                then 'E'

                when 
                       t156c.xmsprl = 'X'
                    or t156c.xmsprq = 'X'
                    or t156c.xmsprs = 'X'
                    or t156c.xmspre = 'X'
                    or t156c.xsqtra = 'X'
                    or t156c.xsqbwe = 'X'
                then 'Q'

                when t156c.xretme = 'X' then 'R'
                
                when 
                       t156c.xmtvla = 'X'
                    or t156c.xmtvqu = 'X'
                    or t156c.xmtvsp = 'X'
                    or t156c.xmtvei = 'X'
                then 'M'

                when t156c.xmeiks = 'X' then 'T'

                when 
                         mseg.wertu is null 
                     and mseg.mengu = 'X'
                     then 'U'
                
                else 'V'
            end as bsttyp,
 ////////////////////description (this logic comes from the link for bstaus bsttyp)///////////////////////////   
            case 
                when bstaus = 'T' and bsttyp = '-' then 'Tied Empties Stock'
                when bstaus = 'A' and bsttyp = '-' then 'Unrestricted'
                when bstaus = 'E' and bsttyp = '-' then 'Restricted-use stock'
                when bstaus = 'B' and bsttyp = '-' then 'In Qual. Inspection'
                when bstaus = 'D' and bsttyp = '-' then 'Blocked Stock'
                when bstaus = 'F' and bsttyp = '-' then 'Stck i.trnsf.(plant)'
                when bstaus = 'H' and bsttyp = '-' then 'Transit'
                when bstaus = 'K' and bsttyp = '-' then 'Consignmt at custom.'
                when bstaus = 'L' and bsttyp = '-' then 'Consigmt at cust. QI'
                when bstaus = 'M' and bsttyp = '-' then 'Restr. cons.at cust.'
                when bstaus = 'N' and bsttyp = '-' then 'Return.pack.at cust.'
                when bstaus = 'O' and bsttyp = '-' then 'Re.pck.at custom. QI'
                when bstaus = 'P' and bsttyp = '-' then 'Rstr.cust.ret.packng'
                when bstaus = 'Q' and bsttyp = '-' then 'Mat. prov. to Vendor'
                when bstaus = 'R' and bsttyp = '-' then 'Mat. prov.to vend QI'
                when bstaus = 'S' and bsttyp = '-' then 'Rstr.mat.prov.to ven'
                when bstaus = 'W' and bsttyp = '-' then 'Valuated Goods Receipt Blocked Stock'
                when bstaus = 'U' and bsttyp = '-' then 'SC Stock in Transfer'
                when bstaus = 'X' and bsttyp = '-' then 'Cust.Consig. Transf.'
                when bstaus = 'A' and bsttyp = 'K' then 'Unr-use consign.stck'
                when bstaus = 'B' and bsttyp = 'K' then 'Cons.qual.insp. stck'
                when bstaus = 'D' and bsttyp = 'K' then 'Blocked'
                when bstaus = 'E' and bsttyp = 'K' then 'Restr.-use consignmt'
                when bstaus = 'A' and bsttyp = 'E' then 'Sales order'
                when bstaus = 'B' and bsttyp = 'E' then 'Sales order'
                when bstaus = 'D' and bsttyp = 'E' then 'Blckd sales order (if MSEG-SOBKZ <> T)'
                when bstaus = 'E' and bsttyp = 'E' then 'Restr. sales order'
                when bstaus = 'G' and bsttyp = 'E' then 'Mat. Prov. By Cust.'
                when bstaus = 'H' and bsttyp = 'E' then 'Transit'
                when bstaus = 'A' and bsttyp = 'Q' then 'Project stock'
                when bstaus = 'B' and bsttyp = 'Q' then 'Project in QI'
                when bstaus = 'D' and bsttyp = 'Q' then 'Project blocked'
                when bstaus = 'E' and bsttyp = 'Q' then 'Restr. project stock'
                when bstaus = 'H' and bsttyp = 'Q' then 'Transit'
                when bstaus = 'C' and bsttyp = 'R' then 'Return'
                when bstaus = 'A' and bsttyp = 'M' then 'Unrestricted-use RTP'
                when bstaus = 'B' and bsttyp = 'M' then 'Qual. RTP'
                when bstaus = 'D' and bsttyp = 'M' then 'Blocked RTP'
                when bstaus = 'D' and bsttyp = 'T' then 'Blckd sales order ( BSTTYP = T if MSEG-SOBKZ = T; otherwise BSTTYP = E )'
                when bstaus = 'E' and bsttyp = 'M' then 'Restricted RTP'
                when bstaus = 'V' and bsttyp = 'V' then '-'
                else 'Non-Valuated Stock'
            end as description,
/////////////sta_veraenderung_setzen (documented in BSTAUS_BSTTYP.docx)//////////////////
            mseg.shkzg as shkzg_tuned,
            case 
                    when shkzg_tuned = 'S'
                    then 'plus'
                    else 'minus'                  
            end as cred_deb_ind,     
///////////////BWBREL (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)///////////////
            case 
                    when bstaus = 'V' and bsttyp = 'V' then 2
                    when mseg.matnr = mseg.matbf or mseg.matbf is null
                        then 1
                    else 2
            end as bwbrel,
/////////////////////conversion logic needed////////////////////
            case 
                when null then mseg.menge
                when mseg.meins = mseg.erfme
                then erfmg
                else 9999999999  --this is just to remember that we need to do currency conversion here
            end as hlp_menge,
//////////////////////BWAPPLNM///////////////////////////////////
            'MM' as bwapplnm,   --this is just to put the field in, so far i've seen that it might be RT as in Retail, 
                                --as for the field BWINDUS in structure MCBW_ZUSA. it also relates to table ROAPPL and TMCLBW
/////////////////rocancel (this one i got from looking at all the examples in the PSA. Hard coded)//////////////////////////
            case 
                when mseg.bwart in ('102', '162', '262', '642', '906') then 'X'
                else '-'
            end as rocancel,
////////////////////bwmng (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)//////////////////////////
            case 
                when rocancel = 'X'
                    then mseg.menge*-1
                when bwbrel = 1 then mseg.menge
                when hlp_menge is null
                    then mseg.menge
                else hlp_menge
            end as bwmng,
//////////////////bwgeo (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)////////////////
            case 
                when rocancel = 'X' then mseg.dmbtr*-1
                when bwbrel = 1 then mseg.dmbtr
                else mseg.dmbtr
            end as bwgeo,
/////////////, bwgvo, bwgvp --still not much about them (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)///////////
            mseg.vkwra as bwgvo,
            mseg.vkwrt as bwgvp,
////////////ZU_ABGANG (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx and BWVORG.pdf)//////////////
            case 
                when mseg.bwart = '122' and mseg.shkzg = 'S' then 1
                when mseg.bwart = '122' and mseg.shkzg = 'H' then -1
                when mseg.bwart = '123' and mseg.shkzg = 'S' then -1
                when mseg.bwart = '123' and mseg.shkzg = 'H' then 1
                when t156.xstbw is null and mseg.shkzg = 'S' then 1
                when t156.xstbw is null and mseg.shkzg = 'H' then -1
                when t156.xstbw is not null and mseg.shkzg = 'S' then -1
                when t156.xstbw is not null and mseg.shkzg = 'H' then 1
             end as departure_ind,                          --known as zu_abgang 
/////////////BWVORG (documented in BWVORG.pdf)////////////////
            case 
                when 
                        tmca.umlkz = 'X' 
                    and mseg.ummat <> mseg.matnr 
                    and departure_ind > 0
                    then '004'
                when 
                        tmca.umlkz = 'X' 
                    and mseg.ummat <> mseg.matnr 
                    and departure_ind < 0 
                    then '104'
                when 
                        tmca.umlkz = 'X' 
                    and mseg.umwrk <> mseg.werks 
                    and departure_ind > 0 
                    then '010'   
                when 
                        tmca.umlkz = 'X' 
                    and mseg.umwrk <> mseg.werks 
                    and departure_ind < 0 
                    then '110'                    
                when 
                        tmca.invkz = 'X'
                    and departure_ind > 0 
                    then '005'
                when 
                        tmca.invkz = 'X' 
                    and departure_ind < 0  
                    then '105'
                when 
                        tmca.korr = 'X'
                    and departure_ind > 0 
                    then '006'
                when 
                        tmca.korr = 'X' 
                    and departure_ind < 0  
                    then '106'
                when 
                        mseg.lifnr is not null 
                    and tmca.korr is null 
                    and tmca.invkz is null
                    and tmca.umlkz is null 
                    and departure_ind > 0
                    then '001'
                when 
                        mseg.lifnr is not null 
                    and tmca.korr is null 
                    and tmca.invkz is null
                    and tmca.umlkz is null 
                    and departure_ind < 0  
                    then '101'  
                when departure_ind > 0 
                    then '000'
                else '100'
            end as bwvorg,  
////////////////bwkey (same as WERKS)////////////////////////////
            mseg.werks as bwkey,
 //////////////XAUTO-CNT02 (documented in the BSTAUS_BSTTYP.docx)////////////////////// 
            case
                when mseg.xauto = 'X'
                then '02' else '01'
            end as T156M_counter,
            t156m.cnt02,
//////////////every time we have an F in mseg.zustd_t156m, we need to clear the flag and join to t156m.zustd/////
            case 
                when mseg.zustd_t156m = 'F'
                then ''
                else zustd_t156m 
            end as zustd_t156m_tuned,    
//////////////////other fields/////////////////////////////
             mseg.sobkz,
             t156c.xlabst,
             t156m.lbbsa,
             t156c.lbbsa,
             mseg.bustm, 
             t156m.bustm,
             mseg.zustd,
 /////////////////this bwcounter is for the lines, see that is is set 1 when there is only 1 line and to 2 when there are 2/////////
             case 
                when t156m.kbbsa is not null
                then 2
                else 1
             end as bwcounter,
//////////////other fields///////////////////////////////////
             'K1' as periv,             
             mseg.*     
            
            FROM mseg
        
        left join t156m
            on zustd_t156m_tuned = coalesce(t156m.zustd, '')
            and mseg.BUSTM = t156m.BUSTM
            and T156M_counter = t156m.cnt02
            
        left join t156c
            on coalesce(mseg.sobkz, '') = coalesce(t156c.sobkz, '')
            and t156m.LBBSA = t156c.LBBSA
            
        left join t156 
            on t156.bwart = mseg.bwart

        left join tmca    
            on mseg.bwart = tmca.bwart
            and coalesce(mseg.sobkz, '') = coalesce(tmca.sobkz, '')
            and coalesce(mseg.kzbew, '') = coalesce(tmca.kzbew, '')   

        left join mkpf
            on mseg.mjahr = mkpf.mjahr
            and mseg.mblnr = mkpf.mblnr 
    ),

    
        final_2 as (
            SELECT
////////////////////////bstaus (this logic comes from the link for bstaus bsttyp)///////////////////
            case 
                when 
                           t156c.xlabst = 'X' 
                        or t156c.xklabs = 'X' 
                        or t156c.xmeikl = 'X'
                        or t156c.xmsprl = 'X'
                        or t156c.xmtvla = 'X'
                    then 'A'
                    
                when      
                           t156c.xinsme = 'X'
                        or t156c.xkinsm = 'X'
                        or t156c.xmeikq = 'X'
                        or t156c.xmsprq = 'X'
                        or t156c.xmtvqu = 'X'
                    then 'B'
                    
                when t156c.xretme = 'X'
                    then 'C'
                    
                when       
                           t156c.xspeme = 'X'
                        or t156c.xkspem = 'X'
                        or t156c.xmeiks = 'X'
                        or t156c.xmsprs = 'X'
                        or t156c.xmtvsp = 'X'
                    then 'D'
                    
                when      
                           t156c.xeinme = 'X'
                        or t156c.xkeinm = 'X'
                        or t156c.xmeike = 'X'
                        or t156c.xmspre = 'X'
                        or t156c.xmtvei = 'X'    
                    then 'E'
                    
                when t156c.xumlme = 'X' or t156c.xumlmc = 'X'
                    then 'F'

                when t156c.xmkubl = 'X'
                    then 'G'
                    
                when       t156c.xtrame = 'X'
                        or t156c.xsatra = 'X'
                        or t156c.xsqtra = 'X'
                    then 'H'
                    
                when t156c.xmklkk = 'X'
                    then 'K'

                when t156c.xmkqkk = 'X'
                    then 'L'

                when t156c.xmkekk = 'X'
                    then 'M'

                when t156c.xmklkl = 'X'
                    then 'N'

                when t156c.xmkqkl = 'X'
                    then 'O'

                when t156c.xmkekl = 'X'
                    then 'P'

                 when t156c.xmslbo = 'X'
                    then 'Q'
                    
                when t156c.xmsqbo = 'X'
                    then 'R'

                when t156c.xmsebo = 'X'
                    then 'S'

                when t156c.xglgmg = 'X'
                    then 'T'

                when t156c.xmslbu = 'X'
                    then 'U'

                when 
                       t156c.xbwesb = 'X' 
                    or t156c.xsabwe = 'X'
                    or t156c.xsqbwe = 'X'
                    then 'W'

                when t156c.xmkukk = 'X'
                    then 'X'
                    
                else 'V'
                    
            end as bstaus,
/////////////////////bsttyp (this logic comes from the link)//////////////////     
            case 
                when 
                       t156c.xglgmg = 'X'
                    or t156c.xlabst = 'X'
                    or t156c.xeinme = 'X'
                    or t156c.xinsme = 'X'
                    or t156c.xspeme = 'X'
                    or t156c.xumlme = 'X'
                    or t156c.xumlmc = 'X'
                    or t156c.xtrame = 'X'
                    or t156c.xmklkk = 'X'
                    or t156c.xmkqkk = 'X'
                    or t156c.xmkekk = 'X'
                    or t156c.xmklkl = 'X'
                    or t156c.xmkqkl = 'X'
                    or t156c.xmkekl = 'X'
                    or t156c.xmslbo = 'X'
                    or t156c.xmsqbo = 'X'
                    or t156c.xmsebo = 'X'
                    or t156c.xbwesb = 'X'
                    or t156c.xmslbu = 'X'
                    or t156c.xmkukk = 'X'
                then '-'
            
                when 
                       t156c.xklabs = 'X'
                    or t156c.xkinsm = 'X'
                    or t156c.xkspem = 'X'
                    or t156c.xkeinm = 'X'
                then 'K'

                when 
                       t156c.xmeikl = 'X'
                    or t156c.xmeikq = 'X'
                    or t156c.xmeiks = 'X'
                    or t156c.xmeike = 'X'
                    or t156c.xmkubl = 'X'
                    or t156c.xsatra = 'X'
                    or t156c.xsabwe = 'X'
                then 'E'

                when 
                       t156c.xmsprl = 'X'
                    or t156c.xmsprq = 'X'
                    or t156c.xmsprs = 'X'
                    or t156c.xmspre = 'X'
                    or t156c.xsqtra = 'X'
                    or t156c.xsqbwe = 'X'
                then 'Q'

                when t156c.xretme = 'X' then 'R'
                
                when 
                       t156c.xmtvla = 'X'
                    or t156c.xmtvqu = 'X'
                    or t156c.xmtvsp = 'X'
                    or t156c.xmtvei = 'X'
                then 'M'

                when t156c.xmeiks = 'X' then 'T'

                when 
                         mseg.wertu is null 
                     and mseg.mengu = 'X'
                     then 'U'
                
                else 'V'
            end as bsttyp,
////////////////////description (this logic comes from the link)///////////////////////////    
            case 
                when bstaus = 'T' and bsttyp = '-' then 'Tied Empties Stock'
                when bstaus = 'A' and bsttyp = '-' then 'Unrestricted'
                when bstaus = 'E' and bsttyp = '-' then 'Restricted-use stock'
                when bstaus = 'B' and bsttyp = '-' then 'In Qual. Inspection'
                when bstaus = 'D' and bsttyp = '-' then 'Blocked Stock'
                when bstaus = 'F' and bsttyp = '-' then 'Stck i.trnsf.(plant)'
                when bstaus = 'H' and bsttyp = '-' then 'Transit'
                when bstaus = 'K' and bsttyp = '-' then 'Consignmt at custom.'
                when bstaus = 'L' and bsttyp = '-' then 'Consigmt at cust. QI'
                when bstaus = 'M' and bsttyp = '-' then 'Restr. cons.at cust.'
                when bstaus = 'N' and bsttyp = '-' then 'Return.pack.at cust.'
                when bstaus = 'O' and bsttyp = '-' then 'Re.pck.at custom. QI'
                when bstaus = 'P' and bsttyp = '-' then 'Rstr.cust.ret.packng'
                when bstaus = 'Q' and bsttyp = '-' then 'Mat. prov. to Vendor'
                when bstaus = 'R' and bsttyp = '-' then 'Mat. prov.to vend QI'
                when bstaus = 'S' and bsttyp = '-' then 'Rstr.mat.prov.to ven'
                when bstaus = 'W' and bsttyp = '-' then 'Valuated Goods Receipt Blocked Stock'
                when bstaus = 'U' and bsttyp = '-' then 'SC Stock in Transfer'
                when bstaus = 'X' and bsttyp = '-' then 'Cust.Consig. Transf.'
                when bstaus = 'A' and bsttyp = 'K' then 'Unr-use consign.stck'
                when bstaus = 'B' and bsttyp = 'K' then 'Cons.qual.insp. stck'
                when bstaus = 'D' and bsttyp = 'K' then 'Blocked'
                when bstaus = 'E' and bsttyp = 'K' then 'Restr.-use consignmt'
                when bstaus = 'A' and bsttyp = 'E' then 'Sales order'
                when bstaus = 'B' and bsttyp = 'E' then 'Sales order'
                when bstaus = 'D' and bsttyp = 'E' then 'Blckd sales order (if MSEG-SOBKZ <> T)'
                when bstaus = 'E' and bsttyp = 'E' then 'Restr. sales order'
                when bstaus = 'G' and bsttyp = 'E' then 'Mat. Prov. By Cust.'
                when bstaus = 'H' and bsttyp = 'E' then 'Transit'
                when bstaus = 'A' and bsttyp = 'Q' then 'Project stock'
                when bstaus = 'B' and bsttyp = 'Q' then 'Project in QI'
                when bstaus = 'D' and bsttyp = 'Q' then 'Project blocked'
                when bstaus = 'E' and bsttyp = 'Q' then 'Restr. project stock'
                when bstaus = 'H' and bsttyp = 'Q' then 'Transit'
                when bstaus = 'C' and bsttyp = 'R' then 'Return'
                when bstaus = 'A' and bsttyp = 'M' then 'Unrestricted-use RTP'
                when bstaus = 'B' and bsttyp = 'M' then 'Qual. RTP'
                when bstaus = 'D' and bsttyp = 'M' then 'Blocked RTP'
                when bstaus = 'D' and bsttyp = 'T' then 'Blckd sales order ( BSTTYP = T if MSEG-SOBKZ = T; otherwise BSTTYP = E )'
                when bstaus = 'E' and bsttyp = 'M' then 'Restricted RTP'
                when bstaus = 'V' and bsttyp = 'V' then '-'
                else 'Non-Valuated Stock'
            end as description,
/////////////this is a hard-coded part to reverse the debit credit indicator (see everything has been inverted to make the second line happen)////////            
            case 
                when mseg.shkzg = 'S'
                        then 'H'
                when mseg.shkzg = 'H'
                        then 'S'
            end as shkzg_tuned,
            
            case 
                when shkzg_tuned = 'H'
                then 'plus'
                else 'minus'                  
            end as cred_deb_ind,     
///////////////BWBREL (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)///////////////
            case 
                    when bstaus = 'V' and bsttyp = 'V' then 2
                    when mseg.matnr = mseg.matbf or mseg.matbf is null
                        then 1
                    else 2
            end as bwbrel,
/////////////////////conversion logic needed////////////////////
            case 
                when null then mseg.menge
                when mseg.meins = mseg.erfme
                then erfmg
                else 9999999999  --this is just to remember that we need to do currency conversion here
            end as hlp_menge,
////////////////BWAPPLNM ////////////////////////
            'MM' as bwapplnm, --this is just to put the field in, so far i've seen that it might be RT
                            --as in Retail, as for the field BWINDUS in structure MCBW_ZUSA. it also relates to table ROAPPL and TMCLBW
/////////////////rocancel (this one i got from looking at all the examples in the PSA. Hard coded)//////////////////////////
            case 
                when mseg.bwart in ('102', '162', '262', '642', '906') then 'X'
                else '-'
            end as rocancel,
////////////////////bwmng (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)///////////////
            case 
                when rocancel = 'X'
                    then mseg.menge*-1
                when bwbrel = 1 then mseg.menge
                when hlp_menge is null
                    then mseg.menge
                else hlp_menge
            end as bwmng,
//////////////////bwgeo (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)////////////////
            case 
                when rocancel = 'X' then mseg.dmbtr*-1
                when bwbrel = 1 then mseg.dmbtr
                else mseg.dmbtr
            end as bwgeo,
/////////////, bwgvo, bwgvp --still not much about them (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx)///////////
            mseg.vkwra as bwgvo,
            mseg.vkwrt as bwgvp,
////////////ZU_ABGANG (documented in BWVORG / BWBREL / BWMNG / BWGEO / BWGVO / BWGVP.docx and BWVORG.pdf)//////////////
            case 
                when mseg.bwart = '122' and shkzg_tuned = 'S' then 1
                when mseg.bwart = '122' and shkzg_tuned = 'H' then -1
                when mseg.bwart = '123' and shkzg_tuned = 'S' then -1
                when mseg.bwart = '123' and shkzg_tuned = 'H' then 1
                when t156.xstbw is null and shkzg_tuned = 'S' then 1
                when t156.xstbw is null and shkzg_tuned = 'H' then -1
                when t156.xstbw is not null and shkzg_tuned = 'S' then -1
                when t156.xstbw is not null and shkzg_tuned = 'H' then 1
             end as departure_ind,                          --known as zu_abgang 
/////////////BWVORG (documented in BWVORG.pdf)////////////////
            case 
                when 
                        tmca.umlkz = 'X' 
                    and mseg.ummat <> mseg.matnr 
                    and departure_ind > 0
                    then '004'
                when 
                        tmca.umlkz = 'X' 
                    and mseg.ummat <> mseg.matnr 
                    and departure_ind < 0 
                    then '104'
                when 
                        tmca.umlkz = 'X' 
                    and mseg.umwrk <> mseg.werks 
                    and departure_ind > 0 
                    then '010'   
                when 
                        tmca.umlkz = 'X' 
                    and mseg.umwrk <> mseg.werks 
                    and departure_ind < 0 
                    then '110'                    
                when 
                        tmca.invkz = 'X'
                    and departure_ind > 0 
                    then '005'
                when 
                        tmca.invkz = 'X' 
                    and departure_ind < 0  
                    then '105'
                when 
                        tmca.korr = 'X'
                    and departure_ind > 0 
                    then '006'
                when 
                        tmca.korr = 'X' 
                    and departure_ind < 0  
                    then '106'
                when 
                        mseg.lifnr is not null 
                    and tmca.korr is null 
                    and tmca.invkz is null
                    and tmca.umlkz is null 
                    and departure_ind > 0
                    then '001'
                when 
                        mseg.lifnr is not null 
                    and tmca.korr is null 
                    and tmca.invkz is null
                    and tmca.umlkz is null 
                    and departure_ind < 0  
                    then '101'  
                when departure_ind > 0 
                    then '000'
                else '100'
            end as bwvorg,  
////////////////bwkey (Werks)////////////////////////////
            mseg.werks as bwkey,
 //////////////XAUTO-CNT02 (documented in the BSTAUS_BSTTYP.docx)//////////////////////  
            case
                when mseg.xauto = 'X'
                then '02' else '01'
                end as T156M_counter,
            t156m.cnt02,
//////////////every time we have an F in mseg.zustd_t156m, we need to clear the flag and join to t156m.zustd/////
            case 
                when mseg.zustd_t156m = 'F'
                then ''
                else zustd_t156m 
            end as zustd_t156m_tuned,
///////////////////other fields/////////////////////////////////////
             mseg.sobkz,
             t156c.xlabst,
             t156m.lbbsa,
             t156c.lbbsa,
             mseg.bustm, 
             t156m.bustm,
             mseg.zustd,
 /////////////////this bwcounter is for the lines, see that is is set 1 and the one above is set to 2 when that happens to match the PSA resulto/////////
             1 as bwcounter,
             'K1' as periv,
             mseg.*

            FROM mseg
    
        left join t156m
            on zustd_t156m_tuned = coalesce(t156m.zustd, '')
            and mseg.BUSTM = t156m.BUSTM
            and T156M_counter = t156m.cnt02

        left join t156c 
            on coalesce(mseg.sobkz, '') = coalesce(t156c.sobkz, '')
            and t156m.KBBSA = t156c.LBBSA
            
        left join t156 
            on t156.bwart = mseg.bwart

        left join tmca    
            on mseg.bwart = tmca.bwart
            and coalesce(mseg.sobkz, '') = coalesce(tmca.sobkz, '')
            and coalesce(mseg.kzbew, '') = coalesce(tmca.kzbew, '')    

        left join mkpf
            on mseg.mjahr = mkpf.mjahr
            and mseg.mblnr = mkpf.mblnr

        where t156m.kbbsa is not null
    )

select 
    bwcounter, matnr, mjahr, BWVORG, shkzg_tuned, bwgeo, bwgvo, bwgvp, bwkey, bwbrel, bwmng, mblnr, werks, kostl, kzbew, bsttyp, bstaus, dmbtr, cred_deb_ind, 
    shkzg, lgort, menge, hlp_menge, bwart, rocancel, bwapplnm, waers, meins, bwtar

from (
        select * 
        from final_2
            union all 
        select * 
        from final
     );
