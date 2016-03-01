#!/bin/csh -f
#set up search path for "source" command to follow
alias source 'source `source.path \!\!:1`'
#set up database connect string
setenv MMIS_PSWD `grep -i MMIS_PSWD $AIM_CONFIG|awk -F= 'NR == 1 {print $2}'`

#####################################################################
#
#       Job Script:     updtRemitXref   
#
#       Decription:   Update t_fin_remit_txn_xref
#
#       Change Log:
#
#       Change date Name             Description
#       ----------- --------------   --------------------
#       05/19/2010  R. N.            Initial Creation
#       05/19/2010  Cesar Lopez      Modify to use Bulk bind
#
#####################################################################
source $SRCDIR/job_begin.src

if ($restart_step != "") then
        goto $restart_step
endif

#####################################################################
#
#    js010 - Update records in T_FIN_REMIT_TXN_XREF 
#
#####################################################################
js010:
setenv  JS      "js010"
jsbeg_msg.csh

##############################################################
# Input Files
##############################################################
#None

##############################################################
# Output Files
##############################################################
setenv dd_LOG "$DATADIR/updt_sql.log"
rm -f $dd_LOG

source $SRCDIR/override.src

##############################################################
# Execute Program
##############################################################
echo "Executing: sqlplus..."

sqlplus /nolog  <<!
 CONNECT $MMIS_PSWD
 WHENEVER SQLERROR EXIT -1 ROLLBACK;

 SET SERVEROUTPUT ON
 SPOOL $dd_LOG

 DECLARE

  CURSOR my_cursor IS
      select FRX.ROWID,
             H.cde_clm_type
        from t_fin_remit_txn_xref_TEST FRX,
             t_hist_directory     H 
       where frx.cde_txn = 'C'
         and frx.sak_txn = h.sak_claim;


    TYPE t_char_array   IS TABLE OF CHAR  INDEX BY BINARY_INTEGER;
    TYPE t_rowid_array  IS TABLE OF ROWID INDEX BY BINARY_INTEGER;
    
    
    v_rowids         t_rowid_array;
    v_cde_clm_types   t_char_array;
    
    v_bulk_rows        NUMBER := 500;

    cnt                NUMBER := 0;
    commit_int         NUMBER := 0;
    err_msg            VARCHAR2(100);

BEGIN

  dbms_output.put_line('Starting');

  OPEN my_cursor;
  LOOP
    FETCH my_cursor
    BULK COLLECT INTO v_rowids, v_cde_clm_types
    LIMIT v_bulk_rows;
    
        
    EXIT WHEN v_rowids.count < 1;
    
    dbms_output.put_line('Inside: ' || TO_CHAR(v_rowids.count));
    
    
    
    -- bulk bind
    FORALL i IN 1 .. v_rowids.count
      UPDATE  t_fin_remit_txn_xref_TEST A  
        SET   A.CDE_TXN   = v_cde_clm_types(i) 
      WHERE   A.ROWID = v_rowids(i);
    
    cnt := cnt + v_rowids.count;
    commit_int := commit_int + v_rowids.count;
    
    dbms_output.put_line(TO_CHAR(commit_int) || ' rows committed' );
    COMMIT;
    commit_int := 0;
        
    dbms_output.put_line(' > > ' );
    dbms_output.put_line(TO_CHAR(cnt) || ' rows selected' );
   
    EXIT WHEN v_rowids.count < v_bulk_rows;
    
  END LOOP;
  
  
  CLOSE my_cursor;

END;

.
/
 COMMIT;
 SPOOL OFF
 EXIT;
!

# Check for processing errors
if ($status != 0) then
   cat $dd_LOG
   exit (-1)
endif

# Check for connection errors
setenv ORA_ERR `egrep -sc "^SP2|^ORA" $dd_LOG`

if ($ORA_ERR != 0) then
   echo "ERROR in executing sqlplus"
   cat $dd_LOG
   exit (-1)
endif

#####################################################################
# END OF JOB
#####################################################################
eoj_msg.csh $0

