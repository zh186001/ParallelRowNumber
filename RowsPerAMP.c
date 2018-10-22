#define byte unsigned char
#define boolean int
#define FALSE 0
#define TRUE 1
#define OK 0
#define SQL_TEXT Latin_Text
#define SetError(e, m) strcpy((char *)sqlstate, (e)); strcpy((char *)error_message, (m))

#define _CRT_SECURE_NO_DEPRECATE
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqltypes_td.h>

#define BufferSize (32*1024)

int RowsPerAMP_contract(
 INTEGER *Result,
 int *indicator_Result,
 char sqlstate[6],
 SQL_TEXT extname[129],
 SQL_TEXT specific_name[129],
 SQL_TEXT error_message[257])
{
 int iCount, oCount;
 FNC_TblOpGetStreamCount(&iCount, &oCount);
 if(iCount + oCount != 2)
 {
  SetError("U0003", "Must have one input and one output");
  return -1;
 }

 /* Allocate space for columns. */
 int oColumnCount = 2;
 FNC_TblOpColumnDef_t *oCols = FNC_malloc(TblOpSIZECOLDEF(oColumnCount));
 memset(oCols, 0 , TblOpSIZECOLDEF(oColumnCount));
 oCols->num_columns = oColumnCount;
 oCols->length = TblOpSIZECOLDEF(oColumnCount) - (2 * sizeof(int)) ;
 TblOpINITCOLDEF(oCols, oColumnCount);

 oCols->column_types[0].size.length = sizeof(unsigned short);
 oCols->column_types[0].charset = LATIN_CT;
 oCols->column_types[0].datatype = SMALLINT_DT;
 oCols->column_types[0].bytesize = SIZEOF_SMALLINT;

 oCols->column_types[1].size.length = sizeof(long long);
 oCols->column_types[1].charset = LATIN_CT;
 oCols->column_types[1].datatype = BIGINT_DT;
 oCols->column_types[1].bytesize = SIZEOF_BIGINT;

 FNC_TblOpSetOutputColDef(0, oCols);
 //Stream_Fmt_en format = INDICFMT1;
 //FNC_TblOpSetFormat("RECFMT", 0, ISINPUT, &format, sizeof(format));
 //FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));

 FNC_free(oCols);
 *Result = 1;
}

void RowsPerAMP()
{
 int iCount, oCount;
 FNC_TblOpGetStreamCount(&iCount, &oCount);
 
 Vproc_Info_t *vprocInfo = FNC_Where_Am_I();
 int ampId = vprocInfo->VprocId;

 /* Get Handles */
 FNC_TblOpHandle_t *iHandle = FNC_TblOpOpen(0, ISINPUT, 0);
 FNC_TblOpHandle_t *oHandle = FNC_TblOpOpen(0, ISOUTPUT, 0);

 long long n = 0;
 while(TRUE)
 {
  if (FNC_TblOpRead(iHandle) == TBLOP_EOF) break;
  n++;
 }

 int oColumnCount = 0;
 FNC_TblOpBindAttributeByNdx(oHandle, 0, &ampId, 0, sizeof(unsigned short));
 FNC_TblOpBindAttributeByNdx(oHandle, 1, &n, 0, sizeof(long long));

 FNC_TblOpWrite(oHandle); // Writes current output row

 FNC_TblOpClose(iHandle); // close all contexts
 FNC_TblOpClose(oHandle); // close all contexts
}
