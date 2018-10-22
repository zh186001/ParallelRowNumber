#define DEBUG_FLAG 0
#define byte unsigned char
#define boolean int
#define FALSE 0
#define TRUE 1
#define OK 0
#define SQL_TEXT Latin_Text
#define SetError(e, m) strcpy((char *)sqlstate, (e)); strcpy((char *)error_message, (m))
#define BUFFER_SIZE (32*1024)

#define _CRT_SECURE_NO_DEPRECATE
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqltypes_td.h>

#define BUFFER_SIZE (32*1024)

int ParallelRowNumber_contract(
 INTEGER *Result,
 int *indicator_Result,
 char sqlstate[6],
 SQL_TEXT extname[129],
 SQL_TEXT specific_name[129],
 SQL_TEXT error_message[257])
{
 int iCount, oCount;
 FNC_TblOpGetStreamCount(&iCount, &oCount);
 if(iCount + oCount != 3)
 {
  SetError("U0003", "Must have one input, the RowsPerAMP DIMENSION and one output");
  return -1;
 }

 int iColumnCount = FNC_TblOpGetColCount(0, ISINPUT);
 FNC_TblOpColumnDef_t *iCols = FNC_malloc(TblOpSIZECOLDEF(iColumnCount));
 TblOpINITCOLDEF(iCols, iColumnCount);
 FNC_TblOpGetColDef(0, ISINPUT, iCols);

 /* Allocate space for columns. */
 int oColumnCount = iColumnCount + 1;
 if (DEBUG_FLAG)
  oColumnCount += 2;
 FNC_TblOpColumnDef_t *oCols = FNC_malloc(TblOpSIZECOLDEF(oColumnCount));
 memset(oCols, 0 , TblOpSIZECOLDEF(oColumnCount));
 oCols->num_columns = oColumnCount;
 oCols->length = TblOpSIZECOLDEF(oColumnCount) - (2 * sizeof(int)) ;
 TblOpINITCOLDEF(oCols, oColumnCount);

 /* Get base info */
 //UDT_BaseInfo_t *baseInfos = (UDT_BaseInfo_t *)FNC_malloc(iCols->num_columns * sizeof(UDT_BaseInfo_t));
 //FNC_TblOpGetBaseInfo(iCols, baseInfos);

 /* Copy input columns to output columns. */
 oColumnCount = 0;
 strcpy(oCols->column_types[oColumnCount].column, "ROW_NUM");
 oCols->column_types[oColumnCount].size.length = sizeof(long long);
 oCols->column_types[oColumnCount].charset = LATIN_CT;
 oCols->column_types[oColumnCount].datatype = BIGINT_DT;
 oCols->column_types[oColumnCount].bytesize = SIZEOF_BIGINT;
 if (DEBUG_FLAG) {
  oColumnCount++;
  strcpy(oCols->column_types[oColumnCount].column, "VPROC");
  oCols->column_types[oColumnCount].size.length = sizeof(unsigned int);
  oCols->column_types[oColumnCount].charset = LATIN_CT;
  oCols->column_types[oColumnCount].datatype = SMALLINT_DT;
  oCols->column_types[oColumnCount].bytesize = SIZEOF_SMALLINT;
  
  oColumnCount++;
  strcpy(oCols->column_types[oColumnCount].column, "VPROC_START");
  oCols->column_types[oColumnCount].size.length = sizeof(long long);
  oCols->column_types[oColumnCount].charset = LATIN_CT;
  oCols->column_types[oColumnCount].datatype = BIGINT_DT;
  oCols->column_types[oColumnCount].bytesize = SIZEOF_BIGINT;
 }
 oColumnCount++;
 int i = 0;
 for(; i < iCols->num_columns; i++)
 {
  oCols->column_types[oColumnCount] = iCols->column_types[i];
  oColumnCount++;
 }
 //FNC_free(baseInfos);

 FNC_TblOpSetOutputColDef(0, oCols);
 Stream_Fmt_en format = INDICFMT1;
 FNC_TblOpSetFormat("RECFMT", 0, ISINPUT, &format, sizeof(format));
 FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));

 FNC_free(iCols);
 FNC_free(oCols);
 *Result = 1;
}

typedef struct {
 unsigned short AmpId;
 long long RowCount;
} RowsPerAMP;

int CompareAMPIds(const void *a, const void *b)
{
    RowsPerAMP *amp_a = (RowsPerAMP*)a;
    RowsPerAMP *amp_b = (RowsPerAMP*)b;

    return (amp_a->AmpId - amp_b->AmpId);
}

RowsPerAMP *ReadRowCounts(int ampCount)
{
 BYTE *ptr;
 int null_ind, length;

 //check that second input is a DIMENSION
 FNC_TblOpHandle_t *iHandle = FNC_TblOpOpen(1, ISINPUT, 0);

 RowsPerAMP *rowsPerAMP = FNC_malloc(sizeof(RowsPerAMP) * ampCount);
 int i = 0;
 while(TRUE)
 {
  if (FNC_TblOpRead(iHandle) == TBLOP_EOF) break;

  FNC_TblOpGetAttributeByNdx(iHandle, 0, (void**)&ptr, &null_ind, &length);
  rowsPerAMP[i].AmpId = *((unsigned short *)ptr);
  FNC_TblOpGetAttributeByNdx(iHandle, 1, (void**)&ptr, &null_ind, &length);
  rowsPerAMP[i].RowCount = *((long long *)ptr);
  i++;
 }

 qsort(rowsPerAMP, ampCount, sizeof(RowsPerAMP), CompareAMPIds);
 return rowsPerAMP;
}

void ParallelRowNumber()
{
 int iCount, oCount;
 FNC_TblOpGetStreamCount(&iCount, &oCount);

 Vproc_Info_t *vprocInfo = FNC_Where_Am_I();
 unsigned short myAMP = vprocInfo->VprocId;

 FNC_Node_Info_t *nodeInfo = FNC_TblGetNodeData();
 int ampCount = nodeInfo->NumAMPs;
 RowsPerAMP *rowsPerAMP = ReadRowCounts(ampCount); 

 long long initRowNumber = 0;
 unsigned short i = 0;
 for (; i < myAMP; i++)
  initRowNumber += rowsPerAMP[i].RowCount;

 int iColumnCount = FNC_TblOpGetColCount(0, ISINPUT);
 FNC_TblOpColumnDef_t *iCols = FNC_malloc(TblOpSIZECOLDEF(iColumnCount));
 TblOpINITCOLDEF(iCols, iColumnCount);
 FNC_TblOpGetColDef(0, ISINPUT, iCols);

 /* Get Handles */
 FNC_TblOpHandle_t *iHandle = FNC_TblOpOpen(0, ISINPUT, 0);
 FNC_TblOpHandle_t *oHandle = FNC_TblOpOpen(0, ISOUTPUT, 0);

 long long rowNumber = initRowNumber;
 while(TRUE)
 {
  if (FNC_TblOpRead(iHandle) == TBLOP_EOF) break;
  rowNumber++;

  int oColumnCount = 0;
  FNC_TblOpBindAttributeByNdx(oHandle, oColumnCount, &rowNumber, 0, sizeof(long long));
  if (DEBUG_FLAG) {
   oColumnCount++;
   FNC_TblOpBindAttributeByNdx(oHandle, oColumnCount, &myAMP, 0, sizeof(unsigned short));
   oColumnCount++;
   FNC_TblOpBindAttributeByNdx(oHandle, oColumnCount, &initRowNumber, 0, sizeof(long long));
  }
  oColumnCount++;
  int i = 0;
  for (; i < iCols->num_columns; i++)
  {
   switch (iCols->column_types[i].datatype)
   {
    case CLOB_REFERENCE_DT:
    case BLOB_REFERENCE_DT:
    case JSON_DT:
	/*
	{ 
     LOB_RESULT_LOCATOR lrl_a;
	 LOB_CONTEXT_ID lobId;
     FNC_LobLength_t actLen;
     int truncError;
     BYTE buffer[BUFFER_SIZE];

     lrl_a = FNC_LobCol2Loc(0, oColumnCount);

     if (!(TBLOPISNULL(iHandle->row->indicators,i)))
     {
      FNC_LobOpen_CL(iHandle->row->columnptr[i], &lobId, 0, 0);
			 
      int truncError = 0;
      while (FNC_LobRead(lobId, buffer, BUFFER_SIZE, &actLen) == 0 && !truncError)
       truncError=FNC_LobAppend(lrl_a, buffer, actLen, &actLen);
      FNC_LobClose(lobId);
     }
	}
    */
    break;

    default:
     oHandle->row->columnptr[oColumnCount] = iHandle->row->columnptr[i];
     oHandle->row->lengths[oColumnCount] = iHandle->row->lengths[i];
	 break;
   }

   if (TBLOPISNULL(iHandle->row->indicators,i))
    TBLOPSETNULL(oHandle->row->indicators,oColumnCount); 
   oColumnCount++;
  }
  FNC_TblOpWrite(oHandle); // Writes current output row
 }

 FNC_free(iCols);
 FNC_free(rowsPerAMP);
 FNC_TblOpClose(iHandle); // close all contexts
 FNC_TblOpClose(oHandle); // close all contexts
}
