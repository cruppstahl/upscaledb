#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "ham/hamsterdb.h"
#include "time.h"

#define LOOP 100

#define DBNAME_MILKCOLLECT  1
#define DBNAME_DATESHIFT  2
#define DBNAME_MEMBER  3

#define MONTH			12
#define DAYS			30
#define SHIFT			2
#define CUSTOMERS		500

typedef struct 
{
	char DateShiftId;
	long MilkColId;
    char  Date[11];
    char Shift;
    unsigned int MemberCode;
    char SociCode[6];
    long int SampleNo;
	char MilkType;    
    float Qty;
    float ActQty;	
    float Qtytype;
    float Fat;
    float ActFat;	
    float FatLr;
    float ActFatLr;	
    float Snf;
    float ActSnf;
    float Solid;
    float ActSolid;
	float FatKg;
	float ActFatKg;
	float SnfKg;
	float ActSnfKg;
	float Rate;
    float Amount;
    float ActAmount;	
	unsigned int CanNo;
	char MembCodeAuto;
	char WeightAuto;
	char FatAuto;
	char LrClrAuto;
	char SnfAuto;
	char EntryMode;
} __attribute__((__packed__)) MilkCollect ;


typedef struct 
{
	char DateShiftId;
    char Date[11];
    char Shift;
    char SociCode[6];
	long MilkColId;
} __attribute__((__packed__)) DateShift ;

typedef struct
{
	float Qty;
	float Fat;
	float Snf;
	float Clr;
	float Solid;
	float Amount;
} __attribute__((__packed__)) MC_Data ;

typedef struct 
{
    unsigned int MemberCode;
    char Shift;
    char SociCode[6];
	char MilkType;    
	MC_Data MCData[30];
} __attribute__((__packed__)) Memb ;


DateShift DS;
MilkCollect MilkCol;
DateShift *ptDS;
Memb Member;
Memb *ptMemb;

ham_db_t *db,*db1,*db2;  /* hamsterdb database objects */
ham_env_t *env;         /* hamsterdb environment */
ham_cursor_t *cursor,*cursor1,*cursor2; /* a cursor for each database */
ham_key_t member_key, date_key;
ham_record_t member_record, date_record;
ham_status_t st;        /* status variable */

MilkCollect *a;
float Quantity=0;
float Amnt=0;

char str_date[11];	
char key_str[30];
char filename[12];	

static void errhandler(int level, const char *message);
long counter;

void DataInsert(void);
void PurchaseRegister(void);

int main(void)
{
    register unsigned char i,j,k;
	register int	l;

	ham_parameter_t params_create[]={
		{HAM_PARAM_PAGESIZE, 4096},
		{HAM_PARAM_CACHESIZE, 1024*30},
		{0,0}
	};

	ham_parameter_t params_create_db[]={
		{HAM_PARAM_KEYSIZE, 2},
		{0,0}
	};

	ham_parameter_t params_create_db1[]={
		{HAM_PARAM_KEYSIZE, 2},
		{0,0}
	};

	ham_parameter_t params_create_db2[]={
		{HAM_PARAM_KEYSIZE, 4},
		{0,0}
	};

	memset(&MilkCol,0,sizeof(MilkCol));
	memset(&MilkCol.MemberCode,0,sizeof(MilkCol.MemberCode));

	ham_set_errhandler(errhandler);

	strcpy(MilkCol.SociCode,"12345");
	MilkCol.MilkColId=0;
	MilkCol.MilkType = 'B';    
//	MilkCol.Qty = 1.00;
	MilkCol.ActQty = 0.0;	
	MilkCol.Qtytype = 'L';
//	MilkCol.Fat = 12.5;
	MilkCol.ActFat = 0.0;	
	MilkCol.FatLr = 10.5;
	MilkCol.ActFatLr = 0.0;
	MilkCol.Snf = 0.0;
	MilkCol.ActSnf = 0.0;
	MilkCol.Solid = 0.0;
	MilkCol.ActSolid = 0.0;
	MilkCol.FatKg = 15.5;
	MilkCol.ActFatKg = 0.0;
	MilkCol.SnfKg = 12.5;
	MilkCol.ActSnfKg = 12.5;
	MilkCol.Rate = 25.5;
	MilkCol.Amount = 500.00;
	MilkCol.ActAmount = 0.0;
	MilkCol.CanNo = 101;
	MilkCol.MembCodeAuto = 1;
	MilkCol.WeightAuto = 1;
	MilkCol.FatAuto = 1;
	MilkCol.LrClrAuto = 1;
	MilkCol.SnfAuto = 1;
	MilkCol.EntryMode = 1;
	
	strcpy(DS.SociCode,"12345");
	strcpy(Member.SociCode,"12345");
	counter=0;

  // insert a few customers in the first database
	for(i=0;i<MONTH;i++)
	{
		DS.DateShiftId=0;

	    st=ham_env_new(&env);
		
		sprintf(filename,"Milk%02d12.db",i+1);

		st=ham_env_create_ex(env, filename, HAM_DISABLE_MMAP, 0644, &params_create[0]);

    	st=ham_new(&db);

    	st=ham_new(&db1);

    	st=ham_new(&db2);

    	st=ham_env_create_db(env, db, DBNAME_MILKCOLLECT, HAM_ENABLE_DUPLICATES, &params_create_db[0]);

		st=ham_env_create_db(env, db1, DBNAME_DATESHIFT, 0, &params_create_db1[0]);

		st=ham_env_create_db(env, db2, DBNAME_MEMBER, HAM_ENABLE_DUPLICATES, &params_create_db2[0]);

		st=ham_cursor_create(db2, 0, 0, &cursor2);	

		for(j=0;j<DAYS;j++)
		{
			sprintf(str_date,"%02d/%02d/2012",j+1,i+1);
			strcpy(MilkCol.Date,str_date);
			strcpy(DS.Date,str_date);

			MilkCol.Qty = j;

			for(k=0;k<SHIFT;k++)
			{
				DS.DateShiftId++;
				MilkCol.DateShiftId = DS.DateShiftId;
				if(k == 0)
				{
					MilkCol.Shift = 'M';
					DS.Shift = 'M';
					Member.Shift = 'M';
				}	
				else
				{	
				 	MilkCol.Shift = 'E';
					DS.Shift = 'E';
					Member.Shift = 'E';
				}

			  	date_key.data=(void *)&DS.DateShiftId;
	 		 	date_key.size=sizeof(DS.DateShiftId);
	  			date_record.data=(void *)&DS;
	  			date_record.size=sizeof(DS);
		
				st=ham_insert(db1, 0, &date_key, &date_record, 0);
				counter++;
				if(counter==1562) 
					printf("hit\n");

	    		for (l=1; l<=CUSTOMERS; l++) 
				{
					MilkCol.MilkColId++;

					MilkCol.MemberCode = l;
					MilkCol.SampleNo = l;
					MilkCol.Fat = l;

					Member.MilkType=MilkCol.MilkType;    

					Member.MemberCode=MilkCol.MemberCode;

					member_key.data=(void*)&Member.MemberCode;
		 		 	member_key.size=sizeof(Member.MemberCode);
		  			member_record.data=(void *)&Member;
		  			member_record.size=sizeof(Member);

					st=ham_cursor_find_ex(cursor2, &member_key, &member_record, 0);

/*					while(st !=0)
					{
						ptMemb = (Memb*)member_record.data;							
						st=ham_cursor_move(cursor2, &member_key, &member_record, HAM_CURSOR_NEXT);
					}
*/	
					Member.MCData[j].Qty =	MilkCol.Qty;				
					Member.MCData[j].Fat =	MilkCol.Fat;				
					Member.MCData[j].Snf =	MilkCol.Snf;				
					Member.MCData[j].Clr =	MilkCol.FatLr;				
					Member.MCData[j].Solid =	MilkCol.Solid;				
					Member.MCData[j].Amount =	MilkCol.Amount;	

					//Search Shift
					if(st==HAM_KEY_NOT_FOUND)
					{
						st=ham_insert(db2, 0, &member_key, &member_record, HAM_DUPLICATE);
						counter++;
						if(counter==1562) 
							printf("hit\n");
					}
					else if(st==HAM_SUCCESS)
					{
						while(st ==0 )
						{
							ptMemb = (Memb*)member_record.data;							
							//Check Shift
							if(ptMemb->Shift == MilkCol.Shift)
							{
								if(ptMemb->MilkType == MilkCol.MilkType)
								{
									ptMemb->MCData[j].Qty =	MilkCol.Qty;				
									ptMemb->MCData[j].Fat =	MilkCol.Fat;				
									ptMemb->MCData[j].Snf =	MilkCol.Snf;				
									ptMemb->MCData[j].Clr =	MilkCol.FatLr;						
									ptMemb->MCData[j].Solid =	MilkCol.Solid;				
									ptMemb->MCData[j].Amount =	MilkCol.Amount;	

		  							member_record.data=(void *)ptMemb;
		  							member_record.size=sizeof(Member);

									st=ham_cursor_overwrite(cursor2,&member_record, 0);
									break;
								}
							}	
							st=ham_cursor_move(cursor2, &member_key, &member_record, HAM_CURSOR_NEXT);

							if(ptMemb->MemberCode != MilkCol.MemberCode)
							{
								st=ham_insert(db2, 0, &member_key, &member_record, HAM_DUPLICATE);
								counter++;
								if(counter==1562) 
									printf("hit\n");
								break;
							}
						}
					}
			    }
			}
		}
		st=ham_cursor_close(cursor2);
	    st=ham_env_close(env, HAM_AUTO_CLEANUP);
		ham_delete(db);
		ham_delete(db1);
		ham_delete(db2);
		ham_env_delete(env);

	}	

    return 0;
}

static void errhandler(int level, const char *message)
{
  printf("ERROR: %s\n", message);
}

