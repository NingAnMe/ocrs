/*********************************************************************************
 *Copyright(C) 2011-2012 broadengate
 *FileName: calibration.c 
 *Author: wangpeng 
 *Version: 1.0.0
 *Date:  2012/09/04
 *Description: k0,k1 or ab calibration
**********************************************************************************/ 

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <time.h>
#include <math.h>
#include <signal.h>
#include <unistd.h>
#include "proto.h"


int calibration (char *fy3_name, unsigned short *sds_mersi1, unsigned short *sds_mersi5, unsigned short *sds_mersi6, short *solar_zenith, float *a, float *b, float *sv, float sds_mersi20[20][2000][2048])
{
	
	//printf("##### start calibration func #####\n");
	long  dsl   = 0;
	double daycor = 0.;
	int Jday = 0;
	int year = 0;
	int month = 0;
	int day = 0;
	int i = 0;
	int j = 0;
	int k = 0;
	double slope = 0.;
	double arof = 0.;
	char path[128]    = "";
    char l1bname[128] = "";
    int type = -1;
	
	// 通过FY3A的文件名字 获取 年 月 日  
	get_fy3_date(fy3_name, &year, &month, &day);
    GetFilePath(fy3_name, path, l1bname);

	// 获得卫星发射至今的总天数 
    if ( strncmp(l1bname, "FY3A", 4) == 0 )
    {
        //printf("3A\n");
	    dsl = launch_date(2008, 5, 27, year, month, day);
    }

    if ( strncmp(l1bname, "FY3B", 4) == 0 )
    {
        //printf("3B %d-%d-%d\n",year,month,day);
	    dsl = launch_date(2010, 11, 5, year, month, day);
        if( year == 2013 )
            if ( month >= 3 )
                if( day >= 2 )
                    type = 0;
        if ( year > 2013 )
            type = 0;
    }
//	dsl = launch_date(2008, 5, 27, year, month, day);

	// 获得伽利略日期 
	dom2doy(year, month, day, &Jday);

	// 计算日地距离修正系数 
	get_daycor(&daycor, Jday);

    //printf("type == %d\n", type);
    //type = 1;
    if ( type == 0 )
    
        for(k=0; k<20; k++)
        {
           // slope = (float)dsl * a[k] + b[k];
            for(i=0; i<2000; i++)
            {
        //	printf("sv = %f\n",sv(k,i/10));
            for(j=0; j<2048; j++)
            {
                if(k < 4)	
                {
                    //arof = (sds_mersi1(k,i,j) - sv(k,i/10)) * slope * 0.01;
                    //arof = (sds_mersi1(k,i,j)*1.0 - sv(k,i)) * slope * 0.01; // spencer 20130104 不再除10
                    arof = (sds_mersi1(k,i,j) * 0.01 * 0.01); //FY3B 新数据算法
                    sds_mersi20[k][i][j] = arof; // (daycor*cos(solar_zenith(i,j)*0.01*PI/180.0));	
                }
                if(k==4)
                {
                    sds_mersi20[k][i][j] = sds_mersi5(i,j)/100.0;
                }
                if(k > 4)
                {
                    //arof = (sds_mersi6(k-5,i,j) - sv(k,i/10)) * slope * 0.01;
                    //arof = (sds_mersi6(k-5,i,j) - sv(k,i)) * slope * 0.01; // spencer 20130104 不再除10
                    arof = (sds_mersi6(k-5,i,j) * 0.01 * 0.01); //FY3B 新数据算法
                    sds_mersi20[k][i][j] = arof;  //(daycor*cos(solar_zenith(i,j)*0.01*PI/180.0));
                   // if (i==1442)
                   // {
                   //     if(j==293)
                   //     {
                   //         printf("arof = %u - %f - %f\n",sds_mersi6(k-5,i,j) ,arof, sds_mersi20[k][i][j]);
                   //         printf("solar_zen = %u\n",solar_zenith(i,j));
                   //         printf("sun = %lf - %f\n",daycor, solar_zenith(i,j)*0.01*PI/180.0);
                   //         printf("cos sun = %lf | %f\n",daycor, cos(solar_zenith(i,j)*0.01*PI/180.0));
                   //     }
                   // }
                }
                    
            }
            }
        }
    }
    else
    {

        for(k=0; k<20; k++)
        {
            slope = (float)dsl * a[k] + b[k];
            for(i=0; i<2000; i++)
            {
                for(j=0; j<2048; j++)
                {
                    if(k < 4)	
                    {
                        //arof = (sds_mersi1(k,i,j) - sv(k,i/10)) * slope * 0.01;
                        arof = (sds_mersi1(k,i,j)*1.0 - sv(k,i)) * slope * 0.01; // spencer 20130104 不再除10
                        sds_mersi20[k][i][j] = arof; // (daycor*cos(solar_zenith(i,j)*0.01*PI/180.0));	
                    }
                    if(k==4)
                    {
                        sds_mersi20[k][i][j] = sds_mersi5(i,j)/100.0;
                    }
                    if(k > 4)
                    {
                        //arof = (sds_mersi6(k-5,i,j) - sv(k,i/10)) * slope * 0.01;
                        arof = (sds_mersi6(k-5,i,j) - sv(k,i)) * slope * 0.01; // spencer 20130104 不再除10
                        sds_mersi20[k][i][j] = arof; // (daycor*cos(solar_zenith(i,j)*0.01*PI/180.0));
                    }

                }
            }
        }
    }
	
	return 0;
}


int get_daycor(double *daycor, int Jday)
{
	//const float PI = 3.1415926;
	double EarthSunDist = 0.;
	EarthSunDist  = (1.00014 - 0.01671*cos(1.0*2*PI*(0.9856002831*Jday-3.4532868)/360.0) - \
			 0.00014*cos(2.0*2*PI*(0.9856002831*Jday-3.4532868)/360.0));
	*daycor = pow(1.0/EarthSunDist,2); 
	//printf("daycor in = %lf\n",*daycor);
	return 0;
}


int get_fy3_date(char *fy3_name, int *year, int *month, int *day)
{
	char path[128]    = "";
        char l1bname[128] = "";
        char tmp_l1bname[128] = "";

	
	//在绝对路径中 提取FY3A的文件名字
        GetFilePath(fy3_name, path, l1bname);

        strcpy(tmp_l1bname, l1bname);
        *(tmp_l1bname+23) = 0;
        *year = atoi(tmp_l1bname+19);

        strcpy(tmp_l1bname, l1bname);
        *(tmp_l1bname+25) = 0;
        *month = atoi(tmp_l1bname+23);

        strcpy(tmp_l1bname, l1bname);
        *(tmp_l1bname+27) = 0;
        *day = atoi(tmp_l1bname+25);	

	return 0;
}


int launch_date(int launch_year, int launch_mon, int launch_day, int now_year, int now_mon, int now_day)
{
        struct tm launch_date;
        struct tm now_date;
        time_t launch_sec;
        time_t now_sec;
	long days;

        launch_date.tm_year = launch_year - 1900;
        launch_date.tm_mon = launch_mon -1;
        launch_date.tm_mday = launch_day;
        launch_date.tm_hour = 0;
        launch_date.tm_min = 0;
        launch_date.tm_sec = 0;

        now_date.tm_year = now_year - 1900;
        now_date.tm_mon = now_mon - 1;
        now_date.tm_mday= now_day;
        now_date.tm_hour = 0;
        now_date.tm_min = 0;
        now_date.tm_sec = 0;

        launch_sec = mktime(&launch_date);
        now_sec = mktime(&now_date);
        days = (now_sec - launch_sec)/(24*60*60);
	
        return days;
}       
