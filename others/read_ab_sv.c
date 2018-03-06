/*
	2012-12-31 spencer.原有算法不再有效，使用了新的算法。
	将三个数据集合并为一个数据集，合并后的数据集规模为[行][列] = [20][2000]；
	注意顺序：
	SV_250m_REFL（4×8000×24）合并规则：[0 ~ 3][2000]
	SV_250m_EMIS（8000×24）合并规则：[4][2000]
	SV_1km（15×2000×6）的合并规则：[5 ~ 19][2000]

	1）取基本单位，前后各取5个单位，共11个单位
	2）剔除填充值，求均值A1
	3）A2 = std(A1)；标准偏差
	4）A1 - 2A2 ~ A1 + 2A2 之间的值为有效值
	5）对上述有效值取均值，作为[行][列]
*/
#include "hdf5.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <malloc.h>
#include <time.h>
#include <math.h>
#include <signal.h>

#define LINE_BEFORE 4
#define LINE_AFTER 5
#define FILL_VALUE 0
int read_ab_sv(char *ab_name, char *fy3a_obc_name, float **sv, float *a, float *b)
{
	//printf("##### start read ab_sv func #####\n");
	
	/* 判断文件是否可以访问*/
	if( access(fy3a_obc_name,0) != 0)
    {
		printf("obc file %s not exits\n",fy3a_obc_name);
		return __LINE__;
    }

	hid_t id_file;
	hid_t id_dataset;
    herr_t status;
	herr_t sfound;
    char sds_name[64] = "";
	char buf[256];
	unsigned short sv_250m_refl[4][8000][24]; // 存放数据集读出的数据
	unsigned short sv_250m_emis[8000][24]; // 存放数据集读出的数据
	unsigned short sv_1km[15][2000][6]; // 存放数据集读出的数据

	/* 打开hdf文件*/
	id_file = H5Fopen(fy3a_obc_name, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (id_file<0)
    {
        printf("File open error: %s!\n",fy3a_obc_name);
        return __LINE__;
    }

	*sv = (float *)calloc(20 * 2000, sizeof(float));
	if (*sv == NULL) // 分配内存失败
	{
		fprintf(stdout,"Memory allocation error!\n");
		status = H5Fclose(id_file);
		return __LINE__;
	}
		
	/* 读 "SV_250m_REFL" */
	sprintf(sds_name,"SV_250m_REFL");
		id_dataset = H5Dopen1(id_file, sds_name);
	if (id_dataset<0)
	{
		printf("Dateset open error: %s!\n",sds_name);
		status = H5Fclose(id_file);
		return __LINE__;
	}
			
	status = H5Dread(id_dataset, H5T_NATIVE_USHORT, H5S_ALL, H5S_ALL, H5P_DEFAULT, sv_250m_refl);
	if (status<0)
	{
		printf("Dateset read error: %s!\n",sds_name);
		status = H5Dclose(id_dataset);
		status = H5Fclose(id_file);
		return __LINE__;
	}

	/* 对数据集SV_250m_REFL读出的数据进行处理；SV_250m_REFL（4×8000×24）合并后：[0 ~ 3][2000]*/
	for (int i = 0; i < 4; i++)
	{
		for (int j = 0; j < 2000; j++) // 每4行为一个单位取数
		{
			//sv_250m_refl[i][j * 4][k];
			int iLineS = j - LINE_BEFORE;
			int iLineE = j + LINE_AFTER + 1; // 11个单元
			iLineS = iLineS < 0 ? 0 : iLineS; // 开始不足5个单位，每个单位4 × 24个元素
			iLineE = iLineE >= 2000 ? 2000 : iLineE; // 结束不足5个单位，每个单位4 × 24个元素

			/* 取均值sv_250m_refl[i][iLineS * 4][0] ～ sv_250m_refl[i][iLineE * 4][23]*/
			unsigned long ulCount = 0; // 计数
			unsigned long ulSum = 0; // 和
			for (int k = iLineS * 4; k < iLineE * 4; k++) // K为8000行中的行
			{
				for (int m = 0; m < 24; m++)
				{
					if (FILL_VALUE != sv_250m_refl[i][k][m])
					{
						ulSum += sv_250m_refl[i][k][m];
						ulCount++;
					}
				}
			}
			if (!ulCount) // iLineS * 4 和 iLineE * 4之间没有发现有效值
			{
				continue; // 不取均值、方差、标准偏差，不再次求平均；该点值分配的时候已为0，无需赋值；继续求下一个点
			}
			const float flAvrage = ulSum * 1.0 / ulCount; // 均值
			//if (i == 0 && j == 0 || i == 1 && j == 1 || i == 3 && j == 1000)
			//	printf("iLineS * 4 = %d, iLineE * 4 = %d, ulSum = %d, ulCount = %d, flAvrage = %f.\n", iLineS * 4, iLineE * 4, ulSum, ulCount, flAvrage);
			double dblStdDiff = 0; // 方差
			for (int k = iLineS * 4; k < iLineE * 4; k++) // K为8000行中的行
			{
				for (int m = 0; m < 24; m++)
				{
					if (FILL_VALUE != sv_250m_refl[i][k][m])
					{
						dblStdDiff += pow(sv_250m_refl[i][k][m] - flAvrage, 2);
					}
				}
			}
			dblStdDiff /= ulCount; // ulCount在求均值的时候，已经进行了统计
			const float flStd = sqrt(dblStdDiff); // 标准偏差
			//if (0 == i && 0 == j || i == 1 && j == 1 || i == 3 && j == 1000)
			//	printf("dblSMSEkkkkkkkkkkkkkkkk= %f.\n", dblStdDiff, flStd);

			/* 4）A1 - 2A2 ~ A1 + 2A2 之间的值为有效值*/
			ulCount = 0; // 计数
			ulSum = 0; // 和
			for (int k = iLineS * 4; k < iLineE * 4; k++)
			{
				for (int m = 0; m < 24; m++)
				{
					if (FILL_VALUE != sv_250m_refl[i][k][m] && 
						flAvrage - 2 * flStd <= sv_250m_refl[i][k][m] &&
						flAvrage + 2 * flStd >= sv_250m_refl[i][k][m])
					{
						ulSum += sv_250m_refl[i][k][m];
						ulCount++;
					}
				}
			}
			if (!ulCount) // iLineS * 4 和 iLineE * 4之间没有发现有效值
			{
				continue; // 不再次求平均；该点值分配的时候已为0，无需赋值；继续求下一个点
			}
			(*sv)[i * 2000 + j] = ulSum * 1.0 / ulCount; // 行 × 行宽 + 列；按照顺序得到0~4行
			//if (j < 10)
			//{
			//	printf("ulSum = %d, ulCount = %d, (*sv)[%d][%d] = %f.\n", ulSum, ulCount, i, j, (*sv)[i * 2000 + j]);
			//}
		}
	}

	/* 读 "SV_250m_EMIS"*/
	sprintf(sds_name,"SV_250m_EMIS");
		id_dataset = H5Dopen1(id_file, sds_name);
	if (id_dataset<0)
	{
		printf("Dateset open error: %s!\n",sds_name);
		status = H5Fclose(id_file);
		return __LINE__;
	}
			
	status = H5Dread(id_dataset, H5T_NATIVE_USHORT, H5S_ALL, H5S_ALL, H5P_DEFAULT, sv_250m_emis);
	if (status<0)
	{
		printf("Dateset read error: %s!\n",sds_name);
		status = H5Dclose(id_dataset);
		status = H5Fclose(id_file);
		return __LINE__;
	}

	/* 对数据集SV_250m_EMIS读出的数据进行处理；SV_250m_EMIS（8000 × 24）合并后[4][2000]*/
	for (int j = 0; j < 2000; j++) // 每4行为一个单位取数
	{
		//sv_250m_emis[j * 4][k];
		int iLineS = j - LINE_BEFORE;
		int iLineE = j + LINE_AFTER + 1; // 11个单元
		iLineS = iLineS < 0 ? 0 : iLineS; // 开始不足5个单位，每个单位4 × 24个元素
		iLineE = iLineE >= 2000 ? 2000 : iLineE; // 结束不足5个单位，每个单位4 × 24个元素

		/* 取均值sv_250m_emis[iLineS * 4][0] ～ sv_250m_emis[iLineE * 4][23]*/
		unsigned long ulCount = 0; // 计数
		unsigned long ulSum = 0; // 和
		for (int k = iLineS * 4; k < iLineE * 4; k++) // 最后两个维度的行
		{
			for (int m = 0; m < 24; m++)
			{
				if (FILL_VALUE != sv_250m_emis[k][m])
				{
					ulSum += sv_250m_emis[k][m];
					ulCount++;
				}
			}
		}
		if (!ulCount)
		{
			continue; // 同上一数据集的处理
		}
		float flAvrage = ulSum * 1.0 / ulCount; // 均值
		double dblStdDiff = 0; // 方差
		for (int k = iLineS * 4; k < iLineE * 4; k++)
		{
			for (int m = 0; m < 24; m++)
			{
				if (FILL_VALUE != sv_250m_emis[k][m])
				{
					dblStdDiff += pow(sv_250m_emis[k][m] - flAvrage, 2);
				}
			}
		}
		dblStdDiff /= ulCount;
		float flStd = sqrt(dblStdDiff); // 标注差

		/* 4）A1 - 2A2 ~ A1 + 2A2 之间的值为有效值*/
		ulCount = 0; // 计数
		ulSum = 0; // 和
		for (int k = iLineS * 4; k < iLineE * 4; k++)
		{
			for (int m = 0; m < 24; m++)
			{
				if (FILL_VALUE != sv_250m_emis[k][m] && 
					flAvrage - 2 * flStd <= sv_250m_emis[k][m] &&
					flAvrage + 2 * flStd >= sv_250m_emis[k][m])
				{
					ulSum += sv_250m_emis[k][m];
					ulCount++;
				}
			}
		}
		if (!ulCount)
		{
			continue; // 同上一数据集的处理
		}
		(*sv)[4 * 2000 + j] = ulSum * 1.0 / ulCount; // 行 × 行宽 + 列；按照顺序得到第5行
		//if (j < 10)
		//{
		//	printf("ulSum = %d, ulCount = %d, (*sv)[%d][%d] = %f.\n", ulSum, ulCount, 4, j, (*sv)[4 * 2000 + j]);
		//}
	}

	/*  读   "SV_1km" */
	sprintf(sds_name,"SV_1km");
	id_dataset = H5Dopen1(id_file, sds_name);
	if (id_dataset<0)
	{
		printf("Dateset open error: %s!\n",sds_name);
		status = H5Fclose(id_file);
		return __LINE__;
	}
			
	status = H5Dread(id_dataset, H5T_NATIVE_USHORT, H5S_ALL, H5S_ALL, H5P_DEFAULT, sv_1km);
	if (status<0)
	{
		printf("Dateset read error: %s!\n",sds_name);
		status = H5Dclose(id_dataset);
		status = H5Fclose(id_file);
		return __LINE__;
	}

	/* 对数据集SV_1km读出的数据进行处理；SV_1km（15 × 2000 × 6）合并后[5 ~ 19][2000]*/
	for (int i = 0; i < 15; i++)
	{
		for (int j = 0; j < 2000; j++) // 每4行为一个单位取数
		{
			//sv_1km[i][j * 4][k];
			int iLineS = j - LINE_BEFORE;
			int iLineE = j + LINE_AFTER + 1; // 11个单元
			iLineS = iLineS < 0 ? 0 : iLineS; // 开始不足5个单位，每个单位4 × 24个元素
			iLineE = iLineE >= 2000 ? 2000 : iLineE; // 结束不足5个单位，每个单位4 × 24个元素

			/* 取均值sv_1km[i][iLineS][0] ～ sv_1km[i][iLineE][5]*/
			unsigned long ulCount = 0; // 计数
			unsigned long ulSum = 0; // 和
			for (int k = iLineS; k < iLineE; k++) // 最后两个维度的行
			{
				for (int m = 0; m < 6; m++)
				{
					if (FILL_VALUE != sv_1km[i][k][m])
					{
						ulSum += sv_1km[i][k][m];
						ulCount++;
					}
				}
			}
			if (!ulCount)
			{
				continue; // 同上一数据集的处理
			}
			float flAvrage = ulSum * 1.0 / ulCount; // 均值
			double dblStdDiff = 0; // 方差
			for (int k = iLineS; k < iLineE; k++) // 最后两个维度的行
			{
				for (int m = 0; m < 6; m++)
				{
					if (FILL_VALUE != sv_1km[i][k][m])
					{
						dblStdDiff += pow(sv_1km[i][k][m] - flAvrage, 2);
					}
				}
			}
			dblStdDiff /= ulCount;
			float flStd = sqrt(dblStdDiff); // 标注差

			/* 4）A1 - 2A2 ~ A1 + 2A2 之间的值为有效值*/
			ulCount = 0; // 计数
			ulSum = 0; // 和
			for (int k = iLineS; k < iLineE; k++)
			{
				for (int m = 0; m < 6; m++)
				{
					if (FILL_VALUE != sv_1km[i][k][m] && 
						flAvrage - 2 * flStd <= sv_1km[i][k][m] &&
						flAvrage + 2 * flStd >= sv_1km[i][k][m])
					{
						ulSum += sv_1km[i][k][m];
						ulCount++;
					}
				}
			}
			if (!ulCount)
			{
				continue; // 同上一数据集的处理
			}
			(*sv)[(i + 5) * 2000 + j] = ulSum * 1.0 / ulCount; // 行 × 行宽 + 列；按照顺序得到5~19行
			//if (j < 10)
			//{
			//	printf("ulSum = %d, ulCount = %d, (*sv)[%d][%d] = %f.\n", ulSum, ulCount, i + 5, j, (*sv)[(i + 5) * 2000 + j]);
			//}
		}
	}
	
	status = H5Dclose(id_dataset);
	status = H5Fclose(id_file);

	/* 得到a和b*/
	char *p = NULL;
	FILE *fp = fopen(ab_name,"rt");
	if(fp == NULL)
	{
		printf( "open failed: %s", ab_name);
		return __LINE__;
	}
	while(  fgets( buf, sizeof(buf), fp ) )
	{
		p = strtok(buf," ");
		*a=atof(p);
		p = strtok(NULL,"");
		*b=atof(p);	
		a++;
		b++;
	}
	
	fclose(fp);
	return 0;
}

