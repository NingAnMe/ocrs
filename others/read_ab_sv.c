/*
	2012-12-31 spencer.ԭ���㷨������Ч��ʹ�����µ��㷨��
	���������ݼ��ϲ�Ϊһ�����ݼ����ϲ�������ݼ���ģΪ[��][��] = [20][2000]��
	ע��˳��
	SV_250m_REFL��4��8000��24���ϲ�����[0 ~ 3][2000]
	SV_250m_EMIS��8000��24���ϲ�����[4][2000]
	SV_1km��15��2000��6���ĺϲ�����[5 ~ 19][2000]

	1��ȡ������λ��ǰ���ȡ5����λ����11����λ
	2���޳����ֵ�����ֵA1
	3��A2 = std(A1)����׼ƫ��
	4��A1 - 2A2 ~ A1 + 2A2 ֮���ֵΪ��Чֵ
	5����������Чֵȡ��ֵ����Ϊ[��][��]
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
	
	/* �ж��ļ��Ƿ���Է���*/
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
	unsigned short sv_250m_refl[4][8000][24]; // ������ݼ�����������
	unsigned short sv_250m_emis[8000][24]; // ������ݼ�����������
	unsigned short sv_1km[15][2000][6]; // ������ݼ�����������

	/* ��hdf�ļ�*/
	id_file = H5Fopen(fy3a_obc_name, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (id_file<0)
    {
        printf("File open error: %s!\n",fy3a_obc_name);
        return __LINE__;
    }

	*sv = (float *)calloc(20 * 2000, sizeof(float));
	if (*sv == NULL) // �����ڴ�ʧ��
	{
		fprintf(stdout,"Memory allocation error!\n");
		status = H5Fclose(id_file);
		return __LINE__;
	}
		
	/* �� "SV_250m_REFL" */
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

	/* �����ݼ�SV_250m_REFL���������ݽ��д���SV_250m_REFL��4��8000��24���ϲ���[0 ~ 3][2000]*/
	for (int i = 0; i < 4; i++)
	{
		for (int j = 0; j < 2000; j++) // ÿ4��Ϊһ����λȡ��
		{
			//sv_250m_refl[i][j * 4][k];
			int iLineS = j - LINE_BEFORE;
			int iLineE = j + LINE_AFTER + 1; // 11����Ԫ
			iLineS = iLineS < 0 ? 0 : iLineS; // ��ʼ����5����λ��ÿ����λ4 �� 24��Ԫ��
			iLineE = iLineE >= 2000 ? 2000 : iLineE; // ��������5����λ��ÿ����λ4 �� 24��Ԫ��

			/* ȡ��ֵsv_250m_refl[i][iLineS * 4][0] �� sv_250m_refl[i][iLineE * 4][23]*/
			unsigned long ulCount = 0; // ����
			unsigned long ulSum = 0; // ��
			for (int k = iLineS * 4; k < iLineE * 4; k++) // KΪ8000���е���
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
			if (!ulCount) // iLineS * 4 �� iLineE * 4֮��û�з�����Чֵ
			{
				continue; // ��ȡ��ֵ�������׼ƫ����ٴ���ƽ�����õ�ֵ�����ʱ����Ϊ0�����踳ֵ����������һ����
			}
			const float flAvrage = ulSum * 1.0 / ulCount; // ��ֵ
			//if (i == 0 && j == 0 || i == 1 && j == 1 || i == 3 && j == 1000)
			//	printf("iLineS * 4 = %d, iLineE * 4 = %d, ulSum = %d, ulCount = %d, flAvrage = %f.\n", iLineS * 4, iLineE * 4, ulSum, ulCount, flAvrage);
			double dblStdDiff = 0; // ����
			for (int k = iLineS * 4; k < iLineE * 4; k++) // KΪ8000���е���
			{
				for (int m = 0; m < 24; m++)
				{
					if (FILL_VALUE != sv_250m_refl[i][k][m])
					{
						dblStdDiff += pow(sv_250m_refl[i][k][m] - flAvrage, 2);
					}
				}
			}
			dblStdDiff /= ulCount; // ulCount�����ֵ��ʱ���Ѿ�������ͳ��
			const float flStd = sqrt(dblStdDiff); // ��׼ƫ��
			//if (0 == i && 0 == j || i == 1 && j == 1 || i == 3 && j == 1000)
			//	printf("dblSMSEkkkkkkkkkkkkkkkk= %f.\n", dblStdDiff, flStd);

			/* 4��A1 - 2A2 ~ A1 + 2A2 ֮���ֵΪ��Чֵ*/
			ulCount = 0; // ����
			ulSum = 0; // ��
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
			if (!ulCount) // iLineS * 4 �� iLineE * 4֮��û�з�����Чֵ
			{
				continue; // ���ٴ���ƽ�����õ�ֵ�����ʱ����Ϊ0�����踳ֵ����������һ����
			}
			(*sv)[i * 2000 + j] = ulSum * 1.0 / ulCount; // �� �� �п� + �У�����˳��õ�0~4��
			//if (j < 10)
			//{
			//	printf("ulSum = %d, ulCount = %d, (*sv)[%d][%d] = %f.\n", ulSum, ulCount, i, j, (*sv)[i * 2000 + j]);
			//}
		}
	}

	/* �� "SV_250m_EMIS"*/
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

	/* �����ݼ�SV_250m_EMIS���������ݽ��д���SV_250m_EMIS��8000 �� 24���ϲ���[4][2000]*/
	for (int j = 0; j < 2000; j++) // ÿ4��Ϊһ����λȡ��
	{
		//sv_250m_emis[j * 4][k];
		int iLineS = j - LINE_BEFORE;
		int iLineE = j + LINE_AFTER + 1; // 11����Ԫ
		iLineS = iLineS < 0 ? 0 : iLineS; // ��ʼ����5����λ��ÿ����λ4 �� 24��Ԫ��
		iLineE = iLineE >= 2000 ? 2000 : iLineE; // ��������5����λ��ÿ����λ4 �� 24��Ԫ��

		/* ȡ��ֵsv_250m_emis[iLineS * 4][0] �� sv_250m_emis[iLineE * 4][23]*/
		unsigned long ulCount = 0; // ����
		unsigned long ulSum = 0; // ��
		for (int k = iLineS * 4; k < iLineE * 4; k++) // �������ά�ȵ���
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
			continue; // ͬ��һ���ݼ��Ĵ���
		}
		float flAvrage = ulSum * 1.0 / ulCount; // ��ֵ
		double dblStdDiff = 0; // ����
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
		float flStd = sqrt(dblStdDiff); // ��ע��

		/* 4��A1 - 2A2 ~ A1 + 2A2 ֮���ֵΪ��Чֵ*/
		ulCount = 0; // ����
		ulSum = 0; // ��
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
			continue; // ͬ��һ���ݼ��Ĵ���
		}
		(*sv)[4 * 2000 + j] = ulSum * 1.0 / ulCount; // �� �� �п� + �У�����˳��õ���5��
		//if (j < 10)
		//{
		//	printf("ulSum = %d, ulCount = %d, (*sv)[%d][%d] = %f.\n", ulSum, ulCount, 4, j, (*sv)[4 * 2000 + j]);
		//}
	}

	/*  ��   "SV_1km" */
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

	/* �����ݼ�SV_1km���������ݽ��д���SV_1km��15 �� 2000 �� 6���ϲ���[5 ~ 19][2000]*/
	for (int i = 0; i < 15; i++)
	{
		for (int j = 0; j < 2000; j++) // ÿ4��Ϊһ����λȡ��
		{
			//sv_1km[i][j * 4][k];
			int iLineS = j - LINE_BEFORE;
			int iLineE = j + LINE_AFTER + 1; // 11����Ԫ
			iLineS = iLineS < 0 ? 0 : iLineS; // ��ʼ����5����λ��ÿ����λ4 �� 24��Ԫ��
			iLineE = iLineE >= 2000 ? 2000 : iLineE; // ��������5����λ��ÿ����λ4 �� 24��Ԫ��

			/* ȡ��ֵsv_1km[i][iLineS][0] �� sv_1km[i][iLineE][5]*/
			unsigned long ulCount = 0; // ����
			unsigned long ulSum = 0; // ��
			for (int k = iLineS; k < iLineE; k++) // �������ά�ȵ���
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
				continue; // ͬ��һ���ݼ��Ĵ���
			}
			float flAvrage = ulSum * 1.0 / ulCount; // ��ֵ
			double dblStdDiff = 0; // ����
			for (int k = iLineS; k < iLineE; k++) // �������ά�ȵ���
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
			float flStd = sqrt(dblStdDiff); // ��ע��

			/* 4��A1 - 2A2 ~ A1 + 2A2 ֮���ֵΪ��Чֵ*/
			ulCount = 0; // ����
			ulSum = 0; // ��
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
				continue; // ͬ��һ���ݼ��Ĵ���
			}
			(*sv)[(i + 5) * 2000 + j] = ulSum * 1.0 / ulCount; // �� �� �п� + �У�����˳��õ�5~19��
			//if (j < 10)
			//{
			//	printf("ulSum = %d, ulCount = %d, (*sv)[%d][%d] = %f.\n", ulSum, ulCount, i + 5, j, (*sv)[(i + 5) * 2000 + j]);
			//}
		}
	}
	
	status = H5Dclose(id_dataset);
	status = H5Fclose(id_file);

	/* �õ�a��b*/
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

