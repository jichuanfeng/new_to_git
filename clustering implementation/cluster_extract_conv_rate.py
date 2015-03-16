# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 12:35:20 2015

@author: jfeng
"""

import pandas as pd
import datetime as dt
import sys

def extract_conv_rate(cluster_file,act_conv_file,lconrate):

    cluster_t=pd.read_csv(cluster_file)

    conv_rate_t=pd.read_csv(act_conv_file,sep='\t',header=None)
    conv_rate_t.rename(columns={0:'num_click',1:'num_convertedclicks',2:'num_uv',3:'num_sale',4:'keyword',5:'matchtype',6:'date'},
                       inplace=True)
    conv_rate_t.fillna(0,inplace=True)

    cluster_conv_rate_t=conv_rate_t.merge(cluster_t[['keyword','matchtype','clusterz']],on=['keyword','matchtype'])
    cluster_conv_rate_t['date']=cluster_conv_rate_t.apply(lambda x: dt.datetime.strptime(x['date'],'%Y-%m-%d'), axis=1)

    today=dt.datetime.now()
    two_weeks_ago=today-dt.timedelta(days=30)

    date_index=(cluster_conv_rate_t.date>=two_weeks_ago)&(cluster_conv_rate_t.date<=today)
    cl_conv_rate_t=cluster_conv_rate_t[date_index]

    cluster_group=cl_conv_rate_t.groupby('clusterz')
    cl_sum_t=cluster_group[['num_click','num_convertedclicks']].sum()
    cl_sum_t['conv_rate']=cl_sum_t.apply(lambda x: (x['num_convertedclicks']*1.0+1)/(x['num_click']+lconrate),axis=1)
    cl_sum_t.reset_index(inplace=True)

    cluster_final_t=cluster_t.merge(cl_sum_t[['clusterz','conv_rate']],on='clusterz',how='left')
    
    return cluster_final_t[['keyword','matchtype','conv_rate']] 

def main():
    cluster_file=sys.argv[1]
    act_conv_file=sys.argv[2]
    store_file=sys.argv[3]
    lconrate=40
    cluster_final_t=extract_conv_rate(cluster_file,act_conv_file,lconrate)
    cluster_final_t.to_csv(store_file,index=False)
    
if __name__=='__main__':
    main()

#cluster_file='C:\TC SEM project\Keyword Clustering\within_cam_mat_new\clusterz_t.txt'
#act_conv_file='C:\TC SEM project\Keyword Clustering\within_cam_mat_new\sem_nonbrand_t_3_11_cluster.csv'
#cluster_final_t=extract_conv_rate(cluster_file,act_conv_file)

