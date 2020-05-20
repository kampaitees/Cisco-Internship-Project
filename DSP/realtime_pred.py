import math
from fbprophet import Prophet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from influxdb import DataFrameClient
from ExportCsvToInflux import ExporterObject
from models import Timeseries_Modeling
import sys
import os


#influx -host 10.241.91.192 -precision rfc3339

def ConfigSectionMap(section):
    dict1 = {}
    Config = configparser.ConfigParser()
    Config.read("./config.cfg")
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def remove_timezone(row):
    row['Date & Time'] = row['Date & Time'][:-6]
    return row


def get_data(granularity="1d",end_week="8w",clustername='AMS01_VPP1_GSB_Meeting_Cluster',datasrc='vROM'):
    query = "select mean(*) from "+datasrc+" where clustername=\'"+clustername+"\'" 
    if end_week:
        query+=" and time>now()-"+end_week
    query+=" group by time("+granularity+")"
    print(query)
    df = client.query(query)[datasrc]
    collist = df.columns.values.tolist()

    for colname in collist:
        if colname[:4]=="mean":
            newcolname = colname[5:]
            df = df.rename(columns={colname:newcolname})
    df = df.reset_index()
    df = df.rename(columns={'index':'Date & Time'})
    df['Date & Time'] = df['Date & Time'].astype('str')
    df = df.apply(remove_timezone, axis = 1)

    return df



options = ConfigSectionMap("Local_options")
server_ip         = options["server_ip"]
db_name           = options["db_name"]
train_period      = options["train_period"]
aggregation_level = options["aggregation_level"]
datasrc           = options["datasrc"]


exporter = ExporterObject()
client = DataFrameClient(host=server_ip,port=8086)
client.switch_database(db_name)


clusters = ['YYZ01-VPP1-Management-Cluster', 'SJC02-VPP2-TPGW-Cluster', 'SYD01-VPP1-Management-Cluster', 'SJC02-VPP3-Meeting4-Cluster', 'LHR03-TVPP1-Tahoe-CMS', 'SJC02-DC3-BTS-NBRWES', 'SYD01-VPP1-CloudCenter-Cluster', 'DFW01-VPP1-CloudCenter-Cluster', 'SJC02-VPP1-DBaaS-Cluster', 'DFW01-VPP1-Management-Cluster', 'SJC02-VPP2-Connect-Cluster', 'SJC02-TVPP2-VTS-Cluster', 'NRT03-VPP1-DBaaS-Cluster', 'SJC02-VPP3-Infrastructure-Cluster', 'mitajon-hosts', 'DFW02-TVPP2-VTS-Cluster', 'SJC02-TVPP1-CloudCenter2-Cluster', 'AMS01-VPP1-Redis-Cluster', 'LHR03-VPP1-Jabber-Cluster', 'DFW02-TVPP1-Management-Cluster', 'SJC02-TVPP3-vTS-M4-Cluster2', 'LHR03-VPP1-MMP-Cluster4', 'DFW01-VPP2-SDE-Cluster', 'DFW01-VPP1-Unallocated', 'IAD02-VPP1-MMP3-Cluster', 'SJC02-VPP1-Connect-Cluster', 'AMS01-TVPP1-Tahoe-CMS', 'DFW02-VPP1-MMP-Cluster', 'SYD01-VPP1-Voice', 'SJC02-DC3-BTS-SearchFarm-Cluster', 'SJC02-VPP2-Meeting2-Cluster', 'SJC02-VPP1-Management-Cluster', 'SJC02-TVPP2-CloudCenter-Cluster', 'SJC02-TVPP2-VTS-Cluster2', 'DFW02-VPP1-QLIK-Cluster', 'JFK01-VPP1-Unallocated', 'DFW02-VPP1-Meeting2-Cluster', 'SYD10-IPOP1-C-Series-Unallocated', 'SJC02-TVPP1-VMR-Cluster', 'DFW02-TVPP1-VMR-Cluster', 'AMS01-VPP1-GSB-Killers-Cluster', 'JFK01-VPP1-Management', 'IAD02-VPP1-Powered-Off', 'DFW02-TVPP2-CloudCenter-Cluster', 'DFW01-VPP3-Jabber-Cluster', 'NRT03-TVPP1-Tahoe-CMS', 'DFW02-TVPP5-Management-Cluster', 'SJC02-DC3-BTS-Management-Cluster', 'DFW01-VPP3-MMP-Cluster', 'SJC02-TVPP2-CloudCenter-Cluster(migrate2sj2t)', 'SJC02-VPP2-Management-Cluster', 'SJC02-DC3-BTS-Miscellaneous-Cluster', 'CloudCenterTest1', 'SJC02-TVPP3-Management-Cluster', 'SIN01-VPP1-Unallocated', 'LHR03-VPP1-Redis-Cluster', 'LHR03-VPP1-Killers-Cluster', 'SJC02-VPP4-Connect-Cluster', 'LHR03-VPP1-MMP-Cluster2', 'DFW02-TVPP5-vTS-Cluster', 'DFW01-VPP3-Miscellaneous-Cluster', 'SIN01-VPP1-MMP-Cluster2', 'AMS01-TVPP1-Unallocated', 'SJC02-TVPP5-vTS-Cluster', 'DFW02-VPP1-CloudCenter-Cluster', 'SIN01-VPP1-Redis-Cluster', 'NRT03-TVPP1-vTS-M4-Cluster', 'DFW02-VPP1-Infrastructure-Cluster', 'SJC02-DC3-BTS-Connect-Cluster', 'LHR03-TVPP1-vTS-M4-Cluster', 'DFW02-TVPP2-Management-Cluster', 'LabOCP', 'CloudCenterTest2', 'LHR03-VPP1-MMP-Cluster3', 'DFW01-VPP2-Management-Cluster', 'SJC02-TVPP4-Management-Cluster', 'LHR03-VPP1-Unallocated', 'DFW01-VPP3-Management-Cluster', 'DFW01-VPP1-Redis-Cluster', 'SJC02-TVPP5-Management-Cluster', 'AMS01-VPP1-GSB-Infrastructure-Cluster', 'DFW01-VPP1-MMP-Cluster2', 'DFW02-TVPP3-Management-Cluster', 'IAD02-VPP1-NBRWES', 'SJC02-VPP4-Unallocated', 'DFW01-VPP3-WebEx11-Cluster', 'SJC02-VPP1-MMP-Cluster', 'SJC02-VPP4-Management-Cluster', 'AMS01-VPP1-GSB-QLIK-Cluster', 'AMS01-VPP1-Performance', 'SJC02-DC3-BTS-Jabber-Cluster', 'SJC02-VPP3-Meeting2-Cluster', 'SJC02-VPP1-Meeting-Cluster', 'SJC02-DC3-BTS-Infrastructure-Cluster', 'AMS01-TVPP1-Management-Cluster', 'ORD10-VPP1-Management-Cluster', 'DFW01-VPP2-WebEx11-Cluster', 'DFW01-VPP2-MMP-Cluster', 'SJC02-DC3-BTS-Pod19-Unallocated', 'DFW01-VPP2-QLIK-Cluster', 'AMS01-VPP1-GSB-CloudCenter-Cluster', 'SJC02-VPP2-Meeting-Cluster', 'NRT03-TVPP1-Management-Cluster', 'SJC02-VPP7-Acano-Cluster', 'IAD02-VPP1-Meeting2-Cluster', 'SJC02-TVPP2-Management-Cluster', 'DFW01-VPP1-WebEx11-Cluster', 'DFW02-TVPP4-Management-Cluster', 'DFW02-TVPP1-CloudCenter-Cluster', 'DFW01-VPP1-Infrastructure-Cluster', 'SJC02-VPP3-Management-Cluster', 'SJC02-VPP3-Meeting-Cluster', 'LHR03-TVPP1-Management-Cluster', 'ORD10-VPP1-Unallocated', 'SYD01-VPP1-Redis-Cluster', 'SJC02-TVPP3-vTS-M4-Cluster', 'SJC02-VPP1-CloudCenter-Cluster', 'Automation-CI-Services', 'AMS01-VPP1-GSB-MMP3-Cluster', 'NRT03-VPP1-Infrastructure-Cluster', 'IAD02-VPP1-Meeting-Cluster', 'SJC02-VPP4-NBRWES-Cluster', 'SJC02-TVPP2-Tahoe-CMS(migrate2sj2t)', 'SJC02-TVPP1-Management-Cluster', 'LHR03-VPP1-Meeting-C220-Cluster', 'LHR03-VPP1-CloudCenter-Cluster', 'DFW02-VPP1-Meeting-NBRWES2', 'AMS01-VPP1-GSB-WebEx11-Cluster', 'DFW02-VPP1-Meeting-Cluster', 'DFW02-TVPP3-vTS-Cluster', 'SYD10-IPOP1-Meeting2-Cluster', 'NRT03-VPP1-CloudCenter-Cluster', 'SIN01-VPP1-Meeting-Cluster', 'SJC02-VPP2-Miscellaneous-Cluster', 'SJC02-VPP4-Meeting-Cluster', 'SJC02-TVPP1-CloudCenter-Cluster', 'SJC02-VPP2-QLIK-Internal', 'SJC02-DC3-BTS-Meeting-Cluster', 'SIN01-TVPP1-Management-Cluster', 'SJC02-TVPP3-vTS-Cluster', 'IAD02-VPP1-Unallocated', 'SJC02-VPP2-Infrastructure-Cluster', 'AMS01-TVPP1-VMR-Cluster', 'SJC02-VPP7-Management-Cluster', 'DFW01-VPP1-Miscellaneous-Cluster', 'JFK01-VPP1-MMP3', 'AMS01-VPP1-Unallocated', 'ORD10-VPP1-Meeting-Cluster', 'NRT03-VPP1-MMP2', 'SYD01-VPP1-NBRWES', 'DFW02-VPP1-Redis-Cluster', 'LHR03-TVPP1-CloudCenter-Cluster2', 'SJC02-VPP7-Meeting-Cluster', 'NRT03-VPP1-Meeting', 'NSX-POC', 'DFW02-TVPP3-vTS-M4-Cluster', 'SJC02-TVPP1-Unallocated', 'LHR03-TVPP1-CloudCenter-Cluster', 'JFK01-VPP1-Standard-Tier', 'SJC02-VPP3-Unallocated', 'LHR03-VPP1-WebEx11-Cluster', 'JFK01-VPP1-MMP2', 'SIN01-VPP1-Infrastructure-Cluster', 'DFW02-TVPP1-Unallocated', 'LHR03-VPP1-NBRWES-Cluster2', 'DFW01-VPP1-Connect-Cluster', 'SYD10-IPOP1-Management-Cluster', 'IaaS-Storage', 'SIN01-VPP1-NBRWES-Cluster', 'SJC02-VPP1-NBRWES2-Cluster', 'SYD01-VPP1-Unallocated', 'Test_Cluster', 'DFW02-TVPP4-Unallocated', 'DFW02-TVPP2-VMR-Cluster', 'SYD01-VPP1-Data', 'DFW02-TVPP2-Tahoe-CMS', 'AMS01-VPP1-Management-Cluster', 'SJC02-VPP2-Jabber-Cluster', 'SJC02-TVPP1-vTS-M4-Cluster', 'SJC02-TVPP2-VMR-Cluster', 'SJC02-TVPP4-Unallocated', 'LHR03-VPP1-Infra-Cluster', 'SJC02-VPP3-Miscellaneous-Cluster', 'LHR03-VPP1-Management-Cluster', 'SYD01-VPP1-DBaaS-Cluster', 'LHR03-VPP1-NBRWES-Cluster', 'DFW01-VPP3-Connect-Cluster', 'NRT03-VPP1-MMP', 'DFW02-TVPP2-VTS-Cluster2', 'DFW02-VPP1-Meeting3-Cluster', 'IAD02-VPP1-MMP-Cluster', 'DFW01-VPP2-Connect-Cluster', 'JFK01-VPP1-MMP', 'DFW01-VPP3-Infrastructure-Cluster', 'SIN01-VPP1-Management-Cluster', 'issues', 'DFW02-TVPP4-UCRE', 'DFW02-TVPP1-vTS-M4', 'NRT03-VPP1-QLIK-Cluster', 'SIN01-VPP1-NBRWES2-Cluster', 'NRT03-TVPP1-VMR-Cluster', 'AMS01-VPP1-GSB-MMP-Cluster', 'ORD10-VPP1-MMP-Cluster', 'SIN01-TVPP1-vTS-M4-Cluster', 'AMS01-TVPP1-CloudCenter-Cluster', 'IAD02-VPP1-Infrastructure-Cluster', 'NRT03-TVPP1-CloudCenter-Cluster', 'SJC02-VPP4-Miscellaneous-Cluster', 'DFW02-VPP1-Miscellaneous-Cluster', 'IAD02-VPP1-MMP2-Cluster', 'AMS01-TVPP1-vTS-M4-Cluster', 'SJC02-TVPP3-Unallocated', 'SJC02-TVPP5-VMR-Cluster', 'SJC02-VPP1-Jabber-Cluster', 'LHR03-TVPP1-Unallocated', 'SJC02-VPP2-QLIK', 'SJC02-VPP1-Unallocated', 'DFW02-VPP1-QLIK-Internal', 'LHR03-VPP1-MMP-Cluster', 'SIN01-TVPP1-Unallocated', 'DFW02-TVPP3-Unallocated', 'DFW02-TVPP1-Tahoe-CMS', 'IAD02-VPP1-Redis-Cluster', 'AMS01-VPP1-GSB-Connect-Cluster', 'DFW02-VPP1-DBaaS-Cluster', 'IAD02-VPP1-Management-Cluster', 'DFW01-VPP2-Jabber-Cluster', 'SIN01-VPP1-Meeting-C220-Cluster', 'NRT03-VPP1-Management', 'NRT03-VPP1-Redis-Cluster', 'SJC02-VPP1-Infrastructure-Cluster', 'SJC02-VPP1-WebEx11-Cluster', 'NRT03-VPP1-Unallocated', 'YYZ01-VPP1-Meeting-Cluster', 'SJC02-TVPP1-vTSPOC-Cluster', 'DFW01-VPP2-Meeting-Cluster', 'AMS01-VPP1-DBaaS-Cluster', 'SJC02-VPP3-Kafka-Cluster', 'DFW01-VPP2-Miscellaneous-Cluster', 'SYD01-VPP1-MMP2-Cluster', 'SJC02-VPP1-Redis-Cluster', 'DFW01-VPP3-Meeting-Cluster', 'DFW01-VPP3-CloudCenter-Cluster', 'SJC02-VPP1-Miscellaneous-Cluster', 'SJC02-VPP1-Meeting-NBRWES', 'YYZ01-VPP1-Infrastructure-Cluster', 'DFW01-VPP2-Pod04-Unallocated', 'DFW02-TVPP2-Unallocated', 'SJC02-TVPP2-Unallocated', 'SuperMicro-Testing', 'DFW02-TVPP3-vTS-M4-Cluster2', 'SYD01-VPP1-MMP', 'SJC02-TVPP4-VMR-Cluster', 'DFW02-VPP1-High-CPU-Clister', 'SJC02-VPP2-WebEx11-Cluster', 'SJC02-VPP1-MMP-Cluster4', 'SJC02-TVPP2-Tahoe-CMS', 'AMS01-VPP1-GSB-MMP2-Cluster', 'SIN01-VPP1-MMP-Cluster', 'DFW01-VPP1-Meeting-Cluster', 'AMS01-TVPP1-Tahoe2-CMS', 'AMS01-VPP1-GSB-C220-Meeting-Cluster', 'SIN01-TVPP1-CloudCenter-Cluster', 'DFW01-VPP3-Unallocated', 'AMS01-VPP1-GSB-Miscellaneous-Cluster', 'AMS01-VPP1-GSB-Meeting-Cluster', 'AMS01-VPP1-GSB-Jabber-Cluster', 'Management-Cluster', 'SJC02-DC3-BTS-QLIK-Cluster', 'DFW01-VPP1-Meeting-Cluster-02', 'DFW02-TVPP1-vTSPOC-Cluster', 'YYZ01-VPP1-Miscellaneous-Cluster', 'SIN01-VPP1-Miscellaneous-Cluster', 'DFW01-VPP1-MMP-Cluster', 'SJC02-DC3-BTS-WebEx11-Cluster', 'SJC02-VPP1-Meeting2-Cluster', 'SYD01-VPP1-Infrastructure', 'YYZ01-VPP1-Redis-Cluster', 'JFK01-VPP1-Infra', 'AMS01-TVPP1-CloudCenter-Cluster2', 'LHR03-TVPP1-VMR-Cluster', 'SJC02-VPP2-Meeting-NBRWES', 'LHR03-TVPP1-Tahoe2-CMS', 'SJC02-TVPP1-Tahoe-CMS', 'DFW02-VPP1-Meeting-NBRWES', 'SIN01-VPP1-QLIK-Cluster', 'SJCL1-VPP1-Unallocated', 'DFW02-TVPP5-Unallocated', 'LHR03-VPP1-QLIK-Cluster', 'SJC02-DC3-BTS-Pod03-Unallocated', 'SIN01-TVPP1-VMR-Cluster', 'JFK01-VPP1-NBRWES', 'SJC02-VPP4-Meeting2-Cluster', 'SIN01-VPP1-NBRWES-Cluster3', 'DFW02-VPP1-Unallocated', 'SJC02-VPP3-Meeting3-Cluster', 'SIN01-VPP1-Meeting2-Cluster', 'YYZ01-VPP1-Unallocated', 'LHR03-VPP1-Meeting-Cluster', 'Load-Balancer-Testing', 'SJC02-VPP1-MMP-Cluster3', 'SJC02-VPP1-MMP-Cluster2', 'DFW01-VPP2-Infrastructure-Cluster', 'DFW02-VPP1-Management-Cluster', 'DFW01-VPP1-Jabber-Cluster', 'SJC02-VPP1-NBRWES-C220', 'SYD01-VPP1-Tahoe-CMS', 'NRT03-TVPP1-Unallocated', 'SJC02-VPP4-Jabber-Cluster'] 
unsuccessful_clusters = []

client.query("drop measurement predictions")

for i,clustername in enumerate(clusters):
    try:
        print("\n\n\n\n\n\n"+str(i)+"  "+clustername+"\n\n\n")
        data = get_data(granularity = aggregation_level,end_week= train_period,clustername=clustername,datasrc=datasrc)
        #print(data)
        if len(data)<30:
            continue
        model = Timeseries_Modeling(data)

        pred_df = model.get_predictions_dataframe()
        pred_df["clustername"] = clustername

        try:
            os.remove('predictions.csv')
            os.remove('predictions_influx.csv')
        except:
            print("no prediction file exists")

        pred_df.to_csv("predictions.csv")
        
        exporter.export_csv_to_influx('predictions.csv',
            db_name=db_name,
            db_measurement="predictions",
            field_columns=["cpu","mem","disk"],
            time_column="time",
            db_server_name=server_ip+":8086",
            tag_columns=["clustername"])
    except:
        unsuccessful_clusters.append(clustername)

print(unsuccessful_clusters)


        
