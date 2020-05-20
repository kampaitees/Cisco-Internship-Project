import requests
import pandas as pd
import time
import os
from ExportCsvToInflux import ExporterObject
from influxdb import DataFrameClient


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


class VROM_API():

    def __init__(self,vrom_options,local_options):
        self.username  = vrom_options["username"]
        self.authsrc   = vrom_options["authsrc"]
        self.password  = vrom_options["password"]
        self.days      = vrom_options["days"]
        self.server_ip = local_options["server_ip"]
        self.dbname    = local_options["db_name"]



    def get_token(self):

        url = "https://vrom1.webex.com/suite-api/api/auth/token/acquire"

        payload = "{\n\t\"username\" : \""+self.username+"\",\n\t\"authSource\" : \""+self.authsrc+"\",\n\t\"password\" : \""+self.password+"\"\n}"
        headers = {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data = payload,verify=False)
        r = dict(response.json())
        return r["token"]




    def get_cluster_id(self,token):
        url = "https://vrom1.webex.com/suite-api/api/resources?resourceKind=ClusterComputeResource"

        payload = {}
        headers = {
          'Accept': 'application/json',
          'Authorization': 'vRealizeOpsToken '+token
        }

        response = requests.request("GET", url, headers=headers, data = payload,verify=False)
        r = dict(response.json())

        resourceId = dict()
        for cluster in r["resourceList"]:
            resourceId[cluster['resourceKey']['name']]=cluster["identifier"]
        return resourceId




    def get_stat_keys(self,token,resourceId):
        url = "https://vrom1.webex.com/suite-api/api/resources/"+resourceId+"/statkeys"

        payload = {}
        headers = {
          'Accept': 'application/json',
          'Authorization': 'vRealizeOpsToken '+token
        }

        response = requests.request("GET", url, headers=headers, data = payload,verify=False)
        r = dict(response.json())

        stat_keys = []
        for d in r["stat-key"]:
            stat_keys.append(d["key"])
        return stat_keys





    def get_stat_value(self,token,resourceId):

        end =  int(round(time.time() * 1000))
        start = end - 24*60*60*1000*self.days

        url = "https://vrom1.webex.com/suite-api/api/resources/stats/query"

        payload = "{\n\t\"begin\":"+str(start)+",\n\t\"end\":"+str(end)+",\n\t\"intervalType\":\"DAYS\",\n\t\"intervalQuantifier\":1,\n\t\"rollUpType\":\"AVG\",\n\t\"resourceId\":[\n\t\t\""+resourceId+"\"\n\t],\n\t\"statKey\": [\n\t\t\"mem|usage_average\",\"cpu|usagemhz_average\",\"diskspace|total_usage\"\n\t]\n}"
        headers = {
          'Accept': 'application/json',
          'Authorization': 'vRealizeOpsToken '+token,
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data = payload,verify=False)
        r = dict(response.json())
        if len(r["values"][0]["stat-list"]['stat'])<3:
            return []
        timestamps  = []
        timestamps1 = r["values"][0]["stat-list"]['stat'][0]['timestamps']
        timestamps2 = r["values"][0]["stat-list"]['stat'][1]['timestamps']
        timestamps3 = r["values"][0]["stat-list"]['stat'][2]['timestamps']

        print(len(timestamps1),len(timestamps2),len(timestamps3))
        if len(timestamps)<len(timestamps1):
            timestamps = timestamps1
        if len(timestamps)<len(timestamps2):
            timestamps = timestamps2
        if len(timestamps)<len(timestamps3):
            timestamps = timestamps3

        mem  = (len(timestamps)-len(timestamps1))*[0]+r["values"][0]["stat-list"]['stat'][0]['data']
        cpu  = (len(timestamps)-len(timestamps2))*[0]+r["values"][0]["stat-list"]['stat'][1]['data']
        disk = (len(timestamps)-len(timestamps3))*[0]+r["values"][0]["stat-list"]['stat'][2]['data']
        print(len(mem),len(cpu),len(disk))
        df = pd.DataFrame()

        df["timestamps"] = timestamps
        df["mem"] = mem
        df["cpu"] = cpu
        df["disk"] = disk

        return df





    def influx_transfer(self,clustername):
        exporter = ExporterObject()
        if not os.path.exists(clustername+".csv"):
            return
        try:
            os.remove(clustername+"_influx.csv")
        except:
            print("transfer index 1")
        exporter.export_csv_to_influx(clustername+'.csv',
                                      db_name=self.dbname,
                                      db_measurement="vROM",
                                      field_columns = ["mem","cpu","disk"],
                                      time_column = "timestamps",
                                      tag_columns = ["clustername"],
                                      db_server_name=self.server_ip+":8086")






    def update_data(self):

        token = get_token()
        resourceId = get_cluster_id(token)

        #stat_keys = get_stat_keys(token,resourceId['AMS01-TVPP1-VMR-Cluster'])
        #df = get_stat_value(token,resourceId['SJC02-TVPP5-Unallocated'],210)

        clusters = list(resourceId.keys())

        for cluster in clusters:
            df = get_stat_value(token,resourceId[cluster])
            df["clustername"] = cluster
            try:
                os.remove(cluster+".csv")
            except:
                print(cluster+" doesnt exist")
            if len(df)!=0:
                df.to_csv(cluster+".csv")
                print(cluster+" written")

        dropped_clusters = []
        for cluster in clusters:
            try:
                influx_transfer(cluster)
                print(cluster)
            except:
                print(cluster +" unsuccessful transfer")
                dropped_clusters.append(cluster)

        print(dropped_clusters)



class ALPHA():

    def __init__(self,alpha_options,local_options):
        self.username  = alpha_options["username"]
        self.password  = alpha_options["password"]
        self.interval  = alpha_options["interval"]
        self.alpha_dbname = alpha_options["db_name"]
        self.alpha_ip  = alpha_options["server_ip"]
        self.server_ip = local_options["server_ip"]
        self.dbname    = local_options["db_name"]
        
        
        
    def local_transfer(self,df,metric,fields,tags):
        
        client = DataFrameClient(host=self.server_ip,port=8086)
        client.switch_database(self.dbname)
        exporter = ExporterObject()
        try:
            os.remove("temp.csv")
            os.remove("temp_influx.csv")
        except:
            print("transfer index 1")
            
        df = df.reset_index()
        df = df.rename(columns={'index':'time'})
        
        df['time'] = df['time'].astype('str')
        for i in range(len(df)):
            df.loc[i,"time"] = df.loc[i,"time"][:-6]
        df.to_csv("temp.csv")
        #client.query("drop measurement "+metric)
        print(str(self.server_ip)+":8086")
        exporter.export_csv_to_influx('temp.csv',
            db_name="DSP",
            db_measurement=metric,
            field_columns=fields,
            tag_columns=tags,
            time_column="time",
            db_server_name=str(self.server_ip)+":8086")
            
            
        
    def get_alpha_data(self):
    
        cpu_alpha_query = "select usagemhz_average,esxhostname,clustername from vsphere_host_cpu where time>now()-"+self.interval+" and cpu='instance-total'"
        
        mem_alpha_query = "select consumed_average,esxhostname,clustername from vsphere_host_mem where time>now()-"+self.interval
        
        disk_alpha_query = "select used_latest,dsname from vsphere_datastore_disk where time>now()-"+self.interval+" and disk='instance-total'"
        
        alpha_client = DataFrameClient(host=self.alpha_ip,port=8086,username=self.username,password=self.password)

        alpha_client.switch_database(self.alpha_dbname)

        cpu = alpha_client.query(cpu_alpha_query)['vsphere_host_cpu'] 
        self.local_transfer(cpu,"cpu",["usagemhz_average"],["esxhostname","clustername"])

        mem = alpha_client.query(mem_alpha_query)['vsphere_host_mem']
        self.local_transfer(mem,"mem",["consumed_average"],["esxhostname","clustername"])
        
        disk = alpha_client.query(disk_alpha_query)['vsphere_datastore_disk']
        self.local_transfer(disk,"disk",["used_latest"],["dsname"])
        
    
    def disk_rollup(self):
        
        client = DataFrameClient(host=self.server_ip,port=8086)
        client.switch_database(self.dbname)
        clusters = ['YYZ01-VPP1-Management-Cluster', 'SJC02-VPP2-TPGW-Cluster', 'SYD01-VPP1-Management-Cluster', 'SJC02-VPP3-Meeting4-Cluster', 'LHR03-TVPP1-Tahoe-CMS', 'SJC02-DC3-BTS-NBRWES', 'SYD01-VPP1-CloudCenter-Cluster', 'DFW01-VPP1-CloudCenter-Cluster', 'SJC02-VPP1-DBaaS-Cluster', 'DFW01-VPP1-Management-Cluster', 'SJC02-VPP2-Connect-Cluster', 'SJC02-TVPP2-VTS-Cluster', 'NRT03-VPP1-DBaaS-Cluster', 'SJC02-VPP3-Infrastructure-Cluster', 'mitajon-hosts', 'DFW02-TVPP2-VTS-Cluster', 'SJC02-TVPP1-CloudCenter2-Cluster', 'AMS01-VPP1-Redis-Cluster', 'LHR03-VPP1-Jabber-Cluster', 'DFW02-TVPP1-Management-Cluster', 'SJC02-TVPP3-vTS-M4-Cluster2', 'LHR03-VPP1-MMP-Cluster4', 'DFW01-VPP2-SDE-Cluster', 'DFW01-VPP1-Unallocated', 'IAD02-VPP1-MMP3-Cluster', 'SJC02-VPP1-Connect-Cluster', 'AMS01-TVPP1-Tahoe-CMS', 'DFW02-VPP1-MMP-Cluster', 'SYD01-VPP1-Voice', 'SJC02-DC3-BTS-SearchFarm-Cluster', 'SJC02-VPP2-Meeting2-Cluster', 'SJC02-VPP1-Management-Cluster', 'SJC02-TVPP2-CloudCenter-Cluster', 'SJC02-TVPP2-VTS-Cluster2', 'DFW02-VPP1-QLIK-Cluster', 'JFK01-VPP1-Unallocated', 'DFW02-VPP1-Meeting2-Cluster', 'SYD10-IPOP1-C-Series-Unallocated', 'SJC02-TVPP1-VMR-Cluster', 'DFW02-TVPP1-VMR-Cluster', 'AMS01-VPP1-GSB-Killers-Cluster', 'JFK01-VPP1-Management', 'IAD02-VPP1-Powered-Off', 'DFW02-TVPP2-CloudCenter-Cluster', 'DFW01-VPP3-Jabber-Cluster', 'NRT03-TVPP1-Tahoe-CMS', 'DFW02-TVPP5-Management-Cluster', 'SJC02-DC3-BTS-Management-Cluster', 'DFW01-VPP3-MMP-Cluster', 'SJC02-TVPP2-CloudCenter-Cluster(migrate2sj2t)', 'SJC02-VPP2-Management-Cluster', 'SJC02-DC3-BTS-Miscellaneous-Cluster', 'CloudCenterTest1', 'SJC02-TVPP3-Management-Cluster', 'SIN01-VPP1-Unallocated', 'LHR03-VPP1-Redis-Cluster', 'LHR03-VPP1-Killers-Cluster', 'SJC02-VPP4-Connect-Cluster', 'LHR03-VPP1-MMP-Cluster2', 'DFW02-TVPP5-vTS-Cluster', 'DFW01-VPP3-Miscellaneous-Cluster', 'SIN01-VPP1-MMP-Cluster2', 'AMS01-TVPP1-Unallocated', 'SJC02-TVPP5-vTS-Cluster', 'DFW02-VPP1-CloudCenter-Cluster', 'SIN01-VPP1-Redis-Cluster', 'NRT03-TVPP1-vTS-M4-Cluster', 'DFW02-VPP1-Infrastructure-Cluster', 'SJC02-DC3-BTS-Connect-Cluster', 'LHR03-TVPP1-vTS-M4-Cluster', 'DFW02-TVPP2-Management-Cluster', 'LabOCP', 'CloudCenterTest2', 'LHR03-VPP1-MMP-Cluster3', 'DFW01-VPP2-Management-Cluster', 'SJC02-TVPP4-Management-Cluster', 'LHR03-VPP1-Unallocated', 'DFW01-VPP3-Management-Cluster', 'DFW01-VPP1-Redis-Cluster', 'SJC02-TVPP5-Management-Cluster', 'AMS01-VPP1-GSB-Infrastructure-Cluster', 'DFW01-VPP1-MMP-Cluster2', 'DFW02-TVPP3-Management-Cluster', 'IAD02-VPP1-NBRWES', 'SJC02-VPP4-Unallocated', 'DFW01-VPP3-WebEx11-Cluster', 'SJC02-VPP1-MMP-Cluster', 'SJC02-VPP4-Management-Cluster', 'AMS01-VPP1-GSB-QLIK-Cluster', 'AMS01-VPP1-Performance', 'SJC02-DC3-BTS-Jabber-Cluster', 'SJC02-VPP3-Meeting2-Cluster', 'SJC02-VPP1-Meeting-Cluster', 'SJC02-DC3-BTS-Infrastructure-Cluster', 'AMS01-TVPP1-Management-Cluster', 'ORD10-VPP1-Management-Cluster', 'DFW01-VPP2-WebEx11-Cluster', 'DFW01-VPP2-MMP-Cluster', 'SJC02-DC3-BTS-Pod19-Unallocated', 'DFW01-VPP2-QLIK-Cluster', 'AMS01-VPP1-GSB-CloudCenter-Cluster', 'SJC02-VPP2-Meeting-Cluster', 'NRT03-TVPP1-Management-Cluster', 'SJC02-VPP7-Acano-Cluster', 'IAD02-VPP1-Meeting2-Cluster', 'SJC02-TVPP2-Management-Cluster', 'DFW01-VPP1-WebEx11-Cluster', 'DFW02-TVPP4-Management-Cluster', 'DFW02-TVPP1-CloudCenter-Cluster', 'DFW01-VPP1-Infrastructure-Cluster', 'SJC02-VPP3-Management-Cluster', 'SJC02-VPP3-Meeting-Cluster', 'LHR03-TVPP1-Management-Cluster', 'ORD10-VPP1-Unallocated', 'SYD01-VPP1-Redis-Cluster', 'SJC02-TVPP3-vTS-M4-Cluster', 'SJC02-VPP1-CloudCenter-Cluster', 'Automation-CI-Services', 'AMS01-VPP1-GSB-MMP3-Cluster', 'NRT03-VPP1-Infrastructure-Cluster', 'IAD02-VPP1-Meeting-Cluster', 'SJC02-VPP4-NBRWES-Cluster', 'SJC02-TVPP2-Tahoe-CMS(migrate2sj2t)', 'SJC02-TVPP1-Management-Cluster', 'LHR03-VPP1-Meeting-C220-Cluster', 'LHR03-VPP1-CloudCenter-Cluster', 'DFW02-VPP1-Meeting-NBRWES2', 'AMS01-VPP1-GSB-WebEx11-Cluster', 'DFW02-VPP1-Meeting-Cluster', 'DFW02-TVPP3-vTS-Cluster', 'SYD10-IPOP1-Meeting2-Cluster', 'NRT03-VPP1-CloudCenter-Cluster', 'allclusters', 'SIN01-VPP1-Meeting-Cluster', 'SJC02-VPP2-Miscellaneous-Cluster', 'SJC02-VPP4-Meeting-Cluster', 'SJC02-TVPP1-CloudCenter-Cluster', 'SJC02-VPP2-QLIK-Internal', 'SJC02-DC3-BTS-Meeting-Cluster', 'SIN01-TVPP1-Management-Cluster', 'SJC02-TVPP3-vTS-Cluster', 'IAD02-VPP1-Unallocated', 'SJC02-VPP2-Infrastructure-Cluster', 'AMS01-TVPP1-VMR-Cluster', 'SJC02-VPP7-Management-Cluster', 'DFW01-VPP1-Miscellaneous-Cluster', 'JFK01-VPP1-MMP3', 'AMS01-VPP1-Unallocated', 'ORD10-VPP1-Meeting-Cluster', 'NRT03-VPP1-MMP2', 'SYD01-VPP1-NBRWES', 'DFW02-VPP1-Redis-Cluster', 'LHR03-TVPP1-CloudCenter-Cluster2', 'SJC02-VPP7-Meeting-Cluster', 'NRT03-VPP1-Meeting', 'NSX-POC', 'DFW02-TVPP3-vTS-M4-Cluster', 'SJC02-TVPP1-Unallocated', 'LHR03-TVPP1-CloudCenter-Cluster', 'JFK01-VPP1-Standard-Tier', 'SJC02-VPP3-Unallocated', 'LHR03-VPP1-WebEx11-Cluster', 'JFK01-VPP1-MMP2', 'SIN01-VPP1-Infrastructure-Cluster', 'DFW02-TVPP1-Unallocated', 'LHR03-VPP1-NBRWES-Cluster2', 'DFW01-VPP1-Connect-Cluster', 'SYD10-IPOP1-Management-Cluster', 'IaaS-Storage', 'SIN01-VPP1-NBRWES-Cluster', 'SJC02-VPP1-NBRWES2-Cluster', 'SYD01-VPP1-Unallocated', 'Test_Cluster', 'DFW02-TVPP4-Unallocated', 'DFW02-TVPP2-VMR-Cluster', 'SYD01-VPP1-Data', 'DFW02-TVPP2-Tahoe-CMS', 'AMS01-VPP1-Management-Cluster', 'SJC02-VPP2-Jabber-Cluster', 'SJC02-TVPP1-vTS-M4-Cluster', 'SJC02-TVPP2-VMR-Cluster', 'SJC02-TVPP4-Unallocated', 'LHR03-VPP1-Infra-Cluster', 'SJC02-VPP3-Miscellaneous-Cluster', 'LHR03-VPP1-Management-Cluster', 'SYD01-VPP1-DBaaS-Cluster', 'LHR03-VPP1-NBRWES-Cluster', 'DFW01-VPP3-Connect-Cluster', 'NRT03-VPP1-MMP', 'DFW02-TVPP2-VTS-Cluster2', 'DFW02-VPP1-Meeting3-Cluster', 'IAD02-VPP1-MMP-Cluster', 'DFW01-VPP2-Connect-Cluster', 'JFK01-VPP1-MMP', 'DFW01-VPP3-Infrastructure-Cluster', 'SIN01-VPP1-Management-Cluster', 'issues', 'DFW02-TVPP4-UCRE', 'DFW02-TVPP1-vTS-M4', 'NRT03-VPP1-QLIK-Cluster', 'SIN01-VPP1-NBRWES2-Cluster', 'NRT03-TVPP1-VMR-Cluster', 'AMS01-VPP1-GSB-MMP-Cluster', 'ORD10-VPP1-MMP-Cluster', 'SIN01-TVPP1-vTS-M4-Cluster', 'AMS01-TVPP1-CloudCenter-Cluster', 'IAD02-VPP1-Infrastructure-Cluster', 'NRT03-TVPP1-CloudCenter-Cluster', 'SJC02-VPP4-Miscellaneous-Cluster', 'DFW02-VPP1-Miscellaneous-Cluster', 'IAD02-VPP1-MMP2-Cluster', 'AMS01-TVPP1-vTS-M4-Cluster', 'SJC02-TVPP3-Unallocated', 'SJC02-TVPP5-VMR-Cluster', 'SJC02-VPP1-Jabber-Cluster', 'LHR03-TVPP1-Unallocated', 'SJC02-VPP2-QLIK', 'SJC02-VPP1-Unallocated', 'DFW02-VPP1-QLIK-Internal', 'LHR03-VPP1-MMP-Cluster', 'SIN01-TVPP1-Unallocated', 'DFW02-TVPP3-Unallocated', 'DFW02-TVPP1-Tahoe-CMS', 'IAD02-VPP1-Redis-Cluster', 'AMS01-VPP1-GSB-Connect-Cluster', 'DFW02-VPP1-DBaaS-Cluster', 'IAD02-VPP1-Management-Cluster', 'DFW01-VPP2-Jabber-Cluster', 'SIN01-VPP1-Meeting-C220-Cluster', 'NRT03-VPP1-Management', 'NRT03-VPP1-Redis-Cluster', 'SJC02-VPP1-Infrastructure-Cluster', 'SJC02-VPP1-WebEx11-Cluster', 'NRT03-VPP1-Unallocated', 'YYZ01-VPP1-Meeting-Cluster', 'SJC02-TVPP1-vTSPOC-Cluster', 'DFW01-VPP2-Meeting-Cluster', 'AMS01-VPP1-DBaaS-Cluster', 'SJC02-VPP3-Kafka-Cluster', 'DFW01-VPP2-Miscellaneous-Cluster', 'SYD01-VPP1-MMP2-Cluster', 'SJC02-VPP1-Redis-Cluster', 'DFW01-VPP3-Meeting-Cluster', 'DFW01-VPP3-CloudCenter-Cluster', 'SJC02-VPP1-Miscellaneous-Cluster', 'SJC02-VPP1-Meeting-NBRWES', 'YYZ01-VPP1-Infrastructure-Cluster', 'DFW01-VPP2-Pod04-Unallocated', 'DFW02-TVPP2-Unallocated', 'SJC02-TVPP2-Unallocated', 'SuperMicro-Testing', 'DFW02-TVPP3-vTS-M4-Cluster2', 'SYD01-VPP1-MMP', 'SJC02-TVPP4-VMR-Cluster', 'DFW02-VPP1-High-CPU-Clister', 'SJC02-VPP2-WebEx11-Cluster', 'SJC02-VPP1-MMP-Cluster4', 'SJC02-TVPP2-Tahoe-CMS', 'AMS01-VPP1-GSB-MMP2-Cluster', 'SIN01-VPP1-MMP-Cluster', 'DFW01-VPP1-Meeting-Cluster', 'AMS01-TVPP1-Tahoe2-CMS', 'AMS01-VPP1-GSB-C220-Meeting-Cluster', 'SIN01-TVPP1-CloudCenter-Cluster', 'DFW01-VPP3-Unallocated', 'AMS01-VPP1-GSB-Miscellaneous-Cluster', 'AMS01-VPP1-GSB-Meeting-Cluster', 'AMS01-VPP1-GSB-Jabber-Cluster', 'Management-Cluster', 'SJC02-DC3-BTS-QLIK-Cluster', 'DFW01-VPP1-Meeting-Cluster-02', 'DFW02-TVPP1-vTSPOC-Cluster', 'YYZ01-VPP1-Miscellaneous-Cluster', 'SIN01-VPP1-Miscellaneous-Cluster', 'DFW01-VPP1-MMP-Cluster', 'SJC02-DC3-BTS-WebEx11-Cluster', 'SJC02-VPP1-Meeting2-Cluster', 'SYD01-VPP1-Infrastructure', 'YYZ01-VPP1-Redis-Cluster', 'JFK01-VPP1-Infra', 'AMS01-TVPP1-CloudCenter-Cluster2', 'LHR03-TVPP1-VMR-Cluster', 'SJC02-VPP2-Meeting-NBRWES', 'LHR03-TVPP1-Tahoe2-CMS', 'SJC02-TVPP1-Tahoe-CMS', 'DFW02-VPP1-Meeting-NBRWES', 'SIN01-VPP1-QLIK-Cluster', 'SJCL1-VPP1-Unallocated', 'DFW02-TVPP5-Unallocated', 'LHR03-VPP1-QLIK-Cluster', 'SJC02-DC3-BTS-Pod03-Unallocated', 'SIN01-TVPP1-VMR-Cluster', 'JFK01-VPP1-NBRWES', 'SJC02-VPP4-Meeting2-Cluster', 'SIN01-VPP1-NBRWES-Cluster3', 'DFW02-VPP1-Unallocated', 'SJC02-VPP3-Meeting3-Cluster', 'SIN01-VPP1-Meeting2-Cluster', 'YYZ01-VPP1-Unallocated', 'LHR03-VPP1-Meeting-Cluster', 'Load-Balancer-Testing', 'SJC02-VPP1-MMP-Cluster3', 'SJC02-VPP1-MMP-Cluster2', 'DFW01-VPP2-Infrastructure-Cluster', 'DFW02-VPP1-Management-Cluster', 'DFW01-VPP1-Jabber-Cluster', 'SJC02-VPP1-NBRWES-C220', 'SYD01-VPP1-Tahoe-CMS', 'NRT03-TVPP1-Unallocated', 'SJC02-VPP4-Jabber-Cluster'] 
        application = ['meeting', 'webex11', 'infra', 'jabber', 'ccc', 'connect', 'redis', 'mmp', 'qlik', 'jabber', 'killers', 'nbrwes', 'infrastructure', 'mgt', 'cloudcenter', 'vmr', 'dbaas', 'tahoe', 'vts', 'management']

        for c in clusters:
            cluster = c.lower();
            for app in application:
                if app in cluster:
                    dc = c.split("-")
                    if(len(dc)<3):
                        continue
                    client.query("drop measurement disk_temp")
                    query = "select sum(x) as x into disk_temp from (select mean(used_latest) as x from \"disk\" where time>now()-1d and dsname=~ /(?i)"+dc[0]+"-"+dc[1]+".*"+app+".*/ "+ "group by dsname,time(1d)) group by time(1d)"
                    print(query)
                    client.query(query)
                    query = "select x from disk_temp"
                    df = client.query(query)
                    if "disk_temp" not in df:
                        continue
                    df = df["disk_temp"]
                    df["clustername"] = c
                    self.local_transfer(df,"disk_final",["x"],["clustername"])
        
    def update_data(self):
        
        local_client = DataFrameClient(host=self.server_ip,port=8086)

        local_client.switch_database(self.dbname)
        
        temp_measurements = ["cpu","disk","disk_final","mem","test"]
        
        for m in temp_measurements:
            local_client.query("drop measurement "+m)
        
        self.get_alpha_data()
        
        self.disk_rollup()
        

        cpu_local_query  = "select sum(x) as cpu into test from (select mean(usagemhz_average) as x from cpu where time>now()-1d group by time(1d),esxhostname,clustername) group by time(1d),clustername"

        mem_local_query  = "select sum(x) as mem into test from (select mean(consumed_average) as x from mem where time>now()-1d group by time(1d),esxhostname,clustername) group by time(1d),clustername"

        disk_local_query = "select mean(x) as disk into test from disk_final group by clustername,time(1d)"
        
        
        
        local_client.query("drop measurement test")

        local_client.query(cpu_local_query)

        local_client.query(mem_local_query)
        
        local_client.query(disk_local_query)
        
        local_client.query("select * into alpha from test group by clustername")
        

datasrc = ALPHA(ConfigSectionMap("Alpha_options"),ConfigSectionMap("Local_options"))
df = datasrc.update_data()
# data_stream = VROM_API()
# data_stream.update_data()
# data_stream = ALPHA()
# data_stream.update_data()












