import math
from fbprophet import Prophet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from influxdb import DataFrameClient
from ExportCsvToInflux import ExporterObject

def get_test_timestamps(start_time):
        start_time = pd.Timestamp(start_time)
        end_time = start_time+pd.Timedelta(days=120)
        df = pd.DataFrame(list(pd.date_range(start=start_time, end=end_time,freq='D')))
        df = df.rename(columns={0:'ds'})
        return df


class Timeseries_Modeling:

    def __init__(self, data):
        self.data = data
        self.split_data()
        self.train_test_split()
        self.models = {  }
        self.future_data = {  }
        self.forecast_data = {  }
        self.periods = {  }
        self.freqs = {  }
        self.results = { }
        
    def split_data(self): 
        """
        Separation of different metrics into individual dataframes
        """
   
        self.memory_data = self.data[['Date & Time']]
        self.memory_data.loc[:,'y'] = self.data['mem']
        self.memory_data = self.memory_data.rename(columns={'Date & Time':'ds'})

        self.disk_data = self.data[['Date & Time']]
        self.disk_data.loc[:,'y'] = self.data['disk']
        self.disk_data = self.disk_data.rename(columns={'Date & Time':'ds'})

        self.cpu_data = self.data[['Date & Time']]
        self.cpu_data.loc[:,'y'] = self.data['cpu']
        self.cpu_data = self.cpu_data.rename(columns={'Date & Time':'ds'})

    

    def add_regressors(self,model_kind,regressor_names):
        if model_kind == 'memory':
            self.models['memory'].add_regressor(regressor_name)
            for i in range(len(regressor_names)):
                self.memory_data.loc[:,regressor_names[i]] = self.data[regressor_names[i]]
        elif model_kind == 'cpu':
            self.models['cpu'].add_regressor(regressor)
            for i in range(len(regressor_names)):
                self.cpu_data.loc[:,regressor_names[i]] = self.data[regressor_names[i]]
        elif model_kind == 'disk':
            self.models['disk'].add_regressor(regressor)
            for i in range(len(regressor_names)):
                self.disk_data.loc[:,regressor_names[i]] = self.data[regressor_names[i]]

    def train_test_split(self):
        """
        Train data , Test data split
        80% of data is used for building the model hence stored as training data. Last 20% of data is stored as testing data.
        """

        self.train_memory_data = self.memory_data.loc[:int(len(self.memory_data))]
        start_time = list(self.train_memory_data[len(self.memory_data)-1:]['ds'])[0]
        
        self.test_memory_data = get_test_timestamps(start_time)
        
        self.train_cpu_data = self.cpu_data.loc[:int(len(self.cpu_data))]
        start_time = list(self.train_cpu_data[len(self.cpu_data)-1:]['ds'])[0]

        self.test_cpu_data = get_test_timestamps(start_time)
        
        self.train_disk_data = self.disk_data.loc[:int(len(self.disk_data))]
        start_time = list(self.train_disk_data[len(self.disk_data)-1:]['ds'])[0]

        self.test_disk_data = get_test_timestamps(start_time)
                                          
      

    def create_prophet_model(self,model_kind):
        if model_kind == 'memory':
            self.models['memory'] = Prophet()
        elif model_kind == 'cpu':
            self.models['cpu'] = Prophet()
        elif model_kind == 'disk':
            self.models['disk'] = Prophet()
        elif model_kind == 'all':
            self.models['memory'] = Prophet()
            self.models['cpu'] = Prophet()
            self.models['disk'] = Prophet()

    def train_prophet_model(self,model_kind):
        if model_kind == 'memory':
            self.models['memory'].fit(self.train_memory_data)
        elif model_kind == 'cpu':
            self.models['cpu'].fit(self.train_cpu_data)
        elif model_kind == 'disk':
            self.models['disk'].fit(self.train_disk_data)
        elif model_kind == 'all':
            self.models['memory'].fit(self.train_memory_data)
            self.models['cpu'] = Prophet(self.train_cpu_data)
            self.models['disk'] = Prophet(self.train_disk_data)

    def make_future_dataframe(self,model_kind,periods,freq):
        if model_kind == 'memory':
            self.periods['memory'] = periods
            self.freqs['memory'] = freq
            self.future_data['memory'] = self.models['memory'].make_future_dataframe(periods, freq)
        elif model_kind == 'cpu':
            self.periods['cpu'] = periods
            self.freqs['cpu'] = freq
            self.future_data['cpu'] = self.models['cpu'].make_future_dataframe(periods, freq)
        elif model_kind == 'disk':
            self.periods['disk'] = periods
            self.freqs['disk'] = freq
            self.future_data['disk'] = self.models['disk'].make_future_dataframe(periods, freq)
        elif model_kind == 'all':
            self.periods['memory'] = periods
            self.freqs['memory'] = freq
            self.periods['cpu'] = periods
            self.freqs['cpu'] = freq
            self.periods['disk'] = periods
            self.freqs['disk'] = freq
            self.future_data['memory'] = self.models['memory'].make_future_dataframe(periods, freq)
            self.future_data['cpu'] = self.models['cpu'].make_future_dataframe(periods, freq)
            self.future_data['disk'] = self.models['disk'].make_future_dataframe(periods, freq)
    
    def make_future_dataframe_for_regressors(self,model_kind,regressor_names):
        if model_kind == 'memory':
            self.future_data['memory'].add_regressor(regressor_name)
            for i in range(len(regressor_names)):
                self.future_data['memory'][:,regressor_names[i]] = self.test_memory_data[regressor_columns[i]]
        elif model_kind == 'cpu':
            self.future_data['cpu'].add_regressor(regressor)
            for i in range(len(regressor_names)):
                self.future_data['cpu'][:,regressor_names[i]] = self.test_cpu_data[regressor_columns[i]]
        elif model_kind == 'disk':
            self.future_data['disk'].add_regressor(regressor)
            for i in range(len(regressor_names)):
                self.future_data['disk'][:,regressor_names[i]] = self.test_disk_data[regressor_columns[i]]

    def predict(self, model_kind):
        if model_kind == 'memory':
            self.forecast_data['memory'] = self.models['memory'].predict(self.future_data['memory'])
        elif model_kind == 'cpu':
            self.forecast_data['cpu'] = self.models['cpu'].predict(self.future_data['cpu'])
        elif model_kind == 'disk':
            self.forecast_data['disk'] = self.models['disk'].predict(self.future_data['disk'])
        elif model_kind == 'all':
            self.forecast_data['memory'] = self.models['memory'].predict(self.future_data['memory'])
            self.forecast_data['cpu'] = self.models['cpu'].predict(self.future_data['cpu'])
            self.forecast_data['disk'] = self.models['disk'].predict(self.future_data['disk'])

    def plot(self, model_kind):
        if model_kind == 'memory':
            self.models['memory'].plot(self.forecast_data['memory'], xlabel = 'Date', ylabel = 'Memory-Usage (Kb)')
            plt.title('Memory-Usage (Kb)')
            plt.show()
        elif model_kind == 'cpu':
            self.models['cpu'].plot(self.forecast_data['cpu'], xlabel = 'Date', ylabel = 'CPU-Usage (Kb)')
            plt.title('CPU-Usage (Kb)')
            plt.show()
        elif model_kind == 'disk':
            self.models['disk'].plot(self.forecast_data['disk'], xlabel = 'Date', ylabel = 'Disk-Usage (Kb)')
            plt.title('Disk-Usage (Kb)')
            plt.show()
        elif model_kind == 'all':
            self.models['memory'].plot(self.forecast_data['memory'], xlabel = 'Date', ylabel = 'Memory-Usage (Kb)')
            plt.title('Memory-Usage (Kb)')
            plt.show()
            self.models['cpu'].plot(self.forecast_data['cpu'], xlabel = 'Date', ylabel = 'CPU-Usage (Kb)')
            plt.title('CPU-Usage (Kb)')
            plt.show()
            self.models['disk'].plot(self.forecast_data['disk'], xlabel = 'Date', ylabel = 'Disk-Usage (Kb)')
            plt.title('Disk-Usage (Kb)')
            plt.show()

    def normalise(self, row):
#             row['ds'] = row['ds'].replace(minute=0)
            return row

    def get_results_df(self, model_kind, normalise_function):
        if model_kind == 'memory':
            self.results['memory'] = pd.DataFrame()
            self.results['memory'][['ds','predicted']] = self.forecast_data['memory'][-self.periods['memory']-1 :][['ds','yhat']]
            #self.results['memory'] = self.results['memory'].apply(normalise_function, axis = 1)
            self.results['memory'] = self.results['memory'].set_index('ds')
        elif model_kind == 'cpu':
            self.results['cpu'] = pd.DataFrame()
            self.results['cpu'][['ds','predicted']] = self.forecast_data['cpu'][-self.periods['cpu']-1 :][['ds','yhat']]
            #self.results['cpu'] = self.results['cpu'].apply(normalise_function, axis = 1)
            self.results['cpu'] = self.results['cpu'].set_index('ds')
        elif model_kind == 'disk':
            self.results['disk'] = pd.DataFrame()
            self.results['disk'][['ds','predicted']] = self.forecast_data['disk'][-self.periods['disk']-1 :][['ds','yhat']]
            #self.results['disk'] = self.results['disk'].apply(normalise_function, axis = 1)
            self.results['disk'] = self.results['disk'].set_index('ds')
        elif model_kind == 'all':
            self.results['memory'] = pd.DataFrame()
            self.results['memory'][['ds','predicted']] = self.forecast_data['memory'][-self.periods['memory']-1 :][['ds','yhat']]
            self.results['memory'] = self.results['memory'].apply(normalise_function, axis = 1)
            self.results['memory'] = self.results['memory'].set_index('ds')
            self.results['cpu'] = pd.DataFrame()
            self.results['cpu'][['ds','predicted']] = self.forecast_data['cpu'][-self.periods['cpu']-1 :][['ds','yhat']]
            self.results['cpu'] = self.results['cpu'].apply(normalise_function, axis = 1)
            self.results['cpu'] = self.results['cpu'].set_index('ds')
            self.results['disk'] = pd.DataFrame()
            self.results['disk'][['ds','predicted']] = self.forecast_data['disk'][-self.periods['disk']-1 :][['ds','yhat']]
            self.results['disk'] = self.results['disk'].apply(normalise_function, axis = 1)
            self.results['disk'] = self.results['disk'].set_index('ds')

    def prepare_test_data(self, model_kind, normalise_function):
        if model_kind == 'memory':
            self.test_memory_data['ds'] = pd.to_datetime(self.test_memory_data['ds'])
            #self.test_memory_data = self.test_memory_data.apply(normalise_function, axis = 1)
            self.test_memory_data = self.test_memory_data.reset_index()
            self.test_memory_data = self.test_memory_data.drop(['index'], axis =1)
            #self.test_memory_data = self.test_memory_data.groupby(self.test_memory_data['ds']).aggregate('mean')
        elif model_kind == 'cpu':
            self.test_cpu_data['ds'] = pd.to_datetime(self.test_cpu_data['ds'])
            #self.test_cpu_data = self.test_cpu_data.apply(normalise_function, axis = 1)
            self.test_cpu_data = self.test_cpu_data.reset_index()
            self.test_cpu_data = self.test_cpu_data.drop(['index'], axis =1)
            #self.test_cpu_data = self.test_cpu_data.groupby(self.test_cpu_data['ds']).aggregate('mean')
        elif model_kind == 'disk':
            self.test_disk_data['ds'] = pd.to_datetime(self.test_disk_data['ds'])
            #self.test_disk_data = self.test_disk_data.apply(normalise_function, axis = 1)
            self.test_disk_data = self.test_disk_data.reset_index()
            self.test_disk_data = self.test_disk_data.drop(['index'], axis =1)
            #self.test_disk_data = self.test_disk_data.groupby(self.test_disk_data['ds']).aggregate('mean')
        elif model_kind == 'all':
            self.test_memory_data['ds'] = pd.to_datetime(self.test_memory_data['ds'])
            self.test_memory_data = self.test_memory_data.apply(normalise_function, axis = 1)
            self.test_memory_data = self.test_memory_data.reset_index()
            self.test_memory_data = self.test_memory_data.drop(['index'], axis =1)
            self.test_memory_data = self.test_memory_data.groupby(self.test_memory_data['ds']).aggregate('mean')

            self.test_cpu_data['ds'] = pd.to_datetime(self.test_cpu_data['ds'])
            self.test_cpu_data = self.test_cpu_data.apply(normalise_function, axis = 1)
            self.test_cpu_data = self.test_cpu_data.reset_index()
            self.test_cpu_data = self.test_cpu_data.drop(['index'], axis =1)
            self.test_cpu_data = self.test_cpu_data.groupby(self.test_cpu_data['ds']).aggregate('mean')
            
            self.test_disk_data['ds'] = pd.to_datetime(self.test_disk_data['ds'])
            self.test_disk_data = self.test_disk_data.apply(normalise_function, axis = 1)
            self.test_disk_data = self.test_disk_data.reset_index()
            self.test_disk_data = self.test_disk_data.drop(['index'], axis =1)
            self.test_disk_data = self.test_disk_data.groupby(self.test_disk_data['ds']).aggregate('mean')

    def get_prediction_accuracy(self,model_kind):
        if model_kind == 'memory':
            self.results['memory']['actual'] = self.test_memory_data['y']
            self.results['memory']['squared_error'] = (self.results['memory']['actual'] - self.results['memory']['predicted'])**2
            self.results['memory']['accuracy'] = 100 - abs(self.results['memory']['actual'] - self.results['memory']['predicted'])/self.results['memory']['actual']*100
            self.results['memory'].squared_error.mean()
        elif model_kind == 'cpu':
            self.results['cpu']['actual'] = self.test_cpu_data['y']
            self.results['cpu']['squared_error'] = (self.results['cpu']['actual'] - self.results['cpu']['predicted'])**2
            self.results['cpu']['accuracy'] = 100 - abs(self.results['cpu']['actual'] - self.results['cpu']['predicted'])/self.results['cpu']['actual']*100
            self.results['cpu'].squared_error.mean()
        elif model_kind == 'disk':
            self.results['disk']['actual'] = self.test_disk_data['y']
            self.results['disk']['squared_error'] = (self.results['disk']['actual'] - self.results['disk']['predicted'])**2
            self.results['disk']['accuracy'] = 100 - abs(self.results['disk']['actual'] - self.results['disk']['predicted'])/self.results['disk']['actual']*100
            self.results['disk'].squared_error.mean()
        elif model_kind == 'all':
            self.results['memory']['actual'] = self.test_memory_data['y']
            self.results['memory']['squared_error'] = (self.results['memory']['actual'] - self.results['memory']['predicted'])**2
            self.results['memory']['accuracy'] = 100 - abs(self.results['memory']['actual'] - self.results['memory']['predicted'])/self.results['memory']['actual']*100
            self.results['memory'].squared_error.mean()

            self.results['cpu']['actual'] = self.test_cpu_data['y']
            self.results['cpu']['squared_error'] = (self.results['cpu']['actual'] - self.results['cpu']['predicted'])**2
            self.results['cpu']['accuracy'] = 100 - abs(self.results['cpu']['actual'] - self.results['cpu']['predicted'])/self.results['cpu']['actual']*100
            self.results['cpu'].squared_error.mean()

            self.results['disk']['actual'] = self.test_disk_data['y']
            self.results['disk']['squared_error'] = (self.results['disk']['actual'] - self.results['disk']['predicted'])**2
            self.results['disk']['accuracy'] = 100 - abs(self.results['disk']['actual'] - self.results['disk']['predicted'])/self.results['disk']['actual']*100
            self.results['disk'].squared_error.mean()


    def remove_hours(row):
        row['ds'] = row['ds'].replace(hour=0)
        return row


    def remove_mins(row):
        row['ds'] = row['ds'].replace(minute=0)
        return row


    def remove_secs(row):
        row['ds'] = row['ds'].replace(second=0)
        return row


    def get_predictions_dataframe(self):
        pred_df = pd.DataFrame()
        self.create_prophet_model('disk')
        self.train_prophet_model('disk')
        self.prepare_test_data('disk', self.remove_mins)
        self.make_future_dataframe('disk',len(self.test_disk_data)*24,'H')
        self.predict('disk')
        self.get_results_df('disk',self.remove_mins)
        predictions = self.results['disk']
        predictions = predictions.reset_index()
        pred_df['time'] = predictions['ds']
        pred_df['disk'] = predictions['predicted']

        
        self.create_prophet_model('memory')
        self.train_prophet_model('memory')
        self.prepare_test_data('memory', self.remove_mins)
        self.make_future_dataframe('memory',len(self.test_memory_data)*24,'H')
        self.predict('memory')
        self.get_results_df('memory',self.remove_mins)
        predictions = self.results['memory']
        predictions = predictions.reset_index()
        pred_df['mem'] = predictions['predicted']
        

        self.create_prophet_model('cpu')
        self.train_prophet_model('cpu')
        self.prepare_test_data('cpu', self.remove_mins)
        self.make_future_dataframe('cpu',len(self.test_cpu_data)*24,'H')
        self.predict('cpu')
        self.get_results_df('cpu',self.remove_mins)
        predictions = self.results['cpu']
        predictions = predictions.reset_index()
        pred_df['cpu'] = predictions['predicted']

        return pred_df



