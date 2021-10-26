import os
import datetime
import glob
from itertools import groupby
import pandas as pd
import xarray as xr
from functools import reduce



class cordex_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.lat_lon_csv = r"/home/hsaf/Ortak/CORDEX/all/tas/lat_lon.csv"
        self.lat_lon = None
        self.parameter = None
        self.cordex_data_folder = r"/home/hsaf/Ortak/CORDEX/all/pr/"
        self.csv_output_folder = r"/home/hsaf/Ortak/CORDEX/all/pr" \
                                 r"/output"
        self.files_names = []
        self.models=[]
        self.csv_file_names = None
        self.parameter ="pr"
        self.grouped_data = []
        self.merged_df_list = []
        self.model_names_scenario_list = []
    def print_duration(self):
        return print("Total Process time:  {}".format(str(self.end_time - self.start_time)))

    def read_lat_lon_info(self):
        self.lat_lon = pd.read_csv(self.lat_lon_csv)
        return self.lat_lon

    def traverse_rcm(self):
        self.files_names = sorted(list(glob.iglob(os.path.join(self.cordex_data_folder,'**',self.parameter+'*.nc'),recursive=True)))
        return self.files_names

    def grouping(self):
        new_files_names =[]
        for file in self.files_names:
            new_files_names.append(file.split(sep="/")[-1])
        del file

        new_files_names = sorted(new_files_names)
        iterator = groupby(new_files_names, lambda string: string.split('-')[0:6])
        for key, group in groupby(new_files_names, lambda x: x.split('_')[0:6]):
            self.models.append(list(group))
        del key, group
        model_names = []
        for model in self.models:
            model_names.append('_'.join(model[0].split('_')[0:8]))

        self.model_dictionary = {}
        for i in range(len(self.models)):
            self.model_dictionary[model_names[i]] = self.models[i]

        for keys in self.model_dictionary.keys():
            for i in range(len(self.model_dictionary[keys])):
                path = os.path.join(self.cordex_data_folder,keys,self.model_dictionary[keys][i])
                self.model_dictionary[keys][i] = path

    def extract_data(self):
        xr.set_options(display_width=70)
        stations = self.lat_lon.values.tolist()
        count = 0
        for keys in self.model_dictionary.keys():
            d ={'time':[],self.parameter:[]}
            model_data = pd.DataFrame(d)
            for station in stations:
                count = count + 1
                for i in range(len(self.model_dictionary[keys])):
                    data = xr.open_dataset(self.model_dictionary[keys][i])
                    if (data._coord_names.__contains__('rlat')):
                        st = data[self.parameter].sel(rlat=station[3],rlon=station[4],method='nearest').hvplot().data[['time',self.parameter]]
                        model_data = model_data.append(st)
                    else:
                        data.close()
                        break
                path_name = os.path.join(self.csv_output_folder,(keys+'_'+station[0]+'.csv'))
                model_data.to_csv(path_name,index=False)
                percent = (count / (len(self.model_dictionary.keys())*len(stations)))*100
                print(f"%{percent:.2f} is done!")
                print(f"{len(self.model_dictionary.keys())*len(stations)-count} left!")

    def traverse_csv(self):

        self.csv_file_names = sorted(list(glob.iglob(os.path.join(self.csv_output_folder,'**',('*'+self.station_name+'*.csv')),recursive=True)))
        #for i in range(len(self.csv_file_names)):
        #    self.csv_file_names[i] = self.csv_file_names[i].split(sep='/')[-1]
        historical_list = [k for k in self.csv_file_names if 'historical' in k]
        rcp45 = [k for k in self.csv_file_names if 'rcp45' in k]
        rcp85 = [k for k in self.csv_file_names if 'rcp85' in k]
        self.grouped_data = [historical_list, rcp45, rcp85]
        del historical_list, rcp45, rcp85
        return self.grouped_data,self.csv_file_names

    def merge_csv(self):
        for scenario in self.grouped_data:
            scenario_list_data = []
            model_names = []
            for i in range(len(scenario)):
                model_names.append(scenario[i].split(sep='/')[-1])
                scenario_list_data.append(pd.read_csv(scenario[i]))
            self.merged_df = reduce(lambda left, right: pd.merge(left, right, on=['time'],
                                                                 how='left'), scenario_list_data)
            new_column_names = ['time']
            new_column_names.extend(model_names)
            self.model_names_scenario_list.append(model_names)
            self.merged_df.columns = new_column_names
            self.merged_df.to_csv(os.path.join(self.csv_output_folder,(model_names[0].split('_')[3]+'_daily_'+self.station_name+'.csv')))
            self.merged_df_list.append(self.merged_df)
            del self.merged_df

    def monthly_yearly_conversion(self):
        if (self.parameter=="tas" or self.parameter=='tmin' or self.parameter=='tmax' ):
            for i in range(len(self.merged_df_list)):
                self.merged_df_list[i]['time'] = pd.to_datetime\
                    (self.merged_df_list[i]['time'].astype(str),format = '%Y-%m-%d %H:%M:%S',errors='coerce')
                self.merged_df_list[i]['Year']=self.merged_df_list[i]['time'].dt.year
                self.merged_df_list[i]['Month']=self.merged_df_list[i]['time'].dt.month
                ##Yearly Part
                scenario_yearly = self.merged_df_list[i].groupby("Year")[self.model_names_scenario_list[i]].mean()
                path_name = os.path.join(self.csv_output_folder,
                                         (self.model_names_scenario_list[i][0].split('_')[3]+'_yearly_'+self.station_name+'.csv'))
                scenario_yearly.to_csv(path_name)
                ##Monthly Part
                scenario_monthly = self.merged_df_list[i].groupby(["Year",'Month'])[self.model_names_scenario_list[i]].mean()
                path_name = os.path.join(self.csv_output_folder,
                                         (self.model_names_scenario_list[i][0].split('_')[3]+'_monthly_'+self.station_name+'.csv'))
                scenario_monthly.to_csv(path_name,)
        elif (self.parameter=='pr' or self.parameter=='prAdjusted'):
            for i in range(len(self.merged_df_list)):
                self.merged_df_list[i]['time'] = pd.to_datetime(self.merged_df_list[i]['time'].astype(str),
                                                                format = '%Y-%m-%d %H:%M:%S',errors='coerce')
                self.merged_df_list[i]['Year']=self.merged_df_list[i]['time'].dt.year
                self.merged_df_list[i]['Month']=self.merged_df_list[i]['time'].dt.month
                ##Yearly Part
                scenario_yearly = self.merged_df_list[i].groupby("Year")[self.model_names_scenario_list[i]].sum()
                path_name = os.path.join(self.csv_output_folder,
                                         (self.model_names_scenario_list[i][0].split('_')[3] + '_yearly_'+self.station_name + '.csv'))
                scenario_yearly.to_csv(path_name)
                ##Monthly Part
                scenario_monthly = self.merged_df_list[i].groupby(["Year", 'Month'])[
                    self.model_names_scenario_list[i]].sum()
                path_name = os.path.join(self.csv_output_folder,
                                         (self.model_names_scenario_list[i][0].split('_')[3] + '_monthly_'+self.station_name + '.csv'))
                scenario_monthly.to_csv(path_name)

    def extract(self):
        print(f"Process has been started at {self.start_time}")
        self.read_lat_lon_info()
        self.traverse_rcm()
        self.grouping()
        self.extract_data()
        self.end_time = datetime.datetime.now()
        self.print_duration()

    def monthly_conversion(self):
        print(f"Conservation from daily data to monthly data is beginning at {datetime.datetime.now()}. \n"
              f"All the historical, rcp45, rcp85 data will be merged and exported to Excel file!")
        self.start_time = datetime.datetime.now()
        self.read_lat_lon_info()
        for station in self.lat_lon.values.tolist():
            self.station_name=station[0]
            self.traverse_csv()
            self.merge_csv()
            self.monthly_yearly_conversion()
            self.end_time = datetime.datetime.now()
            print(f"For {station[0]} Station:")
            self.print_duration()




if __name__ =='__main__':
    cordex_extraction_object = cordex_extraction()
    cordex_extraction_object.extract()
    cordex_extraction_object.monthly_conversion()

