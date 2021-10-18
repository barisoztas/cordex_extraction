import os
import datetime
import glob
from itertools import groupby
import pandas as pd
import xarray as xr



class cordex_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.lat_lon_csv = r"/home/hsaf/Ortak/CORDEX/all/tas/lat_lon.csv"
        self.lat_lon = None
        self.parameter = None
        self.cordex_data_folder = r"/home/hsaf/Ortak/CORDEX/all/tas/"
        self.csv_output_folder = r"/home/hsaf/Ortak/CORDEX/all/tas/output"
        self.files_names = []
        self.models=[]


    def print_duration(self):
        return print("Total Process time:  {}".format(str(self.end_time - self.start_time)))

    def read_lat_lon_info(self):
        self.lat_lon = pd.read_csv(self.lat_lon_csv)
        return self.lat_lon


    def traverse_rcm(self,parameter):
        self.parameter = parameter
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
        station = self.lat_lon.values.tolist()
        count = 0
        for keys in self.model_dictionary.keys():
            count = count+1
            d ={'time':[],self.parameter:[]}
            model_data = pd.DataFrame(d)
            for i in range(len(self.model_dictionary[keys])):
                data = xr.open_dataset(self.model_dictionary[keys][i])
                if (data._coord_names.__contains__('rlat')):
                    st = data["tas"].sel(rlat=station[0][3],rlon=station[0][4],method='nearest').hvplot().data[['time',self.parameter]]
                else:
                    break
                model_data = model_data.append(st)
                data.close()
            model_data.to_csv(os.path.join(self.csv_output_folder,keys+'.csv'),index=False)
            percent = (count / len(self.model_dictionary(keys)))*100
            print(f"%{(count / len(self.model_dictionary(keys))) * 100} is done!")
            print(f"{len(self.model_dictionary(keys))-count} left!")




    def run(self):
        print("Process has been started at {} ".format(self.start_time))
        self.read_lat_lon_info()
        self.traverse_rcm('tas')
        self.grouping()
        self.extract_data()
        self.end_time = datetime.datetime.now()
        self.print_duration()



if __name__ =='__main__':
    cordex_extraction_object = cordex_extraction()
    cordex_extraction_object.run()

