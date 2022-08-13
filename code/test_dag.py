from DAG_functions import extractData, transformData, loadData, deleteFiles
import config

dir_name = config.dir_name

device_df, sensor_df, alarm_df = extractData(dir_name)

device_df, sensor_df, alarm_df = transformData(device_df, sensor_df, alarm_df)

loadData(device_df, sensor_df, alarm_df)

deleteFiles(dir_name)
