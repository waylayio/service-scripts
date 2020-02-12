# Upload Script for timeseries csv

## Usage

1.The csv should look like this:
  
    ```csv
    resource,metric,timestamp,value
    resource_name,waylay.resourcemessage.metric.temperature,2019-10-21T05:25:16Z,21
    resource_name,waylay.resourcemessage.metric.temperature,2019-10-21T05:26:16Z,23
    ...,...,...,...
    ```
The csv can contain different resources and different metrics. 

2. Create a config, you can start from the `config_example.txt`.

3. Make sure [python3](https://www.python.org/downloads/) with [pandas](https://pandas.pydata.org/) is installed.

4. Run the script:

   ```shell script
    python ./upload_resources.py ./example_data.csv ./example_config.txt
   ```   
