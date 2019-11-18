# General Upload Script

## Usage

1. Make sure your data is in a csv format where each metric has its own column and there is a `timestamp` column that is the epoch time in **milliseconds**. Use `;` as separator.

    ```csv
    timestamp;temperature;humidity
    1573636000;21;0.5
    1573637000;23;0.6
    ...;...;...
    ```

2. Create a config, you can start from the `config_example.txt`.

3. Make sure [python3](https://www.python.org/downloads/) is installed, and install the additional requirements.

    ```
    pip install -r requirements.txt
    ```

4. Run the script:

   ```shell script
    python ./upload_script.py ./example_data.csv resource_name ./example_config.txt
   ```   
