# Large Datasets with Valohai

This repository demonstrates the effective utilization of large datasets with [Valohai][vh], a powerful machine learning platform. The example highlights the use of the `boto3` library to access data stored in an AWS S3 bucket, streamlining data handling.

[vh]: https://valohai.com/
[app]: https://app.valohai.com

### **Steps Covered**:

* **Data Fetching**: Access data from the S3 bucket using machine credentials.
* **Preprocessing**: Perform necessary alterations and preprocessing on the fetched data.
* **Archiving**: Conveniently store processed files in .tar format (or other suitable formats) for efficient storage and data transfer. 
* **Dataset Versioning**: Manage dataset versions to incorporate new data into the production dataset. Ensures data changes are tracked and maintain reproducibility. 
* **Dataset Unzipping**: Prepare data for model training by unzipping the dataset.

This streamlined workflow empowers you to focus on your machine learning tasks, while Valohai handles data management, versioning, and efficient storage.

**_Note: In our case, the machines running Valohai jobs have a Service Account with permission to the S3 bucket. This eliminates the need for key rotation or extra authentication steps in the code. Boto3 will automatically use the IAM Role attached to our EC2 instance for authentication._**

## <div align="center">Installation</div>

Login to the [Valohai app][app] and create a new project.

### Configure the repository:

To run the code on Valohai using the terminal, follow these steps:

1. Install Valohai on your machine by running the following command:

```bash
pip install valohai-cli valohai-utils
```

2. Log in to Valohai from the terminal using the command:

```bash
vh login
```

3. Create a project for your Valohai workflow.
   Start by creating a directory for your project:

```bash
mkdir valohai-large-data-management
cd valohai-large-data-management
```

Then, create the Valohai project:

```bash
vh project create
```

4. Clone the repository to your local machine:

```bash
git clone https://github.com/valohai/large-data-management-example.git .
```

**Note: You may need to update the environment section in the valohai.yaml file to point to your specific environment, 
or you can remove the line(s) to use the project default.**

### **Running Executions:**
To run individual steps, execute the following command:

```bash
vh execution run <step-name> --adhoc
```

For example, to run the _generate-dataset_ step, use the command:

```bash
vh execution run generate-dataset --adhoc
```

### Running Pipelines:

To run pipelines, use the following command:
```bash
vh pipeline run <pipeline-name> --adhoc
```

For example, to run the dataset-generation-pipeline pipeline, use the command:
```bash
vh pipeline run dataset-generation-pipeline --adhoc
```


## <div align="center">Details explained</div>

<details open>
<summary> <strong> Data Fetching </strong> </summary>

1. Initialize `S3DataDownloader`: Set up the class with the bucket name, folder prefix, and local save path.

2. Utilize Paginator for Large Datasets: The paginator in `_get_all_files` method efficiently loops through paginated responses, ensuring all S3 objects are fetched, regardless of dataset size.

3. Fetch All Files in the Bucket: Accumulate all objects from the specified S3 bucket and prefix, handling potential data volumes exceeding 10K files.

4. Download and Save Files: Use `s3_client.download_file` to download and store each selected file locally.

</details>

<details open>
<summary> <strong> Data Preprocessing and Dataset Management </strong> </summary>

1. Data Preprocessing: In the `preprocess_data` method, we make some random changes to the binary files, which you can customize for your specific needs.

2. Archiving the Dataset: When creating the production dataset, the `tar_directory` method stores the preprocessed files into a single .tar file. This file is then saved in Valohai outputs and to Valohai datasets for easy dataset access. 

3. Dataset Alias: In the `_save_metadata` method, we create an alias. This alias allows us to easily refer to the latest version of the dataset in other parts of the project. This metadata includes information about the dataset's version, URI, and associated aliases.

4. Updating a single dataset archive or maintaining them in distinct files.
   * `save_updated_dataset_by_appending_tar` Continuously adds new preprocessed data to an existing .tar archive, keeping one evolving dataset.

   * `save_updated_dataset_as_two_tars` Keeps the dataset in separate .tar archives; the initial dataset and additional data are saved in separate tars.
</details>

<details open>
<summary> <strong> Unzipping the Dataset </strong> </summary>

Unzipping to Target Directory: the `untar` function unzips the dataset to the specified directory. Ideal when you want full control over the location where the dataset is extracted.

**_Note:_** `process_archives=False`. By providing this parameter to `valohai.inputs`, you instruct Valohai not to automatically unzip the dataset. 
</details>

## <div align="center">Selecting the Right Archive Format</div>

Choosing the right archive format is crucial when handling large datasets. Let's explore the best uses for three common formats: tar, tar.gz, and zip.

### Tar (.tar)

Use Tar When:

* Bundling files or directories without compression.
* Preserving file attributes, ownership, and timestamps.
* Cross-platform compatibility is important.

Considerations:

Larger file sizes compared to compressed formats.
Ideal for maintaining file attributes.

### Tar.gz (.tar.gz)
Use Tar.gz When:

* Compressing files to save space while retaining metadata.
* Efficient storage and transfer of datasets are key.
* Working with large text-based files.

Considerations:

Smaller file sizes due to compression.
Compression takes more CPU time.

### Zip (.zip)
Use Zip When:

* Compatibility with Windows systems is necessary.
* Balanced compression and cross-platform exchange are desired.
* Password protection or encryption is needed.

Considerations:

Larger file sizes compared to tar.gz.
Offers various compression levels.
