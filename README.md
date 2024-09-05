# aggregates-for-Github-Events


Aggregates-for-Github-Events contains all the processes to generate daily user and repository tables with data from GH Archive.

## Development environment

### Setting up a virtual environment for aggregates-for-Github-Events

First create a virtual environment with conda or virtualenv and activate it. 

* **Conda (Mac OSX)**

```
conda create -n aggregates-for-gh-data python=3.10
conda activate aggregates-for-gh-data
```

* **Virtualenv (Linux and Windows)**

```
virtualenv aggregates-for-gh-data python=3.10
# Linux
source aggregates-for-gh-data/bin/activate
# Windows
aggregates-for-gh-data\Scripts\activate
```

### Install requirements

pip install -r requirements.txt

### Run task locally

Modify the script `config.py`to change the location where you want to store the data.

```
python generate_table_agg.py GenerateJob --local-scheduler
```

### Testing

Run this from the project root.
```
pytest tests/
```