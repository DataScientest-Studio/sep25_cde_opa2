Crypto Bot Projet
DATA Engineer training
==============================
@TODO: Develop here the goal of this project and how to install it.
==============================

Project Organization
--------------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── .env.sample        <- To rename .env, contains the environment variables for this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docker             <- Docker configuration
    │   ├── pgadmin        <- Contains the servers.json file to configure ther servers
    │   │   └── .servers.sample    <- To rename servers.json, remplace 'xxxxxxx' with the right values, same as in .env
    │   │
    │   ├── python         <- Contains the DockerFile to prepare the python container
    │   └── streamlit      <- Contains the DockerFile to prepare the streamlit container
    │
    ├── logs               <- Logs from training and predicting
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── compose.yaml       <- Docker compose file to run the different containers of the project
    │
    ├── Makefile           <- Makefile to automate the different steps of the project
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   ├── make_dataset.py
    │   │   └── binance
    │   │       ├── BinanceDataCollector.py (class to collect data from Binance API and save it in MongoDB)
    │   │       ├── extract_exchange_info.py
    │   │       ├── extract_klines_data.py
    │   │       ├── extract_realtime_data.py
    │   │       └── extract_kline_data_ws.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── init           <- Scripts to init the data bases.
    │   │   ├── init_mongo.py
    │   │   └── init_postgresql.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   ├── visualization  <- Scripts to create exploratory and results oriented visualizations
    │   │   └── visualize.py
    │   │   └── streamlit
    │   │       └── klines_viewer.py
    │   ├── config.py           <- Expose the environement variables, to be imported by other scripts.
    │   ├── custom_logger.py    <- Create a custom logger  
    │   ├── config              <- Describe the parameters used in train_model.py and predict_model.py
    │   └── scripts.sample      <- Python scripts call examples

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
