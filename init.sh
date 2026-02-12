#!/bin/bash

echo "Init folders for CryptoBot Project"

cd ~
 
ROOTDIR=$(pwd)
PROJECT_FOLDER="$ROOTDIR/DATAS/datascientest/projet"

# Create mongo data folder
MONGO_FOLDER="$PROJECT_FOLDER/mongo_data"
if [ -d $MONGO_FOLDER ]; then
    echo "Le dossier $MONGO_FOLDER existe!"
else
    mkdir -p $MONGO_FOLDER
    echo "Dossier $MONGO_FOLDER créé!"
fi

# Create postgresql data folder
PG_FOLDER="$PROJECT_FOLDER/postgresql_data"
if [ -d $PG_FOLDER ]; then
    echo "Le dossier $PG_FOLDER existe!"
else
    mkdir -p $PG_FOLDER
    sudo chown -R 999:999 $PG_FOLDER
    echo "Dossier $PG_FOLDER créé!"
fi

# Create pgadmin data folder
PGADMIN_FOLDER="$PROJECT_FOLDER/pgadmin_data"
if [ -d $PGADMIN_FOLDER ]; then
    echo "Le dossier $PGADMIN_FOLDER existe!"
else
    mkdir -p $PGADMIN_FOLDER
    sudo chown -R 5050:5050 $PGADMIN_FOLDER
    echo "Dossier $PGADMIN_FOLDER créé!"    
fi