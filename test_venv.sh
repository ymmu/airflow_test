#!/bin/bash

app_name="elt_pipeline"
app_home="/opt/airflow/dags/"${app_name}
app_env=${app_home}"/env"

# make a virtual env
if [ ! -e "${app_env}/bin/activate" ]; then
  echo "--- create an ${app_name} env. ---"
  # mkdir ${app_env}
  cd ${app_home} && virtualenv env --python=python3.9
  echo $(pwd)
  source ${app_env}/bin/activate
  # install libs
  pip3 install -r $app_home/requirements.txt
  echo $(pwd)
fi

source $app_env/bin/activate
#sleep 1
#$app_env/bin/python $app_home/main.py --app_home $app_home
