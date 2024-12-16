#!/bin/bash

VENV_NAME="/home/dev/dbt/.env"

# Kiểm tra và tạo Python virtual environment nếu chưa tồn tại
if [ ! -d "$VENV_NAME" ]; then
    echo "Creating Python virtual environment: $VENV_NAME"
    # python -m venv $VENV_NAME
fi

# Activate virtual environment
source $VENV_NAME/bin/activate

export SPARK_HOME=/data/spark-3.5.1-bin-hadoop3
# export JAVA_HOME=/home/dev/duy/jdk-11.0.20
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.13.0.8-4.el8_5.x86_64
export SPARK_CONF_DIR=/home/dev/dbt/dbt-project-demo/spark_conf_dir

export AWS_ACCESS_KEY_ID=Vl21ZDUEq1XNiDDN
export AWS_SECRET_ACCESS_KEY=Su9omOppqPvl9q59ABdIfzwOzDi1PpgZ
export AWS_REGION=auto

check_environment() {
        echo "Checking environment variables..."
    declare -a required_vars=(
        "SPARK_HOME"
        "JAVA_HOME"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
        "AWS_REGION"
    )

    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            echo "WARNING: $var is not set"
        else
            echo "$var is set"
        fi
    done
}
check_environment

echo "Environment setup completed!"
echo "Virtual environment is activated and variables are exported."
echo "Use 'deactivate' to exit the virtual environment when done."