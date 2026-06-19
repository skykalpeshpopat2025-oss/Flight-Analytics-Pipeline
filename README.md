# Flight Delay Analytics & ML Pipeline
**Environment:** Ubuntu Cluster (`sky@hadoop-master`) | **Hadoop Version:** 3.5.0 (Active on Port 9000)

## 📋 Project Overview
This repository contains the complete implementation for the university multi-phase big data architecture roadmap. 

Due to strict, air-gapped lab environment constraints with no external internet domain resolution (no internet access, no `pip3` or `apt` installs, and no pre-installed Apache Spark/PySpark packages), the entire pipeline was engineered completely from scratch using native Java MapReduce and pure Python 3 standard libraries.

---

## 🛠️ Pipeline Architecture & Components

### 1. Ingestion & MapReduce Processing (`/MapReduce-Java`)
Raw flight datasets were staged into HDFS. Native Java MapReduce drivers were executed across the cluster to aggregate and compute two core analytical matrices:
* **Output 1 (`/user/sky/flight_output1`):** Key-Value Mapping of `Airline Carrier` $\rightarrow$ `Average Delay Minutes`.
* **Output 2 (`/user/sky/flight_output2`):** Key-Value Mapping of `Operating Month ID` $\rightarrow$ `Total Flight Cancellations`.

### 2. Simulated Spark Analytics Engine (`/Pipeline-Scripts/spark_analytics.py`)
Because external dependencies like PySpark, Pandas, or NumPy could not be downloaded, a simulated Spark framework was written natively:
* Uses custom Python `subprocess` pipes to stream MapReduce part-files directly out of HDFS.
* Implements a custom structured `MockDataFrame` class mimicking PySpark declarative syntax.
* Handles explicit type casting via `.withColumn()` and relational sorting via `.orderBy()`.
* **Validated Results:** Identified Spirit (NK - $14.47$ mins) and Frontier (F9 - $12.50$ mins) as the most delayed carriers; exposed a major weather anomaly in February (Month ID 2) with $20,517$ total cancellations.

### 3. Native Machine Learning Pipeline (`/Pipeline-Scripts/flight_ml_prediction.py`)
A supervised predictive pipeline engineered completely from scratch without external ML frameworks:
* Implements a **Binary Logistic Regression Sigmoid Classifier** natively.
* Extracts carrier and seasonal features directly from live HDFS data streams.
* Runs a manual optimization loop via **Gradient Descent** ($2500$ epochs, $lr = 0.1$).
* Generates an operational risk matrix mathematically demonstrating the heightened systemic risk coefficients associated with winter travel variables.

---

## 📂 Repository Directory Structure

* **`MapReduce-Java/`** - Contains the source code for the Java Mappers, Reducers, and Cluster Drivers.
* **`Pipeline-Scripts/`** - Contains the pure Python big data simulation engines and ML models (`spark_analytics.py` and `flight_ml_prediction.py`).

*(Note: In compliance with big data storage standards, the multi-gigabyte raw flight datasets and output part-files remain securely hosted within the local HDFS cluster environment and are not pushed to this repository).*
