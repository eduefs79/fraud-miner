
# 🧠 Credit Card Fraud Detection: Logistic Regression Pipeline (Databricks)

[![Databricks](https://img.shields.io/badge/platform-Databricks-red)](https://databricks.com/)
[![AWS S3](https://img.shields.io/badge/storage-S3-blue)](https://aws.amazon.com/s3/)
[![sklearn](https://img.shields.io/badge/modeling-scikit--learn-yellowgreen)](https://scikit-learn.org/)
[![status](https://img.shields.io/badge/status-active-success)]()

---

## 📊 Objective

Design and implement a production-ready ML pipeline for credit card fraud detection using the Medallion Architecture, Airflow, AWS, and Databricks.  
**Note:** The goal is not model accuracy (as the dataset is synthetic), but to demonstrate data engineering, modeling orchestration, and end-to-end deployment capability.

---

## 🔁 Architecture Overview (Mermaid)

```mermaid
graph TD
    subgraph Bronze Layer
      A1[Raw CSV files] --> A2[Airflow DAG: generate_fake_data.py]
      A2 --> A3[S3: bronze/fraud_raw/...]
    end

    subgraph Silver Layer
      A3 --> B1[Airflow DAGs: customer_hub_link.py, merchant_hub_link.py, etc.]
      B1 --> B2[Delta tables in fraud_miner.silver.*]
      B2 --> B3[GeoIP enrichment (GeoIP.py)]
    end

    subgraph Business Data Vault / Feature Layer
      B3 --> C1[fraud_geo_view (JOINed View)]
    end

    subgraph ML Modeling
      C1 --> D1[Feature Engineering]
      D1 --> D2[Train/Test Split]
      D2 --> D3[Logistic Regression + Cross Validation]
      D3 --> D4[Save predictions & scores to fraud_miner.gold.*]
      D3 --> D5[Save model.pkl to S3]
    end

    subgraph Job Deployment
      D5 --> E1[Databricks Job]
      E1 --> E2[Airflow DAG: deploy_fraud_model.py]
    end
```

---

## ✅ Features

- ✅ Full Medallion Architecture: Bronze → Silver → Gold
- ✅ Airflow-managed ingestion and enrichment
- ✅ Databricks + Unity Catalog for Delta Table lifecycle
- ✅ 10-fold cross-validation using scikit-learn
- ✅ Final model and predictions stored on Delta + S3
- ✅ End-to-end orchestration via Airflow and Databricks Jobs
- ✅ Secrets securely managed via AWS Secrets Manager

---

## 🧱 Technologies Used

| Component     | Stack                                |
|---------------|---------------------------------------|
| Language      | Python 3.x                            |
| Platform      | Databricks (Notebooks, Jobs) + Spark  |
| ML Library    | scikit-learn                          |
| Orchestration | Apache Airflow                        |
| Storage       | AWS S3                                |
| Format        | Delta Tables, Parquet, Pickle         |
| Secrets Mgmt  | AWS Secrets Manager                   |

---

## 🚀 Workflow Summary

1. Generate fake customer/card/merchant/transaction data via Airflow
2. Ingest and model Bronze → Silver → Gold Delta layers
3. Enrich transactions with GeoIP data
4. Engineer ML features (`geo_matches_merchant`, `credit_limit`, etc.)
5. Perform cross-validation with logistic regression
6. Save predictions and evaluation metrics to Delta tables
7. Upload final model artifact to S3
8. Deploy via Databricks Jobs (triggered by Airflow)

---

## 📦 Sample Outputs

### 🧪 Evaluation Scores

| run_date           | fold | accuracy |
|--------------------|------|----------|
| 2025-05-07 01:00:00 | 1    | 0.9812   |
| ...                | ...  | ...      |

### 🔍 Model Predictions

| run_date           | true_label | predicted_label |
|--------------------|------------|-----------------|
| 2025-05-07 01:00:00 | 0          | 0               |
| ...                | ...        | ...             |

---

## 💾 Model Artifact

- Path: `s3://fraud-miner/model/logreg_model.pkl`
- Format: Python pickle (`joblib`)
- Usage: Load via `joblib.load()` for scoring or inspection

---

## 💬 Model Accuracy Disclaimer

This project uses synthetic data; therefore, model performance (precision, recall, F1) is not representative. The primary goal is **demonstrating architecture, deployment, and automation capabilities**, not modeling performance.  
In a real-world use case, additional steps like SMOTE, class weighting, and real fraud data tuning would be applied.

---

## 🔮 Next Steps

- 🤖 Compare with RandomForest, XGBoost
- 🧠 Add SHAP or LIME explainability
- 🚀 Register model with MLflow
- 🔁 Automate DAG triggering and versioning
- 📊 Visualize predictions in Power BI or Tableau
- 🛡 Improve schema validation and retry logic

---

## 👨‍💻 Author

**Eduardo Francisco da Silva**  
Lead Data Engineer | Fraud Detection | Data Science Practitioner  
🇧🇷 + 🇺🇸 | [LinkedIn](https://www.linkedin.com/in/eduefs)

![Secret Scan](https://github.com/eduefs79/fraud-miner/actions/workflows/trufflehog-docker-v4.yml/badge.svg)
