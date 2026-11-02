# E-shop : Pipeline ETL & Data Lakehouse (Bronze / Silver / Gold)

Projet de Data Engineering avec **Apache Spark**, **PySpark** et **JupyterLab** en **mode local (`local[*]`)**.

---

## 🚀 Démarrage rapide avec Docker Compose

### 1. Lancer l'environnement
À la racine du projet `E-shop/`, lancez le conteneur :
```bash
docker compose up -d
```

### 2. Accéder aux services
- **JupyterLab** : [http://localhost:8888](http://localhost:8888)  
  *(Token de connexion : `eshop`)*
- **Spark UI (actif pendant l'exécution d'un job)** : [http://localhost:4040](http://localhost:4040)

### 3. Exécuter le Notebook
1. Ouvrez JupyterLab sur [http://localhost:8888](http://localhost:8888)
2. Naviguez dans le dossier `notebooks/`
3. Ouvrez le notebook [`TP_E-shop.ipynb`](notebooks/TP_E-shop.ipynb)

---

## ⚙️ Configuration Spark dans le Notebook (Mode Local)

Dans votre notebook, la session Spark est initialisée en mode `local[*]` (utilise tous les cœurs CPU disponibles sur votre machine) :

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("E-shop Data Lakehouse") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .getOrCreate()

print("Spark Version :", spark.version)
print("Master utilisé :", spark.sparkContext.master)
print("Application ID :", spark.sparkContext.applicationId)
```

---

## 📁 Architecture du projet

```text
E-shop/
├── data/
│   └── source/                  # Données brutes originales (avec anomalies)
│       ├── customers.csv
│       ├── products.csv
│       ├── orders.csv
│       ├── order_items.csv
│       └── promotions.jsonl
├── data_lakehouse/
│   ├── bronze/                  # Zone Bronze : fichiers bruts persistés + manifestes
│   ├── silver/                  # Zone Silver : données nettoyées, typées, dédupliquées
│   ├── gold/                    # Zone Gold : tables analytiques et métriques
│   ├── quarantine/              # Données rejetées et anomalies tracées
│   └── metadata/                # Logs, schémas, manifestes d'ingestion et d'audit
├── notebooks/
│   └── TP_E-shop.ipynb          # Notebook de réalisation du TP
├── scripts/                     # Scripts utilitaires et ETL PySpark
├── docker-compose.yml           # Déploiement PySpark + Jupyter
└── README.md
```

---

## 🛠️ Arrêter le conteneur

```bash
docker compose down
```
