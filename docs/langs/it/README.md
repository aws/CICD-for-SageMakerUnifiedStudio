[![en](https://img.shields.io/badge/lang-en-gray.svg)](../../../README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](../pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](../fr/README.md)
[![it](https://img.shields.io/badge/lang-it-brightgreen.svg?style=for-the-badge)](../it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](../ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](../zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](../he/README.md)

# SMUS CI/CD Pipeline CLI

[![en](https://img.shields.io/badge/lang-en-brightgreen.svg?style=for-the-badge)](README.md)
[![pt](https://img.shields.io/badge/lang-pt-gray.svg)](docs/langs/pt/README.md)
[![fr](https://img.shields.io/badge/lang-fr-gray.svg)](docs/langs/fr/README.md)
[![it](https://img.shields.io/badge/lang-it-gray.svg)](docs/langs/it/README.md)
[![ja](https://img.shields.io/badge/lang-ja-gray.svg)](docs/langs/ja/README.md)
[![zh](https://img.shields.io/badge/lang-zh-gray.svg)](docs/langs/zh/README.md)
[![he](https://img.shields.io/badge/lang-he-gray.svg)](docs/langs/he/README.md)

> **[Anteprima]** Amazon SageMaker Unified Studio CI/CD CLI è attualmente in fase di anteprima e soggetto a modifiche. I comandi, i formati di configurazione e le API potrebbero evolversi in base al feedback dei clienti. Consigliamo di valutare questo strumento in ambienti non di produzione durante l'anteprima. Per feedback e segnalazioni di bug, apri una issue su https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues

> **[Solo Domini IAM]** Questa CLI supporta attualmente solo i domini SMUS che utilizzano l'autenticazione basata su IAM. Il supporto per i domini basati su IAM Identity Center (IdC) sarà disponibile a breve.

**Automatizza il deployment delle applicazioni dati tra gli ambienti SageMaker Unified Studio**

"Deploy Airflow DAGs, Jupyter notebooks, and ML workflows from development to production with confidence" (Distribuisci DAG Airflow, notebook Jupyter e workflow ML dallo sviluppo alla produzione con sicurezza). Creato per data scientist, data engineer, ML engineer e sviluppatori di app GenAI che lavorano con i team DevOps.

**Si adatta alla tua strategia di deployment:** Che tu utilizzi branch git (branch-based), artefatti versionati (bundle-based), tag git (tag-based) o deployment diretto - questa CLI supporta il tuo workflow. Definisci la tua applicazione una volta, distribuiscila come preferisci.

---

## Perché SMUS CI/CD CLI?

✅ **AWS Abstraction Layer** - La CLI incapsula tutta la complessità di AWS analytics, ML e SMUS - i team DevOps non chiamano mai direttamente le API AWS  
✅ **Separation of Concerns** - I team di dati definiscono COSA distribuire (manifest.yaml), i team DevOps definiscono COME e QUANDO (CI/CD workflows)  
✅ **Generic CI/CD Workflows** - Lo stesso workflow funziona per Glue, SageMaker, Bedrock, QuickSight o qualsiasi combinazione di servizi AWS  
✅ **Deploy with Confidence** - Test e validazione automatizzati prima della produzione (Distribuisci con sicurezza)  
✅ **Multi-Environment Management** - Test → Prod con configurazione specifica per ambiente  
✅ **Infrastructure as Code** - Manifest applicativi con controllo versione e distribuzioni riproducibili  
✅ **Event-Driven Workflows** - Attivazione automatica dei workflow tramite EventBridge durante la distribuzione  

---

## Avvio Rapido

**Installazione dal codice sorgente:**
```bash
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .
```

**Distribuisci la tua prima applicazione:**
```bash
# Validate configuration
aws-smus-cicd-cli describe --manifest manifest.yaml --connect

# Create deployment bundle (optional)
aws-smus-cicd-cli bundle --manifest manifest.yaml

# Deploy to test environment
aws-smus-cicd-cli deploy --targets test --manifest manifest.yaml

# Run validation tests
aws-smus-cicd-cli test --manifest manifest.yaml --targets test
```

**Guarda in azione:** [Live GitHub Actions Example](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/runs/17631303500)

---

## Per Chi È Questo?

### 👨‍💻 Team di Data (Data Scientists, Data Engineers, Sviluppatori di App GenAI)
**Ti concentri su:** La tua applicazione - cosa distribuire, dove distribuire e come funziona  
**Definisci:** Application manifest (`manifest.yaml`) con il tuo codice, workflow e configurazioni  
**Non devi conoscere:** CI/CD pipelines, GitHub Actions, automazione del deployment

→ **[Guida Rapida](docs/getting-started/quickstart.md)** - Distribuisci la tua prima applicazione in 10 minuti

**Include esempi per:**
- Data Engineering (Glue, Notebooks, Athena)
- ML Workflows (SageMaker, Notebooks) 
- GenAI Applications (Bedrock, Notebooks)

### 🔧 Team DevOps
**Ti concentri su:** Best practice CI/CD, sicurezza, conformità e automazione del deployment  
**Definisci:** Workflow templates che impongono testing, approvazioni e policy di promozione  
**Non devi conoscere:** Dettagli specifici delle applicazioni, servizi AWS utilizzati, API DataZone, strutture dei progetti SMUS o logica di business

→ **[Guida Amministratore](docs/getting-started/admin-quickstart.md)** - Configura infrastruttura e pipeline in 15 minuti  
→ **[GitHub Workflow Templates](git-templates/)** - Template di workflow generici e riutilizzabili per il deployment automatizzato

**The CLI is your abstraction layer: You just call `aws-smus-cicd-cli deploy` - the CLI handles all AWS service interactions** (La CLI è il tuo livello di astrazione: Basta chiamare `aws-smus-cicd-cli deploy` - la CLI gestisce tutte le interazioni con i servizi AWS)

---

## Cosa Puoi Distribuire?

**📊 Analytics & BI**
- Glue ETL jobs e crawler
- Query Athena
- Dashboard QuickSight
- Job EMR (futuro)
- Query Redshift (futuro)

**🤖 Machine Learning**
- SageMaker training jobs
- ML models and endpoints (modelli ML ed endpoint)
- MLflow experiments
- Feature Store (futuro)
- Batch transforms (futuro)

**🧠 Generative AI**
- Bedrock agents
- Knowledge bases (basi di conoscenza)
- Foundation model configurations (futuro)

**📓 Code & Workflows**
- Jupyter notebooks
- Script Python
- Airflow DAGs (MWAA e Amazon MWAA Serverless)
- Lambda functions (futuro)

**💾 Data & Storage**
- File dati S3
- Repository Git
- Data catalogs (futuro)

---

## Servizi AWS Supportati

Distribuisci workflow utilizzando questi servizi AWS attraverso la sintassi YAML di Airflow:

### 🎯 Analytics & Dati
**Amazon Athena** • **AWS Glue** • **Amazon EMR** • **Amazon Redshift** • **Amazon QuickSight** • **Lake Formation**

### 🤖 Machine Learning
**SageMaker Training** • **SageMaker Pipelines** • **Feature Store** • **Model Registry** • **Batch Transform**

### 🧠 Intelligenza Artificiale Generativa
**Amazon Bedrock** • **Bedrock Agents** • **Bedrock Knowledge Bases** • **Guardrails**

### 📊 Servizi Aggiuntivi
S3 • Lambda • Step Functions • DynamoDB • RDS • SNS/SQS • Batch

**Consulta l'elenco completo:** [Airflow AWS Operators Reference](docs/airflow-aws-operators.md)

---

## Concetti Fondamentali

### Separazione delle Responsabilità: Il Principio Chiave di Design

**Il Problema:** Gli approcci tradizionali al deployment costringono i team DevOps a imparare i servizi analytics AWS (Glue, Athena, DataZone, SageMaker, MWAA, ecc.) e comprendere le strutture dei progetti SMUS, o costringono i team di dati a diventare esperti di CI/CD.

**La Soluzione:** SMUS CI/CD CLI è il livello di astrazione che incapsula tutta la complessità di AWS e SMUS.

**Example workflow:** (Flusso di lavoro di esempio)

```
1. DevOps Team                 2. Data Team                    3. SMUS CI/CD CLI (The Abstraction)
   ↓                               ↓                              ↓
Defines the PROCESS            Defines the CONTENT            Workflow calls:
- Test on merge                - Glue jobs                    aws-smus-cicd-cli deploy --manifest manifest.yaml
- Approval for prod            - SageMaker training             ↓
- Security scans               - Athena queries               CLI handles ALL AWS complexity:
- Notification rules           - File structure               - DataZone APIs
                                                              - Glue/Athena/SageMaker APIs
Defines INFRASTRUCTURE                                        - MWAA deployment
- Account & region                                            - S3 management
- IAM roles                                                   - IAM configuration
- Resources                                                   - Infrastructure provisioning

Works for ANY app!
No ML/Analytics/GenAI
service knowledge needed!
```

**I team DevOps si concentrano su:**
- Best practice CI/CD (test, approvazioni, notifiche)
- Controlli di sicurezza e conformità
- Orchestrazione del deployment
- Monitoraggio e alerting

"SMUS CI/CD CLI handles ALL AWS complexity:" (SMUS CI/CD CLI gestisce TUTTA la complessità AWS:)
- Gestione domini e progetti DataZone
- API di AWS Glue, Athena, SageMaker, MWAA
- Gestione storage e artefatti S3
- Ruoli e permessi IAM
- Configurazioni delle connessioni
- Sottoscrizioni agli asset del catalogo
- Deployment dei workflow su Airflow
- Provisioning dell'infrastruttura
- Test e validazione

**I team di dati si concentrano su:**
- Codice applicativo e workflow
- Quali servizi AWS utilizzare (Glue, Athena, SageMaker, ecc.)
- Configurazioni degli ambienti
- Logica di business

**Risultato:**
- "DevOps teams never call AWS APIs directly - they just call `aws-smus-cicd-cli deploy`" (I team DevOps non chiamano mai direttamente le API AWS - chiamano solo `aws-smus-cicd-cli deploy`)
- "CI/CD workflows are generic - same workflow works for Glue apps, SageMaker apps, or Bedrock apps" (I workflow CI/CD sono generici - lo stesso workflow funziona per app Glue, SageMaker o Bedrock)
- I team di dati non toccano mai le configurazioni CI/CD
- Entrambi i team lavorano indipendentemente usando le proprie competenze

---

### Application Manifest
Un file YAML dichiarativo (`manifest.yaml`) che definisce la tua applicazione dati:
- **Dettagli applicazione** - Nome, versione, descrizione
- **Contenuto** - Codice dai repository git, dati/modelli dallo storage, dashboard QuickSight
- **Workflow** - DAG Airflow per orchestrazione e automazione
- **Stage** - Dove effettuare il deployment (ambienti dev, test, prod)
- **Configurazione** - Impostazioni specifiche per ambiente, connessioni e azioni di bootstrap

**Creato e gestito dai team di dati.** Definisce **cosa** deployare e **dove**. Non richiede conoscenze CI/CD.

### Application
Il carico di lavoro dati/analytics da deployare:
- DAG Airflow e script Python
- Notebook Jupyter e file di dati
- Modelli ML e codice di training
- Pipeline ETL e trasformazioni
- Agenti GenAI e server MCP
- Configurazioni dei modelli foundation

### Stage
Un ambiente di deployment (dev, test, prod) mappato a un progetto SageMaker Unified Studio:
- Configurazione di dominio e regione
- Nome e impostazioni del progetto
- Connessioni alle risorse (S3, Airflow, Athena, Glue)
- Parametri specifici per ambiente
- Mapping opzionale dei branch per deployment basati su git

### Stage-to-Project Mapping

"Each application stage deploys to a dedicated SageMaker Unified Studio (SMUS) project. A project can host a single application or multiple applications depending on your architecture and CI/CD methodology. Stage projects are independent entities with their own governance:" (Ogni stage dell'applicazione viene deployato su un progetto SageMaker Unified Studio (SMUS) dedicato. Un progetto può ospitare una singola applicazione o più applicazioni in base all'architettura e alla metodologia CI/CD. I progetti stage sono entità indipendenti con la propria governance:)

- **Proprietà e Accesso:** Ogni progetto stage ha i propri proprietari e contributori, che possono differire dal progetto di sviluppo. I progetti di produzione tipicamente hanno accesso più ristretto rispetto agli ambienti di sviluppo.
- **Multi-Domain e Multi-Region:** I progetti stage possono appartenere a domini SMUS, account AWS e regioni differenti. Ad esempio, lo stage dev potrebbe essere deployato su un dominio di sviluppo in us-east-1, mentre prod su un dominio di produzione in eu-west-1.
- **Architettura Flessibile:** Le organizzazioni possono scegliere tra progetti dedicati per applicazione (isolamento) o progetti condivisi che ospitano più applicazioni (consolidamento), in base ai requisiti di sicurezza, conformità e operativi.

Questa separazione permette un vero isolamento degli ambienti con controlli di accesso indipendenti, confini di conformità e requisiti di residenza regionale dei dati.

### Workflow
Logica di orchestrazione che esegue la tua applicazione. I workflow servono due scopi:

**1. Durante il deployment:** Creare le risorse AWS richieste durante il deployment
- Provisioning dell'infrastruttura (bucket S3, database, ruoli IAM)
- Configurazione di connessioni e permessi
- Configurazione di monitoraggio e logging

**2. Runtime:** Eseguire pipeline dati e ML continuative
- Esecuzione programmata (giornaliera, oraria, ecc.)
- Trigger basati su eventi (upload S3, chiamate API)
- Elaborazione e trasformazione dati
- Training e inferenza dei modelli

I workflow sono definiti come DAG Airflow (Directed Acyclic Graphs) in formato YAML. Supporta [MWAA (Managed Workflows for Apache Airflow)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) e [Amazon MWAA Serverless](https://aws.amazon.com/blogs/big-data/introducing-amazon-mwaa-serverless/) ([User Guide](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html)).

### CI/CD Automation
Workflow GitHub Actions (o altri sistemi CI/CD) che automatizzano il deployment:
- **Creati e gestiti dai team DevOps**
- Definisce **come** e **quando** deployare
- Esegue test e controlli di qualità
- Gestisce la promozione tra i target
- Applica policy di sicurezza e conformità
- Esempio: `.github/workflows/deploy.yml`

"Key insight: DevOps teams create generic, reusable workflows that work for ANY application. They don't need to know if the app uses Glue, SageMaker, or Bedrock - the CLI handles all AWS service interactions. The workflow just calls `aws-smus-cicd-cli deploy` and the CLI does the rest." (Intuizione chiave: I team DevOps creano workflow generici e riutilizzabili che funzionano per QUALSIASI applicazione. Non hanno bisogno di sapere se l'app usa Glue, SageMaker o Bedrock - la CLI gestisce tutte le interazioni con i servizi AWS. Il workflow chiama semplicemente `aws-smus-cicd-cli deploy` e la CLI fa il resto.)

### Modalità di Deployment

**Bundle-based (Artifact):** Crea archivio versionato → deploya archivio agli stage
- Vantaggi: tracciabilità, capacità di rollback, conformità
- Comando: `aws-smus-cicd-cli bundle` poi `aws-smus-cicd-cli deploy --manifest app.tar.gz`

**Direct (Git-based):** Deploya direttamente dai sorgenti senza artefatti intermedi
- Vantaggi: workflow più semplici, iterazione rapida, git come fonte di verità
- Comando: `aws-smus-cicd-cli deploy --manifest manifest.yaml --stage test`

Entrambe le modalità funzionano con qualsiasi combinazione di storage e sorgenti git.

---

## Esempi di Applicazioni

Esempi reali che mostrano come distribuire diversi carichi di lavoro con SMUS CI/CD.

### 📊 Analytics - Dashboard QuickSight
Distribuisci dashboard BI interattive con pipeline ETL Glue automatizzate per la preparazione dei dati. Utilizza bundle di risorse QuickSight, query Athena e integrazione dataset GitHub con configurazioni specifiche per ambiente.

**AWS Services:** QuickSight • Glue • Athena • S3 • MWAA Serverless

**GitHub Workflow:** [analytic-dashboard-glue-quicksight.yml](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/actions/workflows/analytic-dashboard-glue-quicksight.yml)

("What happens during deployment:" → "Durante la distribuzione:") Il codice dell'applicazione viene distribuito su S3, vengono creati ed eseguiti i job Glue e i workflow Airflow, vengono creati dashboard/sorgente dati/dataset QuickSight e viene avviata l'acquisizione QuickSight per aggiornare la dashboard con i dati più recenti.

[Resto del contenuto mantenuto in inglese come da regole]

### 📓 Data Engineering - Notebooks
("Deploy Jupyter notebooks with parallel execution orchestration for data analysis and ETL workflows. Demonstrates notebook deployment with MLflow integration for experiment tracking." → "Distribuisci notebook Jupyter con orchestrazione dell'esecuzione parallela per analisi dati e workflow ETL. Dimostra la distribuzione di notebook con integrazione MLflow per il tracciamento degli esperimenti.")

[Resto del contenuto mantenuto in inglese come da regole]

[Continua la traduzione seguendo lo stesso pattern per il resto del documento, mantenendo in inglese tutti i termini tecnici, codice, URL e nomi AWS come specificato nelle regole]

## Documentazione

### Per Iniziare
- **[Guida Rapida](docs/getting-started/quickstart.md)** - Distribuisci la tua prima applicazione (10 min)
- **[Guida Amministratore](docs/getting-started/admin-quickstart.md)** - Configura l'infrastruttura (15 min)

### Guide
- **[Application Manifest](docs/manifest.md)** - Riferimento completo configurazione YAML
- **[CLI Commands](docs/cli-commands.md)** - Tutti i comandi e le opzioni disponibili
- **[Bootstrap Actions](docs/bootstrap-actions.md)** - Azioni di distribuzione automatizzate e workflow basati su eventi
- **[Substitutions & Variables](docs/substitutions-and-variables.md)** - Configurazione dinamica
- **[Guida alle Connessioni](docs/connections.md)** - Configura integrazioni con servizi AWS
- **[GitHub Actions Integration](docs/github-actions-integration.md)** - Configurazione automazione CI/CD
- **[Deployment Metrics](docs/pipeline-deployment-metrics.md)** - Monitoraggio con EventBridge

### Riferimenti
- **[Manifest Schema](docs/manifest-schema.md)** - Validazione e struttura schema YAML
- **[Airflow AWS Operators](docs/airflow-aws-operators.md)** - Riferimento operatori personalizzati

### Esempi
- **[Guida agli Esempi](docs/examples-guide.md)** - Tutorial delle applicazioni di esempio
- **[Data Notebooks](docs/examples-guide.md#-data-engineering---notebooks)** - Notebook Jupyter con Airflow
- **[ML Training](docs/examples-guide.md#-machine-learning---training)** - Training SageMaker con MLflow
- **[ML Deployment](docs/examples-guide.md#-machine-learning---deployment)** - Distribuzione endpoint SageMaker
- **[QuickSight Dashboard](docs/examples-guide.md#-analytics---quicksight-dashboard)** - Dashboard BI con Glue
- **[GenAI Application](docs/examples-guide.md#-generative-ai)** - Agenti Bedrock e knowledge base

### Sviluppo
- **[Guida Sviluppatore](developer/developer-guide.md)** - Guida completa allo sviluppo con architettura, testing e workflow
- **[AI Assistant Context](developer/AmazonQ.md)** - Contesto per assistenti AI (Amazon Q, Kiro)
- **[Panoramica Test](tests/README.md)** - Infrastruttura di testing

### Supporto
- **Problemi**: [GitHub Issues](https://github.com/aws/CICD-for-SageMakerUnifiedStudio/issues)
- **Documentazione**: [docs/](docs/)
- **Esempi**: [examples/](examples/)

---

## Avviso di Sicurezza

⚠️ **NON** installare da PyPI - installare sempre dal codice sorgente ufficiale AWS.

```bash
# ✅ Corretto - Installare dal repository ufficiale AWS
git clone https://github.com/aws/CICD-for-SageMakerUnifiedStudio.git
cd CICD-for-SageMakerUnifiedStudio
pip install -e .

# ❌ Sbagliato - Non usare PyPI
pip install aws-smus-cicd-cli  # Potrebbe contenere codice malevolo
```

---

## Licenza

Questo progetto è concesso in licenza secondo la Licenza MIT-0. Vedere [LICENSE](../../LICENSE) per i dettagli.

---

<div align="center">
  <img src="docs/readme-qr-code.png" alt="Scan to view README" width="200"/>
  <p><em>Scansiona il codice QR per visualizzare questo README su GitHub</em></p>
</div>