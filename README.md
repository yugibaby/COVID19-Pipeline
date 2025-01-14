# COVID-19 Pipeline

Data pipeline designed for collecting, processing, and analyzing the JHU CSSE COVID-19 Data Repository.

---

## Features

### Data Collection
- **Ingestion:** Utilizes Dagster and GitHub API to fetch data from the repository.
- **Validation:** Ensures schema integrity using Pandera.
- **Normalization:** Adds missing columns and skips recovered data for simplicity.
- **Storage:** Loads raw data into PostgreSQL.

### Data Processing and Storage
- **ETL with dbt:** Processes raw data into two tables:
  - A comprehensive table optimized for answering the dataset questions.
  - A supplementary table for further analysis.
- **Database:** Stores processed data in PostgreSQL.

---

## Getting Started

### Prerequisites

- Python 3.9 or above
- PostgreSQL
- Dagster
- dbt
- Git

### Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yugibaby/COVID19-Pipeline.git
   cd COVID19-Pipeline
   ```

2. **Set Up Virtual Environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```
3. **Configure Environment Variables:**
   Copy and customize the `.env.example` file:
   ```bash
   cp .env.example .env
   ```

   #### Environment Variable Categories:
   - **Dagster-specific variables:**
     ```plaintext
     GITHUB_API_TOKEN=your-github-api-token
     ```

   - **DBT-specific variables:**
     ```plaintext
     POSTGRES_USER=your-postgres-username
     POSTGRES_PASSWORD=your-postgres-password
     POSTGRES_DB=your-database-name
     POSTGRES_HOST=your-database-host
     POSTGRES_PORT=your-database-port
     POSTGRES_SCHEMA=your-database-schema
     ```

   - **Shared Database URL:**
     ```plaintext
     DATABASE_URL=your-database-url
     ```  

4. **Install Dependencies:**
   ```bash
   cd dagster_pipeline
   pip install -r requirements.txt
   ```



5. **Run the Pipeline:**
   - Navigate to the `dagster_pipeline` directory and start Dagster:
     ```bash
     dagit
     ```
   - Access the UI at `http://localhost:3000` to launch the pipeline.

   ![Dagster Launchpad](assets/dagster_ui.png)

   Run the pipeline in the Launchpad and Dagster will ingest data and load it into PostgreSQL.
   

6. **Process Data with dbt:**
   - Navigate to the `covid_data_project` directory and set environment variables:
     ```bash
     set POSTGRES_HOST=your_host
     set POSTGRES_PORT=your_port
     set POSTGRES_USER=your_username
     set POSTGRES_PASSWORD=your_password
     set POSTGRES_DB=your_db
     set POSTGRES_SCHEMA=your_schema
     ```
   - Validate the setup:
     ```bash
     dbt debug --profiles-dir ~/.dbt
     ```

     Below is an example output of a successful `dbt debug` command:

     ![DBT Debug Output](assets/dbt_check.png)

   - Run dbt transformations:
     ```bash
     dbt run
     ```

     Below is an example output of a successful `dbt run` command:

     ![DBT Run Output](assets/dbt_tables.png)
     ```bash
     dbt run
     ```

---

## Dataset Analysis


#### 1. What are the top 5 most common values in a particular column, and what is their frequency?

- **Confirmed Cases (as of last update):**
  - US: 104,074,402
  - India: 44,966,906
  - France: 39,881,005
  - Germany: 38,423,158
  - Brazil: 37,347,938

- **Deaths (as of last update):**
  - US: 1,148,040
  - Brazil: 717,259
  - India: 538,529
  - Russia: 393,171
  - Mexico: 338,854

- **Highest Case Fatality Ratio (excluding North Korea & MS Zaandam):**
  - Yemen: 18.07%
  - Sudan: 7.86%
  - Syria: 5.51%
  - Peru: 5.15%
  - Somalia: 4.98%

2. How does a particular metric change over time within the dataset?
The two most critical metrics in the dataset are:

Total Confirmed Cases
Total Deaths
These serve as the main variables for computed metrics such as incidence_rate and case_fatality_ratio. 
Both confirmed and deaths columns increase over time as they represent cumulative totals, recorded daily from the beginning of the tracking period to the most recent reporting date.

Notably:
Recording of recovered cases and active cases has been discontinued in many regions, reducing their relevance over time.


3. Is there a correlation between two specific columns? Explain your findings.

In the current dataset, useful correlations are limited because:

Metrics such as confirmed and deaths are reported as cumulative totals, which obscure the rate of change over time.
Direct correlations between totals, such as confirmed and deaths, tend to be trivially high because they inherently increase together.

Introduce a Column for Daily Changes to improve Correlation Analysis:

![Supplementary Table](assets/supplement.png)

Create a new column to track daily increments (e.g., confirmed_diff, deaths_diff).
This would allow meaningful analysis of how metrics evolve over time and potentially reveal correlations between daily changes, seasonality of metrics, clustering of geospatial data for climate correlations, vaccination rate, lockdown guidelines etc..

The case_fatality_ratio metric (deaths/confirmed) could provide insights, but it requires supplementary data (e.g., healthcare access, population density, or comorbidities) to uncover meaningful relationships.

